# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1: Source it
# MAGIC Incrementally syncs the BLS `pr` time-series folder and the DataUSA population API into a Unity Catalog Volume.
# MAGIC
# MAGIC > **Required before you run:** set the `contact` widget (at the top of the notebook) to your real name and
# MAGIC > email address, for example `Jane Doe jane@yourdomain.com`. BLS blocks requests that do not identify the caller,
# MAGIC > so the notebook stops with an error if the widget has no `@` or still holds the `example.com` placeholder.
# MAGIC
# MAGIC **Design**
# MAGIC - Source listing is scraped every run (no hardcoded filenames).
# MAGIC - A manifest in the Volume records what has been landed (size + last-modified). Unchanged files are skipped.
# MAGIC - Changed files are landed as a NEW immutable version (`<file>/v=<last_modified>/<file>`); old versions are never overwritten.
# MAGIC   Bronze (Auto Loader) therefore sees changes as new files; Silver picks the latest version per key.
# MAGIC - Files that vanish from the source are flagged `removed_at` in the manifest, never deleted from the Volume.
# MAGIC - Population JSON is landed only when its content hash changes.
# MAGIC - Safety: an empty/unparseable listing aborts the run instead of "removing" everything.

# COMMAND ----------

# ---------------------------------------------------------------------------------------------
# Parameters: Databricks widgets, so the same notebook runs anywhere without editing the code.
# ---------------------------------------------------------------------------------------------
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "rearc_quest")
dbutils.widgets.text("volume", "raw")
dbutils.widgets.text("contact", "Your Name your.email@example.com")  # REQUIRED by BLS policy

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
CONTACT = dbutils.widgets.get("contact").strip()

# Stop immediately if the contact is missing or is still the placeholder.
# BLS answers anonymous clients with HTTP 403, so failing here gives a clearer message.
assert "@" in CONTACT and "example.com" not in CONTACT.lower(), (
    "The 'contact' widget must contain your real name and email address, for example "
    "'Jane Doe jane@yourdomain.com'. BLS blocks requests that do not identify the caller, and the "
    "placeholder (example.com) is not accepted."
)

# Create the schema and volume if they do not exist yet. IF NOT EXISTS makes this safe to re-run.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# Folder layout inside the Volume:
#   bls/pr/<file>/v=<last_modified>/<file>   raw BLS files, one immutable folder per version
#   population/                              one JSON snapshot each time the API response changes
#   _state/manifest.json                     this notebook's memory of what it has already ingested
ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
BLS_DIR = f"{ROOT}/bls/pr"
POP_DIR = f"{ROOT}/population"
STATE_DIR = f"{ROOT}/_state"          # kept outside the Auto Loader source paths
MANIFEST = f"{STATE_DIR}/manifest.json"

print(ROOT)

# COMMAND ----------

import hashlib
import json
import os
import re
import shutil
import tempfile
import time
from datetime import datetime, timezone

import requests

# Sources: the BLS directory listing page, and the DataUSA API that returns US population by year.
BLS_URL = "https://download.bls.gov/pub/time.series/pr/"
POP_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
)

# One shared HTTP session: the headers below are sent automatically with every request.
session = requests.Session()
# BLS 403s requests without contact info: https://www.bls.gov/bls/pss.htm
# User-Agent identifies who is downloading; Accept-Encoding allows compressed (smaller) responses,
# which `requests` decompresses transparently.
session.headers.update({"User-Agent": CONTACT, "Accept-Encoding": "gzip, deflate"})


def now_iso() -> str:
    # Current UTC time as text, for the ingested_at / removed_at fields in the manifest.
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_with_retry(url: str, *, stream: bool = False, attempts: int = 5, timeout: int = 60):
    """GET with exponential backoff on 429/5xx/network errors. Raises on 403 (fix the User-Agent, don't retry)."""
    for i in range(attempts):
        try:
            r = session.get(url, stream=stream, timeout=timeout)
            # 429 (rate limited) and 5xx (server-side problems) are usually temporary.
            # Raise an error here so they go through the same retry path as network failures.
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"{r.status_code} for {url}", response=r)
            # Any other 4xx/5xx status raises here.
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            # Network errors have no response, so status stays None and they are retried.
            status = getattr(getattr(e, "response", None), "status_code", None)
            # 401/403/404 will fail the same way every time, and after the last attempt there is
            # nothing left to try: stop and let the error surface.
            if status in (401, 403, 404) or i == attempts - 1:
                raise
            wait = 2 ** i  # exponential backoff: 1s, 2s, 4s, 8s
            print(f"retry {i + 1}/{attempts - 1} in {wait}s: {e}")
            time.sleep(wait)


def load_manifest() -> dict:
    # The manifest is this notebook's memory between runs: what was ingested, when, and from where.
    if os.path.exists(MANIFEST):
        with open(MANIFEST) as f:
            return json.load(f)
    # First run: nothing has been ingested yet, so every source file will count as new.
    return {"bls": {}, "population": {}}


def save_manifest(m: dict) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    # Write to a temp file first, then copy it over the real manifest. This lowers (but does not
    # remove) the chance of a half-written manifest, because Volumes don't guarantee atomic renames.
    # A Delta table would be the robust fix for production.
    tmp = f"{MANIFEST}.tmp"
    with open(tmp, "w") as f:
        json.dump(m, f, indent=2, sort_keys=True)
    shutil.copyfile(tmp, MANIFEST)  # Volumes don't guarantee atomic rename; copy over then drop tmp
    os.remove(tmp)


def land(local_path: str, dest_path: str) -> None:
    # Copy a downloaded file into the Volume, creating the destination folders first.
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    shutil.copyfile(local_path, dest_path)

# COMMAND ----------
# MAGIC %md
# MAGIC ## BLS: list the directory

# COMMAND ----------

# The BLS folder is an HTML directory page with one line per file. The regex captures, in order:
#   1. date   2. time   3. size in bytes   4. link (href)   5. file name
# If BLS changes the page layout, this matches nothing and list_bls() aborts the run (see below).
# Apache/IIS style listing, e.g.:
#  9/19/2025  8:31 AM      2354346 <A HREF="/pub/time.series/pr/pr.class">pr.class</A>
LISTING_RE = re.compile(
    r'(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}\s*[AP]M)\s+(\d+)\s+<A\s+HREF="([^"]+)">([^<]+)</A>',
    re.IGNORECASE,
)


def list_bls() -> dict:
    # Returns {file name: {"url", "size", "last_modified"}} for every file currently at the source.
    html = get_with_retry(BLS_URL).text
    files = {}
    for date_s, time_s, size_s, href, name in LISTING_RE.findall(html):
        # Turn "9/19/2025" + "8:31 AM" into a real datetime, then into a sortable ISO-style string.
        ts = datetime.strptime(f"{date_s} {time_s.upper().replace(' ', '')}", "%m/%d/%Y %I:%M%p")
        files[name.strip()] = {
            "url": "https://download.bls.gov" + href if href.startswith("/") else href,
            "size": int(size_s),
            "last_modified": ts.strftime("%Y-%m-%dT%H:%M"),
        }
    # Safety guard: if nothing parsed (layout changed, or an error page), stop here. Carrying on would
    # make the sync below treat every known file as removed.
    if not files:
        raise RuntimeError("Parsed 0 files from BLS listing; aborting so we don't mark everything removed.")
    return files


listing = list_bls()
# Print the source listing so you can see at a glance what BLS has before anything downloads.
print(f"{len(listing)} files at source")
for n, m in sorted(listing.items()):
    print(f"  {n:35s} {m['size']:>12,d}  {m['last_modified']}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## BLS: sync (new / changed / removed)

# COMMAND ----------

manifest = load_manifest()
known = manifest["bls"]   # the same dict object as inside `manifest`, so edits to it are saved with the manifest
run_ts = now_iso()        # one timestamp for the whole run
stats = {"new": 0, "changed": 0, "unchanged": 0, "removed": 0}


def version_key(meta: dict) -> str:
    # "2025-09-19T08:31" -> "202509190831", used as the name of the v= folder.
    return meta["last_modified"].replace("-", "").replace(":", "").replace("T", "")


# Compare every file at the source with what the manifest says we already have.
for name, meta in sorted(listing.items()):
    prev = known.get(name)
    # A file is "unchanged" only if we have seen it, its size and last-modified time still match,
    # and it was not flagged as removed (a file that disappeared and came back is landed again).
    unchanged = (
        prev is not None
        and prev["size"] == meta["size"]
        and prev["last_modified"] == meta["last_modified"]
        and not prev.get("removed_at")
    )
    if unchanged:
        stats["unchanged"] += 1
        continue

    # New or changed: download to a temporary local folder (deleted automatically afterwards).
    with tempfile.TemporaryDirectory() as tmpdir:
        local = os.path.join(tmpdir, name)
        h = hashlib.sha256()
        # Stream the download in 1 MiB chunks so large files never sit fully in memory, and hash
        # the bytes as they are written.
        with get_with_retry(meta["url"], stream=True, timeout=120) as r, open(local, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
                h.update(chunk)
        actual = os.path.getsize(local)
        # Content-Length can differ if the server compresses; only hard-fail on an empty download.
        if actual == 0:
            raise RuntimeError(f"Empty download for {name}")
        # Land it in a version folder named after the source's last-modified time. A changed file gets a
        # NEW folder, so earlier versions are never overwritten.
        dest = f"{BLS_DIR}/{name}/v={version_key(meta)}/{name}"
        land(local, dest)

    # Record what we landed. {**meta} copies url, size and last_modified from the source listing.
    # The hash is an integrity/audit record: change detection above uses size and time, not the hash.
    known[name] = {
        **meta,
        "sha256": h.hexdigest(),
        "bytes_landed": actual,
        "landed_path": dest,
        "ingested_at": run_ts,
        "removed_at": None,
    }
    stats["changed" if prev else "new"] += 1
    print(f"landed {name} ({'changed' if prev else 'new'}) -> {dest}")
    # Checkpoint after every file. If the run dies later, finished files are already recorded and the
    # next run resumes where this one stopped. If it dies between land() and this save, the next run just
    # lands the same file at the same path again, which is harmless.
    save_manifest(manifest)  # checkpoint per file so a mid-run failure resumes cleanly

# Removals: files the manifest knows about that are no longer at the source. Flag them with the time,
# but keep the raw copy in the Volume so Bronze history stays intact.
for name, prev in known.items():
    if name not in listing and not prev.get("removed_at"):
        prev["removed_at"] = run_ts
        stats["removed"] += 1
        print(f"no longer at source: {name} (raw copy retained)")

save_manifest(manifest)
print(stats)  # e.g. {'new': 12, 'changed': 0, 'unchanged': 0, 'removed': 0}; a second run shows all unchanged

# COMMAND ----------
# MAGIC %md
# MAGIC ## Population API (DataUSA)

# COMMAND ----------

# Fetch the whole population dataset (one small JSON document).
resp = get_with_retry(POP_URL)
payload = resp.content   # the exact bytes returned; this is what gets saved to disk
data = resp.json()       # the parsed version, used for validation and hashing
# Sanity check on the response shape, so an error page or format change doesn't get saved as data.
assert "data" in data and len(data["data"]) > 0, "Unexpected population payload shape"

# Fingerprint the content. Sorting keys and removing whitespace first means a response that is
# formatted differently but has the same data does not count as a change.
# hash a canonical form so key-order/whitespace changes don't create false versions
canonical = json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
digest = hashlib.sha256(canonical).hexdigest()

pop_state = manifest["population"]
if pop_state.get("sha256") == digest:
    # Same content as the last snapshot: land nothing.
    print("Population unchanged; nothing landed.")
else:
    # New or changed content: save a new snapshot. The file name carries the time and the first
    # 12 characters of the hash, so every snapshot is unique and nothing is overwritten.
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = f"{POP_DIR}/population_{stamp}_{digest[:12]}.json"
    with tempfile.TemporaryDirectory() as tmpdir:
        local = os.path.join(tmpdir, "population.json")
        with open(local, "wb") as f:
            f.write(payload)
        land(local, dest)
    manifest["population"] = {"sha256": digest, "landed_path": dest, "ingested_at": run_ts, "rows": len(data["data"])}
    save_manifest(manifest)
    print(f"Landed population ({len(data['data'])} records) -> {dest}")

# COMMAND ----------

# Show the top level of the Volume: bls/, population/ and _state/ should all be there.
display(dbutils.fs.ls(ROOT))
