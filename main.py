#!/usr/bin/env python3
"""Download GDELT files whose names contain 'export' and upload to GCS via gsutil.

Usage examples:
  python main.py --bucket gs://gdeltv1 --dest-prefix data/ --dry-run
  python main.py --bucket gs://my-bucket

Notes:
 - Requires `gsutil` on PATH and authenticated gcloud account for the target bucket.
 - Saves downloads under ./downloads
"""
import argparse
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "http://data.gdeltproject.org/events/index.html"


def list_export_links(session):
	resp = session.get(BASE_URL, timeout=30)
	resp.raise_for_status()
	soup = BeautifulSoup(resp.text, "html.parser")
	links = []
	for a in soup.find_all("a", href=True):
		href = a["href"]
		if "export" in href.lower():
			full = urljoin(BASE_URL, href)
			links.append(full)
	return links


def download_file(session, url, dest_dir):
	local_name = url.split("/")[-1]
	dest = dest_dir / local_name
	if dest.exists():
		return dest
	with session.get(url, stream=True, timeout=60) as r:
		r.raise_for_status()
		tmp = dest.with_suffix(".part")
		with open(tmp, "wb") as fh:
			shutil.copyfileobj(r.raw, fh)
		tmp.rename(dest)
	return dest


def gsutil_cp(local_path: Path, bucket: str, dest_prefix: str, dry_run: bool = False):
	bucket = bucket.rstrip('/')
	prefix = dest_prefix or ""
	if prefix and not prefix.endswith('/'):
		prefix = prefix + '/'
	dest_name = f"{bucket}/{prefix}{local_path.name}"
	cmd = ["gsutil", "cp", str(local_path), dest_name]
	print("RUN:", " ".join(cmd))
	if dry_run:
		return 0
	# Make sure gsutil is available on PATH before attempting to run it.
	if shutil.which("gsutil") is None:
		print("Error: 'gsutil' not found on PATH. Please install the Google Cloud SDK or ensure 'gsutil' is available.", file=sys.stderr)
		return 2

	try:
		proc = subprocess.run(cmd, capture_output=True, text=True)
	except FileNotFoundError as e:
		# This should be rare since we checked shutil.which, but handle it just in case.
		print(f"Error running gsutil: {e}", file=sys.stderr)
		return 2

	if proc.returncode != 0:
		if proc.stdout:
			print(proc.stdout)
		if proc.stderr:
			print(proc.stderr, file=sys.stderr)
	return proc.returncode


def parse_args():
	p = argparse.ArgumentParser(description="Download GDELT export files and upload to GCS via gsutil")
	p.add_argument("--bucket", required=True, help="gs://bucket-name or gs://bucket-name/path-prefix (bucket required)")
	p.add_argument("--dest-prefix", default="", help="Optional destination prefix inside the bucket (e.g. data/)")
	p.add_argument("--downloads-dir", default="downloads", help="Local downloads directory")
	p.add_argument("--dry-run", action="store_true", help="Don't actually call gsutil; just print commands")
	return p.parse_args()


def main():
	args = parse_args()
	downloads = Path(args.downloads_dir).resolve()
	downloads.mkdir(parents=True, exist_ok=True)

	session = requests.Session()
	links = list_export_links(session)
	if not links:
		print("No export links found on page.")
		return 0

	print(f"Found {len(links)} links containing 'export'.")
	for url in links:
		try:
			print("Downloading:", url)
			local = download_file(session, url, downloads)
			print("Saved:", local)
			rc = gsutil_cp(local, args.bucket, args.dest_prefix, dry_run=args.dry_run)
			if rc != 0:
				print(f"gsutil failed for {local} (rc={rc})", file=sys.stderr)
		except Exception as e:
			print(f"Error processing {url}: {e}", file=sys.stderr)

	return 0


if __name__ == "__main__":
	raise SystemExit(main())
