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
import zipfile
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
	tmp = dest.with_suffix(".part")
	# If a previous .part exists (from an interrupted download), remove it and start fresh
	try:
		if tmp.exists():
			tmp.unlink()
	except Exception:
		# If we can't remove the temp file, continue and let the write fail if needed
		pass

	with session.get(url, stream=True, timeout=60) as r:
		r.raise_for_status()
		try:
			with open(tmp, "wb") as fh:
				shutil.copyfileobj(r.raw, fh)
			tmp.rename(dest)
		except Exception:
			# On any error while writing, ensure tmp is removed so future runs start clean
			try:
				if tmp.exists():
					tmp.unlink()
			except Exception:
				pass
			raise
	return dest


def gsutil_cp(local_path: Path, bucket: str, dest_prefix: str, dry_run: bool = False):
	bucket = bucket.rstrip('/')
	prefix = dest_prefix or ""
	if prefix and not prefix.endswith('/'):
		prefix = prefix + '/'
	dest_name = f"{bucket}/{prefix}{local_path.name}"
	# If local_path is a directory, upload recursively with -r
	if local_path.is_dir():
		cmd = ["gsutil", "cp", "-r", str(local_path), dest_name]
	else:
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
	p.add_argument("--cleanup", action="store_true", help="Remove zip file after successful extraction and upload")
	p.add_argument("--max-items", type=int, default=0, help="Limit number of files to process (0 = no limit)")
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
	processed = 0
	try:
		for url in links:
			if args.max_items and args.max_items > 0 and processed >= args.max_items:
				break
			try:
				print("Downloading:", url)
				local = download_file(session, url, downloads)
				print("Saved:", local)
				# If the downloaded file is a zip archive, extract into a folder
				if local.suffix.lower() == '.zip':
					# Extract CSV files directly into downloads/ (flat), not per-archive folders
					try:
						with zipfile.ZipFile(local, 'r') as zf:
							members = [m for m in zf.namelist() if m.lower().endswith('.csv')]
							if not members:
								print(f"No CSV files found in {local}")
							for member in members:
								# Normalize path to basename to avoid nested paths inside zips
								target_name = Path(member).name
								target_path = downloads / target_name
								print(f"Extracting {member} -> {target_path}")
								with zf.open(member) as src, open(target_path, 'wb') as dst:
									shutil.copyfileobj(src, dst)
					except zipfile.BadZipFile as e:
						print(f"Bad zip file {local}: {e}", file=sys.stderr)
						continue
					# Upload extracted CSV files individually
					for csvfile in downloads.glob('*.csv'):
						rc = gsutil_cp(csvfile, args.bucket, args.dest_prefix, dry_run=args.dry_run)
						if rc != 0:
							print(f"gsutil failed for {csvfile} (rc={rc})", file=sys.stderr)
					# Optionally remove the zip file after successful extraction/upload
					if args.cleanup:
						try:
							local.unlink()
						except Exception as e:
							print(f"Failed to remove {local}: {e}", file=sys.stderr)
				else:
					rc = gsutil_cp(local, args.bucket, args.dest_prefix, dry_run=args.dry_run)
					if rc != 0:
						print(f"gsutil failed for {local} (rc={rc})", file=sys.stderr)
				processed += 1
			except Exception as e:
				print(f"Error processing {url}: {e}", file=sys.stderr)
				# continue to next link
				continue
	except KeyboardInterrupt:
		print("\nInterrupted by user. Exiting cleanly.")
		# exit, leaving completed downloads and removing any in-progress .part files will be handled on next run

	return 0


if __name__ == "__main__":
	raise SystemExit(main())
