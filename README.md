# GDELT export downloader + GCS uploader

This small tool scrapes the GDELT events index page and downloads any file links that contain the word "export". After downloading, it runs `gsutil cp` to upload each file to a GCS bucket.

Prerequisites
- Python 3.8+
- Install Python deps: pip install -r requirements.txt
- `gsutil` on PATH and authenticated (gcloud auth login && gcloud auth application-default login or gsutil config)

Usage

Example dry-run (prints gsutil commands only):

```bash
python main.py --bucket gs://gdeltv1 --dest-prefix data/ --dry-run
```

Actual upload:

```bash
python main.py --bucket gs://gdeltv1 --dest-prefix data/
```

Notes
- The script saves files to `./downloads` by default.
- It will skip re-downloading files that already exist locally.
- If your bucket path already contains a prefix (e.g. `gs://my-bucket/path`), `--dest-prefix` will be appended after that.
