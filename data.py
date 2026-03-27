"""
Download NSE Bhavcopy data for a date range and merge into SQLite.

Usage:
    uv run data.py --from-date 2025-01-01 --to-date 2025-03-21 --type equity
    uv run data.py --from-date 2025-01-01 --to-date 2025-03-21 --type fo
    uv run data.py --from-date 2025-01-01 --to-date 2025-03-21 --type index
    uv run data.py --from-date 2025-01-01 --to-date 2025-03-21 --type full

Bhavcopy types:
    equity  - CM Bhavcopy (equities)
    fo      - F&O Derivatives Bhavcopy
    index   - Index Bhavcopy (from niftyindices.com)
    full    - Full Bhavcopy with delivery data
"""

import io
import os
import subprocess
import sys
import zipfile
from datetime import date, datetime, timedelta

from jugaad_data.nse import (
    bhavcopy_index_save,
    bhavcopy_save,
    full_bhavcopy_save,
)
from jugaad_data.nse.archives import NSEArchives
import click

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
CACHE_DIR = os.path.join(os.path.expanduser("~"), ".cache", "historical-option-chain-data")
DB_PATH = os.path.join(BASE_DIR, "bhavcopy.db")

# Shared NSEArchives session for FO downloads
_nse_archives = None


def _get_nse_archives():
    global _nse_archives
    if _nse_archives is None:
        _nse_archives = NSEArchives()
    return _nse_archives


def bhavcopy_fo_save_fixed(dt, dest, skip_if_present=True):
    """Download F&O bhavcopy using NSE's current URL format.

    NSE moved from the old /content/historical/DERIVATIVES/... zip to
    /content/fo/BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip
    """
    if isinstance(dt, datetime):
        dt = dt.date()

    fname_zip = f"BhavCopy_NSE_FO_0_0_0_{dt.strftime('%Y%m%d')}_F_0000.csv.zip"
    fname_csv = fname_zip.replace(".zip", "")
    fpath = os.path.join(dest, fname_csv)

    if os.path.isfile(fpath) and skip_if_present:
        return fpath

    a = _get_nse_archives()
    url = f"https://nsearchives.nseindia.com/content/fo/{fname_zip}"
    r = a.s.get(url, timeout=30)
    r.raise_for_status()

    fp = io.BytesIO(r.content)
    with zipfile.ZipFile(file=fp) as zf:
        inner = zf.namelist()[0]
        with zf.open(inner) as zfp:
            text = zfp.read().decode("utf-8")

    with open(fpath, "w") as f:
        f.write(text)
    return fpath


BHAV_TYPES = {
    "equity": {
        "func": bhavcopy_save,
        "table": "equity_bhavcopy",
        "file_fmt": "cm%d%b%Ybhav.csv",
    },
    "fo": {
        "func": bhavcopy_fo_save_fixed,
        "table": "fo_bhavcopy",
        "file_fmt": "BhavCopy_NSE_FO_0_0_0_%Y%m%d_F_0000.csv",
    },
    "index": {
        "func": bhavcopy_index_save,
        "table": "index_bhavcopy",
        "file_fmt": "ind_close_all_%d%m%Y.csv",
    },
    "full": {
        "func": full_bhavcopy_save,
        "table": "full_bhavcopy",
        "file_fmt": "sec_bhavdata_full_%d%b%Ybhav.csv",
    },
}


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def trading_days(from_date: date, to_date: date):
    """Yield weekdays (Mon-Fri) between from_date and to_date inclusive."""
    d = from_date
    while d <= to_date:
        if d.weekday() < 5:  # Mon=0 .. Fri=4
            yield d
        d += timedelta(days=1)


def download_bhavcopy_for_date(dt: date, bhav_type: str, dest: str | None = None) -> str | None:
    """Download a bhavcopy CSV for a single date. Returns the CSV path when available."""
    cfg = BHAV_TYPES[bhav_type]
    save_func = cfg["func"]
    dest = dest or os.path.join(CACHE_DIR, bhav_type)
    os.makedirs(dest, exist_ok=True)

    fname = os.path.join(dest, dt.strftime(cfg["file_fmt"]))
    if os.path.isfile(fname):
        print(f"  [skip] {fname} (already exists)")
        return fname

    try:
        saved = save_func(dt, dest)
        print(f"  [done] {saved}")
        return saved
    except Exception as e:
        print(f"  [fail] {dt} — {e}")
        return None


def download_bhavcopy(from_date: date, to_date: date, bhav_type: str) -> str:
    """Download bhavcopy CSVs for date range. Returns path to cache dir."""
    dest = os.path.join(CACHE_DIR, bhav_type)
    for d in trading_days(from_date, to_date):
        download_bhavcopy_for_date(d, bhav_type, dest)

    return dest


def csv_files_sorted(directory: str) -> list[str]:
    """Return sorted list of CSV file paths in a directory."""
    files = [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.lower().endswith(".csv")
    ]
    return sorted(files)


def merge_to_sqlite(csv_dir: str, table_name: str, db_path: str = DB_PATH):
    """Merge all CSVs from csv_dir into a SQLite table using csv-to-sqlite."""
    csv_files = csv_files_sorted(csv_dir)
    if not csv_files:
        print("  No CSV files found to merge.")
        return

    cmd = [
        sys.executable, "-m", "csv_to_sqlite",
        "-o", db_path,
        "-D",           # drop table if exists
        "-t", "full",   # full type inference (int/float/string)
    ]
    for f in csv_files:
        cmd.extend(["-f", f])

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  csv-to-sqlite error: {result.stderr.strip()}")
        sys.exit(1)

    print(f"  Merged {len(csv_files)} CSV files into '{db_path}'")


def run_split_fo_script(input_file: str | None = None):
    """Run the FO split script to create per-symbol CSVs."""
    split_script = os.path.join(BASE_DIR, "split_fo_by_tickersymbol.py")
    if not os.path.isfile(split_script):
        print("  [warn] split_fo_by_tickersymbol.py not found; skipping split step.")
        return

    cmd = [
        sys.executable,
        split_script,
        "--output-dir",
        os.path.join(DATA_DIR, "fo"),
        "--skip-json",
    ]

    if input_file:
        cmd.extend(["--input-file", input_file])
    else:
        cmd.extend(["--input-dir", os.path.join(CACHE_DIR, "fo")])

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  split script error: {result.stderr.strip() or result.stdout.strip()}")
        sys.exit(1)

    if result.stdout.strip():
        print(result.stdout.strip())


@click.command(help="Download NSE Bhavcopy data and merge into SQLite.")
@click.option(
    "--from-date",
    "from_date_str",
    default=None,
    show_default="7 days before --to-date (or today)",
    help="Start date (YYYY-MM-DD)",
)
@click.option(
    "--to-date",
    "to_date_str",
    default=None,
    show_default="today",
    help="End date (YYYY-MM-DD)",
)
@click.option(
    "--type",
    "bhav_type",
    required=True,
    type=click.Choice(list(BHAV_TYPES.keys())),
    help="Bhavcopy type: equity, fo, index, full",
)
@click.option(
    "--datasette",
    is_flag=True,
    default=False,
    help="Merge downloaded CSVs into SQLite (for Datasette)",
)
@click.option("--db", "db_path", default=DB_PATH, show_default=True, help="SQLite database path")
def main(
    from_date_str: str | None,
    to_date_str: str | None,
    bhav_type: str,
    datasette: bool,
    db_path: str,
):
    default_to_date = date.today()
    try:
        to_date = parse_date(to_date_str) if to_date_str else default_to_date
        from_date = parse_date(from_date_str) if from_date_str else (to_date - timedelta(days=7))
    except ValueError:
        raise click.BadParameter("Dates must be in YYYY-MM-DD format")

    if from_date > to_date:
        raise click.UsageError("--from-date must be <= --to-date")

    cfg = BHAV_TYPES[bhav_type]
    click.echo(f"Downloading '{bhav_type}' bhavcopy from {from_date} to {to_date} ...")
    csv_dir = os.path.join(CACHE_DIR, bhav_type)
    if bhav_type == "fo":
        click.echo("Splitting each FO CSV by ticker symbol immediately after download ...")

    for current_date in trading_days(from_date, to_date):
        downloaded_csv = download_bhavcopy_for_date(current_date, bhav_type, csv_dir)
        if bhav_type == "fo" and downloaded_csv:
            run_split_fo_script(downloaded_csv)

    if datasette:
        click.echo("\nMerging CSVs into SQLite ...")
        merge_to_sqlite(csv_dir, cfg["table"], db_path)
    else:
        click.echo("\nSkipping SQLite merge (use --datasette to enable).")


    click.echo("Done.")


if __name__ == "__main__":
    main()

