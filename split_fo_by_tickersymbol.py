#!/usr/bin/env python3
"""Split NSE F&O CSV files into smaller files by ticker symbol."""

from __future__ import annotations

import csv
import json
import re
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

import click

DEFAULT_INPUT_DIR = Path.home() / ".cache" / "historical-option-chain-data" / "fo"
DEFAULT_OUTPUT_DIR = Path("data/fo")
DEFAULT_JSON_OUTPUT_DIR = Path("data/fo_json")
KNOWN_SYMBOL_COLUMNS = ("TckrSymb", "TickerSymbol", "SYMBOL")


class JsonArrayWriter:
    """Stream rows as a JSON array to avoid loading all rows into memory."""

    def __init__(self, handle) -> None:
        self.handle = handle
        self.first = True
        self.handle.write("[\n")

    def write_row(self, row: dict[str, str]) -> None:
        if not self.first:
            self.handle.write(",\n")
        json.dump(row, self.handle, separators=(",", ":"))
        self.first = False

    def close(self) -> None:
        if not self.first:
            self.handle.write("\n")
        self.handle.write("]\n")


def convert_csv_to_json_array(csv_path: Path, json_path: Path) -> None:
    """Convert one CSV file into a JSON array file with streaming writes."""
    with csv_path.open("r", newline="", encoding="utf-8-sig") as source, json_path.open(
        "w", encoding="utf-8"
    ) as target:
        reader = csv.DictReader(source)
        writer = JsonArrayWriter(target)
        try:
            for row in reader:
                writer.write_row(row)
        finally:
            writer.close()


def sanitize_filename(value: str) -> str:
    """Keep filenames safe for all platforms while preserving readability."""
    safe = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in value.strip())
    return safe or "UNKNOWN"


def resolve_symbol_column(fieldnames: list[str] | None, preferred: str) -> str:
    if not fieldnames:
        raise ValueError("CSV file has no header row")

    if preferred in fieldnames:
        return preferred

    by_casefold = {name.casefold(): name for name in fieldnames}
    if preferred.casefold() in by_casefold:
        return by_casefold[preferred.casefold()]

    for candidate in KNOWN_SYMBOL_COLUMNS:
        if candidate in fieldnames:
            return candidate
        if candidate.casefold() in by_casefold:
            return by_casefold[candidate.casefold()]

    raise ValueError(
        f"Could not find symbol column '{preferred}'. Available columns: {', '.join(fieldnames)}"
    )


def output_subdir_name(csv_path: Path) -> str:
    """Use YYYY-MM-DD extracted from filename when possible; fallback to stem."""
    match = re.search(r"(\d{8})", csv_path.stem)
    if not match:
        return csv_path.stem

    yyyymmdd = match.group(1)
    try:
        return datetime.strptime(yyyymmdd, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return csv_path.stem


def split_one_file(
    csv_path: Path,
    output_root: Path,
    symbol_column: str,
    json_output_root: Path | None = None,
) -> tuple[int, int]:
    subdir_name = output_subdir_name(csv_path)
    output_dir = output_root / subdir_name
    output_dir.mkdir(parents=True, exist_ok=True)
    # Rebuild per-date split files from scratch to avoid duplicate appends on reruns.
    for existing_csv in output_dir.glob("*.csv"):
        existing_csv.unlink()

    json_output_dir = None
    if json_output_root is not None:
        json_output_dir = json_output_root / subdir_name
        json_output_dir.mkdir(parents=True, exist_ok=True)

    seen_symbols: set[str] = set()
    rows_written = 0

    with csv_path.open("r", newline="", encoding="utf-8-sig") as source:
        reader = csv.DictReader(source)
        resolved_column = resolve_symbol_column(reader.fieldnames, symbol_column)
        if not reader.fieldnames:
            return (0, 0)

        for row in reader:
            symbol = (row.get(resolved_column) or "UNKNOWN").strip() or "UNKNOWN"
            safe_symbol = sanitize_filename(symbol)

            out_path = output_dir / f"{safe_symbol}.csv"
            write_header = safe_symbol not in seen_symbols

            with out_path.open("a", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=reader.fieldnames)
                if write_header:
                    writer.writeheader()
                    seen_symbols.add(safe_symbol)
                writer.writerow(row)

            rows_written += 1

    # if json_output_dir is not None:
    #     for split_csv in sorted(output_dir.glob("*.csv")):
    #         json_path = json_output_dir / f"{split_csv.stem}.json"
    #         convert_csv_to_json_array(split_csv, json_path)

    return rows_written, len(seen_symbols)


def iter_csv_files(input_dir: Path) -> Iterable[Path]:
    return sorted(path for path in input_dir.glob("*.csv") if path.is_file())


@click.command()
@click.option(
    "--input-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=DEFAULT_INPUT_DIR,
    show_default=True,
    help="Input directory containing F&O CSV files.",
)
@click.option(
    "--input-file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Single F&O CSV file to split.",
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=DEFAULT_OUTPUT_DIR,
    show_default=True,
    help="Output directory for split files.",
)
@click.option(
    "--symbol-column",
    default="TckrSymb",
    show_default=True,
    help="Ticker symbol column to split on.",
)
@click.option(
    "--json-output-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=DEFAULT_JSON_OUTPUT_DIR,
    show_default=True,
    help="Output directory for per-symbol JSON files.",
)
@click.option(
    "--skip-json",
    is_flag=True,
    default=False,
    help="Skip JSON export and only write split CSV files.",
)
def main(
    input_dir: Path,
    input_file: Path | None,
    output_dir: Path,
    symbol_column: str,
    json_output_dir: Path,
    skip_json: bool,
) -> None:
    files = [input_file] if input_file is not None else list(iter_csv_files(input_dir))
    if not files:
        click.echo(f"No CSV files found in {input_dir}")
        raise SystemExit(1)

    total_rows = 0
    total_outputs = 0

    for csv_file in files:
        try:
            rows, symbols = split_one_file(
                csv_file,
                output_dir,
                symbol_column,
                None if skip_json else json_output_dir,
            )
            total_rows += rows
            total_outputs += symbols
            click.echo(f"{csv_file.name}: {rows} rows -> {symbols} symbol files")
        except ValueError as err:
            click.echo(f"{csv_file.name}: skipped ({err})")

    click.echo(
        f"Done. Processed {len(files)} file(s), wrote {total_rows} rows across "
        f"{total_outputs} split file(s)."
    )
    if not skip_json:
        click.echo(f"JSON files written under: {json_output_dir}")


if __name__ == "__main__":
    main()
