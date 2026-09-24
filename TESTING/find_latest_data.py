from datetime import datetime
from pathlib import Path
import re


DATA_DIRECTORY = Path(r"D:\FNO DATA\Participant data")
DATE_PATTERN = re.compile(r"(?<!\d)(\d{8})(?!\d)")


def parse_filename_date(filename: str) -> datetime | None:
    for match in DATE_PATTERN.finditer(filename):
        value = match.group(1)
        for date_format in ("%d%m%Y", "%Y%m%d"):
            try:
                return datetime.strptime(value, date_format)
            except ValueError:
                continue
    return None


def main() -> None:
    if not DATA_DIRECTORY.is_dir():
        print(f"Data directory not found: {DATA_DIRECTORY}")
        return

    files = [path for path in DATA_DIRECTORY.iterdir() if path.is_file()]
    if not files:
        print(f"No files found in: {DATA_DIRECTORY}")
        return

    dated_files = [
        (date, path)
        for path in files
        if (date := parse_filename_date(path.name)) is not None
    ]

    if dated_files:
        latest_date, latest_file = max(dated_files, key=lambda item: (item[0], item[1].name))
        print(f"Latest available data date: {latest_date:%Y-%m-%d}")
        print(f"File: {latest_file.name}")
        return

    latest_file = max(files, key=lambda path: path.stat().st_mtime)
    modified_date = datetime.fromtimestamp(latest_file.stat().st_mtime)
    print("No date found in filenames; using the last-modified timestamp.")
    print(f"Latest available data date: {modified_date:%Y-%m-%d %H:%M:%S}")
    print(f"File: {latest_file.name}")


if __name__ == "__main__":
    main()
