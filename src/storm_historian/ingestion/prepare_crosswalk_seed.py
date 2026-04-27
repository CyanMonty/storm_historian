"""
Downloads the U.S. Census Bureau 2020 ZCTA-to-County relationship file and
writes a cleaned CSV seed to storm_history_dbt/seeds/zip_county_crosswalk.csv.

Source:
    https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/
    tab20_zcta520_county20_natl.txt

Notes:
  - ZCTAs (ZIP Code Tabulation Areas) are Census approximations of ZIP codes.
    They are not identical to USPS ZIP codes but are the closest freely available
    government geography.
  - The relationship is many-to-many: a single ZCTA can span multiple counties.
    area_ratio indicates what fraction of the ZCTA's land area falls in each
    county. Filter to area_ratio >= 0.5 to get one dominant county per ZIP.
  - county_fips_full is the 5-digit FIPS (2-digit state + 3-digit county).
    It joins to NOAA storm event data via:
        lpad(state_fips::text, 2, '0') || lpad(cz_fips::text, 3, '0')
    where cz_type = 'C' (county events only).
"""

import csv
import io
import sys
from pathlib import Path

import requests

SOURCE_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/"
    "tab20_zcta520_county20_natl.txt"
)

SEED_RELATIVE_PATH = Path("storm_history_dbt") / "seeds" / "zip_county_crosswalk.csv"

OUTPUT_COLUMNS = [
    "zip_code",          # 5-digit ZCTA
    "county_fips_full",  # 5-digit FIPS (state 2 + county 3)
    "state_fips",        # 2-digit state FIPS
    "county_fips",       # 3-digit county FIPS
    "county_name",       # e.g. "Jefferson County"
    "area_ratio",        # fraction of ZCTA land area in this county (0.0–1.0)
]


def download_raw(url: str, session: requests.Session) -> str:
    print(f"Downloading: {url}")
    response = session.get(url, timeout=60)
    response.raise_for_status()
    # Decode explicitly as utf-8-sig to strip the BOM that Census files include
    return response.content.decode("utf-8-sig")


def parse_and_write(raw_text: str, output_path: Path) -> int:
    reader = csv.DictReader(io.StringIO(raw_text), delimiter="|")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()

        for row in reader:
            zcta = row["GEOID_ZCTA5_20"].strip().zfill(5)
            county_full = row["GEOID_COUNTY_20"].strip().zfill(5)
            state_fips = county_full[:2]
            county_fips = county_full[2:]
            county_name = row["NAMELSAD_COUNTY_20"].strip()

            # Skip rows with no ZCTA or county (malformed lines)
            if not zcta.strip("0") or not county_full.strip("0"):
                continue

            # Compute area ratio: intersection land / total ZCTA land
            # Guard against divide-by-zero for water-only ZCTAs
            try:
                zcta_land = float(row["AREALAND_ZCTA5_20"])
                part_land = float(row["AREALAND_PART"])
                area_ratio = round(part_land / zcta_land, 6) if zcta_land > 0 else 0.0
            except (ValueError, KeyError):
                area_ratio = 0.0

            writer.writerow({
                "zip_code": zcta,
                "county_fips_full": county_full,
                "state_fips": state_fips,
                "county_fips": county_fips,
                "county_name": county_name,
                "area_ratio": area_ratio,
            })
            rows_written += 1

    return rows_written


def main() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    output_path = repo_root / SEED_RELATIVE_PATH

    session = requests.Session()
    raw_text = download_raw(SOURCE_URL, session)
    rows_written = parse_and_write(raw_text, output_path)

    print(f"Wrote {rows_written:,} rows to {output_path}")


if __name__ == "__main__":
    sys.exit(main())
