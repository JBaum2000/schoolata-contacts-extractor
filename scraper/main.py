from __future__ import annotations
import argparse, sys
from pathlib import Path
from tqdm import tqdm
import pandas as pd
import traceback
traceback.print_exc()

# ⬇️  Excel-aware helpers
from .io_utils import (
    read_input,
    read_output,
    atomic_write_excel,
    append_contact_fragment,
    merge_fragments,
    wipe_fragments,
    OUTPUT_DEFAULT,
)
from .linkedin_scraper import LinkedInScraper, NoGoodMatchFound


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Extract LinkedIn contacts and store results in an Excel file."
    )
    p.add_argument("--input", required=True, help="INPUT .xlsx with columns id,name")
    p.add_argument(
        "--output",
        default=str(OUTPUT_DEFAULT),
        help="OUTPUT .xlsx (appends unless --no-continue)",
    )
    p.add_argument(
        "--no-continue",
        action="store_true",
        help="Discard existing output workbook and start over",
    )
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    input_path = Path(args.input).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()

    df_in = read_input(input_path)

    # handle --no-continue
    if args.no_continue and output_path.exists():
        if input("⚠️  --no-continue will erase existing output. Proceed? (y/N) ").lower() != "y":
            sys.exit("Aborted.")
        output_path.unlink()

    df_out = read_output(output_path)
    already_done = set(df_out["id"]) if df_out is not None else set()

    scraper = LinkedInScraper()
    scraper.login()

    rows = []
    unmatched_rows = []

    for record in tqdm(df_in.itertuples(index=False), total=len(df_in)):
        school_id, school_name = record.id, record.name
        if school_id in already_done:
            rows.append(df_out[df_out["id"] == school_id].iloc[0].to_dict())
            continue

        tmp_frag = output_path.parent / f"{school_id}.contacts.tmp"
        wipe_fragments(tmp_frag)

        try:
            scraper.search_school(school_name)
            contacts = scraper.harvest_profiles(school_name)
            rows.append({"id": school_id, "name": school_name, "contacts": contacts})

        # CATCH THE NEW EXCEPTION SEPARATELY
        except NoGoodMatchFound as e:
            print(f"🟡 Skipping school: {e}")
            unmatched_rows.append({"id": school_id, "name": school_name})
            continue # Move to the next school

        except Exception as e:
            print(f"❌  Error on {school_name}: {e}", file=sys.stderr)
            continue
        finally:
            wipe_fragments(tmp_frag)

        # ➡️  incremental Excel write
        if rows: # Ensure rows is not empty before creating DataFrame
             atomic_write_excel(pd.DataFrame(rows), output_path)

    scraper.close()
    
    # Final write of all successful rows
    if rows:
        atomic_write_excel(pd.DataFrame(rows), output_path)
    
    # WRITE THE UNMATCHED SCHOOLS FILE AT THE END
    if unmatched_rows:
        unmatched_output_path = output_path.parent / "unmatched_schools.xlsx"
        print(f"ℹ️  Writing {len(unmatched_rows)} unmatched schools to {unmatched_output_path}")
        unmatched_df = pd.DataFrame(unmatched_rows)
        atomic_write_excel(unmatched_df, unmatched_output_path)

    print(f"✅  Finished. Results in {output_path}")


if __name__ == "__main__":
    main()