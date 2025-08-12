from __future__ import annotations
import argparse, sys
from pathlib import Path
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
from .config import MAX_PROFILES_PER_DAY


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
    p.add_argument(
        "--skip-warmup",
        action="store_true",
        help="Skip browser profile warmup phase to start scraping immediately",
    )
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    input_path = Path(args.input).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    unmatched_output_path = output_path.parent / "unmatched_schools.xlsx"

    df_in = read_input(input_path)

    # handle --no-continue
    if args.no_continue and output_path.exists():
        if input("⚠️  --no-continue will erase existing output. Proceed? (y/N) ").lower() != "y":
            sys.exit("Aborted.")
        output_path.unlink()
        # Also remove the unmatched file when starting over
        if unmatched_output_path.exists():
            unmatched_output_path.unlink()

    df_out = read_output(output_path)
    already_done = set(df_out["id"]) if df_out is not None else set()

    unmatched_rows = []
    # Also load previously unmatched schools to avoid re-running them
    if unmatched_output_path.exists():
        df_unmatched_prev = read_output(unmatched_output_path)
        if df_unmatched_prev is not None and "id" in df_unmatched_prev.columns:
            already_done.update(set(df_unmatched_prev["id"]))
            unmatched_rows.extend(df_unmatched_prev.to_dict('records'))

    scraper = LinkedInScraper(skip_warmup=args.skip_warmup)
    scraper.login()

    rows = []
    consecutive_failures = 0

    for record in df_in.itertuples(index=False):
        school_id, school_name = record.id, record.name
        if school_id in already_done:
            # FIX: Only try to append previous results if they exist in the successful output dataframe.
            # This prevents an IndexError for schools that were previously "unmatched".
            if df_out is not None and school_id in df_out["id"].values:
                rows.append(df_out[df_out["id"] == school_id].iloc[0].to_dict())
            continue
        
        print(f"➡️  Iteration start: {school_name} ({school_id})")

        if len(rows) >= MAX_PROFILES_PER_DAY:
            print(f"🏁 Daily limit of {MAX_PROFILES_PER_DAY} profiles reached. Exiting.")
            break

        tmp_frag = output_path.parent / f"{school_id}.contacts.tmp"
        wipe_fragments(tmp_frag)

        try:
            # Log start of school
            print(f"▶️  Starting: {school_name} ({school_id})")
            scraper.search_school(school_name)
            
            # Prepare a row for the current school.
            # We'll append contacts to this row as they are scraped.
            school_row = {"id": school_id, "name": school_name, "contacts": []}
            rows.append(school_row)
            
            # Iteratively process contacts and save after each one
            for contact in scraper.harvest_profiles(school_name):
                school_row["contacts"].append(contact)
                # Atomically write the entire updated dataframe to Excel
                atomic_write_excel(pd.DataFrame(rows), output_path)

            # Log snapshot after completing a school
            try:
                scraper.log_network_snapshot({"phase": "after_school", "school": school_name})
            except Exception:
                pass

            consecutive_failures = 0  # success resets failure counter

        # CATCH THE NEW EXCEPTION SEPARATELY
        except NoGoodMatchFound as e:
            print(f"🟡 Skipping school: {e}")
            unmatched_rows.append({"id": school_id, "name": school_name})
            
            # Write to unmatched file immediately (real-time updates)
            print(f"📝 Adding '{school_name}' to unmatched schools file...")
            unmatched_df = pd.DataFrame(unmatched_rows)
            atomic_write_excel(unmatched_df, unmatched_output_path)
            
            # Also write the main output file
            atomic_write_excel(pd.DataFrame(rows), output_path)
            consecutive_failures = 0  # not counted as fatal failure
            continue # Move to the next school

        except Exception as e:
            # Get the exception type name
            error_type = type(e).__name__
            error_msg = str(e) if str(e) else "No error message"
            print(f"❌  Error on {school_name}: {error_type}: {error_msg}", file=sys.stderr)
            
            # Print more detailed traceback for debugging
            import traceback
            traceback.print_exc(file=sys.stderr)
            
            consecutive_failures += 1
            if consecutive_failures >= 3:
                print("⛔ Detected 3 consecutive failures. Assuming temporary restriction. Exiting gracefully.")
                break
            continue
        finally:
            wipe_fragments(tmp_frag)

    scraper.close()
    
    # Final write of all successful rows
    if rows:
        atomic_write_excel(pd.DataFrame(rows), output_path)
    
    # WRITE THE UNMATCHED SCHOOLS FILE AT THE END
    if unmatched_rows:
        print(f"ℹ️  Writing {len(unmatched_rows)} unmatched schools to {unmatched_output_path}")
        unmatched_df = pd.DataFrame(unmatched_rows)
        atomic_write_excel(unmatched_df, unmatched_output_path)

    print(f"✅  Finished. Results in {output_path}")


if __name__ == "__main__":
    main()