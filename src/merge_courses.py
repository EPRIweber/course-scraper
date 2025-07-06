#!/usr/bin/env python3
import json
import argparse
from typing import List, Dict, Tuple

def load_json(path: str) -> List[Dict]:
    """Load a JSON array of objects from a file."""
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def merge_courses(
    existing: List[Dict],
    scraped: List[Dict]
) -> Tuple[List[Dict], int, int]:
    """
    ## SANITY CHECK FOR UPLOADED DATA
    Merge scraped into existing using the same logic as your SQL MERGE:
      MATCH on (COALESCE(code,''), title)
        → UPDATE description & credits
      NOT MATCHED
        → INSERT new record
    Returns (merged_list, num_inserted, num_updated).
    """
    # Build lookup of existing by (code, title)
    lookup = {}
    for rec in existing:
        code = rec.get('course_code') or ""
        title = rec.get('course_title') or ""
        # copy to avoid mutating original
        lookup[(code, title)] = dict(rec)

    inserted = 0
    updated  = 0

    for rec in scraped:
        code = rec.get('course_code') or ""
        title = rec.get('course_title') or ""
        key = (code, title)

        if key in lookup:
            # UPDATE path: overwrite description & credits
            lookup[key]['course_description'] = rec.get('course_description')
            lookup[key]['course_credits']    = rec.get('course_credits')
            updated += 1
        else:
            # INSERT path
            lookup[key] = dict(rec)
            inserted += 1

    # Final merged list
    merged = list(lookup.values())
    return merged, inserted, updated

def main():
    p = argparse.ArgumentParser(
        description="Locally merge scraped courses into existing dataset, matching on (code,title)."
    )
    p.add_argument("existing", help="Path to existing courses JSON")
    p.add_argument("scraped",  help="Path to scraped courses JSON")
    p.add_argument(
        "-o", "--output",
        default="merged_courses.json",
        help="Where to write the merged JSON"
    )
    args = p.parse_args()

    existing = load_json(args.existing)
    scraped  = load_json(args.scraped)

    merged, inserted, updated = merge_courses(existing, scraped)

    # Write out merged list
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)

    # Summary
    print(f"Existing: {len(existing)}")
    print(f"Scraped:  {len(scraped)}")
    print(f"Merged:   {len(merged)}  (Inserted: {inserted}, Updated: {updated})")
    print(f"Merged JSON written to {args.output!r}")

if __name__ == "__main__":
    main()
