#!/usr/bin/env python3
"""Download Wikidata entities and save as JSONL for import into Entitybase.

Usage:
    python -m src.download_wikidata_entities -o data.jsonl Q42
    python -m src.download_wikidata_entities -o data.jsonl Q42 P31 L42
    python -m src.download_wikidata_entities -o data.jsonl --random-items 100
    python -m src.download_wikidata_entities -o data.jsonl --random-items 50 --random-properties 10 --random-lexemes 20
"""

import argparse
import json
import random
import sys
from pathlib import Path

import requests

USER_AGENT = "EntitybaseImport/1.0 (https://github.com/entitybase; mailto:dev@entitybase.org)"

WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"

MAX_ITEM_ID = 100_000_000
MAX_PROPERTY_ID = 10_000
MAX_LEXEME_ID = 100_000


def generate_random_entity_ids(entity_type: str, count: int, max_id: int) -> list[str]:
    """Generate random entity IDs within valid range."""
    prefix_map = {
        "item": "Q",
        "property": "P",
        "lexeme": "L",
    }
    prefix = prefix_map.get(entity_type.lower(), entity_type[0].upper())
    ids = []
    for _ in range(count):
        rand_id = random.randint(1, max_id)
        ids.append(f"{prefix}{rand_id}")
    return ids


def download_entity(entity_id: str) -> dict:
    """Download entity JSON from Wikidata."""
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{entity_id}.json"
    response = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    data = response.json()

    for key in data.get("entities", {}):
        if key.lower() == entity_id.lower():
            return data["entities"][key]

    raise ValueError(f"Entity {entity_id} not found in response")


def is_valid_entity_id(entity_id: str) -> bool:
    """Check if the string looks like a valid Wikidata entity ID."""
    if not entity_id:
        return False
    prefix = entity_id[0].upper()
    return prefix in ("Q", "P", "L") and entity_id[1:].isdigit() and len(entity_id) > 1


def cmd_download(args):
    """Download Wikidata entities and save as JSONL."""
    import time

    print(f"ID ranges: items Q1-Q{MAX_ITEM_ID:,}, properties P1-P{MAX_PROPERTY_ID:,}, lexemes L1-L{MAX_LEXEME_ID:,}")
    print()

    if args.seed is not None:
        random.seed(args.seed)

    total_requested = len(args.entity_ids) + args.random_items + args.random_properties + args.random_lexemes

    if total_requested == 0:
        print("No entities specified. Use entity IDs or --random-* options.")
        sys.exit(1)

    entity_ids = list(args.entity_ids)

    if args.random_items > 0:
        print(f"Fetching {args.random_items} random items...")
        entity_ids.extend(generate_random_entity_ids("item", args.random_items, MAX_ITEM_ID))

    if args.random_properties > 0:
        print(f"Fetching {args.random_properties} random properties...")
        entity_ids.extend(generate_random_entity_ids("property", args.random_properties, MAX_PROPERTY_ID))

    if args.random_lexemes > 0:
        print(f"Fetching {args.random_lexemes} random lexemes...")
        entity_ids.extend(generate_random_entity_ids("lexeme", args.random_lexemes, MAX_LEXEME_ID))

    invalid = [e for e in args.entity_ids if not is_valid_entity_id(e)]
    if invalid:
        print(f"Warning: Invalid entity IDs skipped: {invalid}", file=sys.stderr)
        entity_ids = [e for e in entity_ids if is_valid_entity_id(e)]

    if not entity_ids:
        print("No valid entities to download.", file=sys.stderr)
        sys.exit(1)

    output_path = args.output

    if not args.append and output_path.exists():
        response = input(f"Overwrite {output_path}? [y/N]: ").strip().lower()
        if response not in ("y", "yes"):
            print("Aborted.")
            sys.exit(1)

    print(f"Downloading {len(entity_ids)} entity(ies) to {output_path}...\n")

    start_time = time.time()
    success_count = 0
    fail_count = 0

    with open(output_path, "a" if args.append else "w") as f:
        for i, entity_id in enumerate(entity_ids):
            try:
                if args.verbose:
                    print(f"[{i+1}/{len(entity_ids)}] Downloading {entity_id}...")
                else:
                    bar_width = 30
                    percent = (i + 1) / len(entity_ids)
                    filled = int(bar_width * percent)
                    bar = "█" * filled + "░" * (bar_width - filled)
                    elapsed = time.time() - start_time
                    rate = (i + 1) / elapsed if elapsed > 0 else 0
                    print(f"\r  [{bar}] {i+1}/{len(entity_ids)} ({percent*100:5.1f}%) | {rate:.1f} entities/sec | {entity_id:<20}", end="")
                entity_data = download_entity(entity_id)
                json_line = json.dumps(entity_data, ensure_ascii=False)
                f.write(json_line + "\n")
                success_count += 1
            except Exception as e:
                fail_count += 1
                if args.verbose:
                    print(f"Error downloading {entity_id}: {e}", file=sys.stderr)

    elapsed = time.time() - start_time
    print(f"\n\nDone! {success_count} succeeded, {fail_count} failed in {elapsed:.1f}s")
    print(f"Wrote to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Download Wikidata entities and save as JSONL for import"
    )
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    download_parser = subparsers.add_parser('download', help='Download Wikidata entities to JSONL')
    download_parser.add_argument(
        "entity_ids",
        nargs="*",
        help="Specific Wikidata entity IDs (e.g., Q42, P31, L42)",
    )
    download_parser.add_argument(
        "--random-items", "-i",
        type=int,
        metavar="N",
        default=0,
        help="Download N random items (Q)",
    )
    download_parser.add_argument(
        "--random-properties", "-p",
        type=int,
        metavar="N",
        default=0,
        help="Download N random properties (P)",
    )
    download_parser.add_argument(
        "--random-lexemes", "-l",
        type=int,
        metavar="N",
        default=0,
        help="Download N random lexemes (L)",
    )
    download_parser.add_argument(
        "--output", "-o",
        type=Path,
        required=True,
        help="Output JSONL file path (required)",
    )
    download_parser.add_argument(
        "--append", "-a",
        action="store_true",
        help="Append to existing JSONL file instead of overwriting",
    )
    download_parser.add_argument(
        "--seed", "-s",
        type=int,
        default=None,
        help="Random seed for reproducibility",
    )
    download_parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print verbose output",
    )

    args = parser.parse_args()

    if args.command == 'download':
        cmd_download(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
