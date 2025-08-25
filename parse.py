#!/usr/bin/env python3
"""
JSON Parser Script for Event Analysis

This script parses JSON files to count total events and target events based on filter parameters.
"""

import json
import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Any


def load_json_file(file_path: str) -> Dict[str, Any]:
    """Load and parse JSON file."""
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in file '{file_path}': {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file '{file_path}': {e}")
        sys.exit(1)


def analyze_events(
    data: Dict[str, Any], filter_param: str, root_id: str
) -> Tuple[int, int, List[str], List[str]]:
    """
    Analyze events in the JSON data.

    Args:
        data: JSON data dictionary
        filter_param: Parameter to filter by (e.g., 'fbc')
        root_id: Root ID to match against

    Returns:
        Tuple of (total_events, target_events, target_event_ids, failed_event_ids)
    """
    total_events = 0
    target_events = 0
    target_event_ids = []
    failed_event_ids = []

    for event_id, event_data in data.items():
        total_events += 1

        # Check if this event has the target filter parameter
        has_filter_param = False

        # Look for the filter parameter in the output querystring field
        filter_key = f"output__{root_id}__querystring___{filter_param}"
        if filter_key in event_data and event_data[filter_key] is not None:
            has_filter_param = True

        if has_filter_param:
            target_events += 1
            target_event_ids.append(event_id)
        else:
            failed_event_ids.append(event_id)

    return total_events, target_events, target_event_ids, failed_event_ids


def format_output(
    total_events: int,
    target_events: int,
    target_event_ids: List[str],
    failed_event_ids: List[str],
    show_ids: bool = False,
) -> str:
    """Format the output string."""
    output_lines = [f"total events: {total_events}", f"target events: {target_events}"]

    if show_ids:
        if target_event_ids:
            output_lines.append("\nlist of ids of target events")
            output_lines.extend(target_event_ids)

        if failed_event_ids:
            output_lines.append("\nlist of ids of failed events")
            output_lines.extend(failed_event_ids)

    return "\n".join(output_lines)


def save_to_file(output_content: str, base_filename: str = None) -> str:
    """Save output to a file with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"{timestamp}.txt"

    try:
        with open(filename, "w", encoding="utf-8") as file:
            file.write(output_content)
        return filename
    except Exception as e:
        print(f"Error saving to file: {e}")
        return ""


def main():
    """Main function to parse command line arguments and execute the script."""
    parser = argparse.ArgumentParser(
        description="Parse JSON files to analyze events based on filter parameters",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python parse.py temp.json fbc 305546688
  python parse.py temp.json fbc 305546688 --show-ids
  python parse.py temp.json fbc 305546688 --show-ids --save-file
        """,
    )

    parser.add_argument(
        "file_name", help="Name of the JSON file to parse (e.g., temp.json)"
    )

    parser.add_argument(
        "target_param", help="Filter parameter to search for (e.g., fbc)"
    )

    parser.add_argument("root_id", help="Root ID to match against (e.g., 305546688)")

    parser.add_argument(
        "--show-ids",
        action="store_true",
        help="Show the list of event IDs that match the filter",
    )

    parser.add_argument(
        "--save-file",
        action="store_true",
        help="Save the result to a timestamped text file",
    )

    args = parser.parse_args()

    # Load and parse the JSON file
    print(f"Loading file: {args.file_name}")
    data = load_json_file(args.file_name)

    # Analyze the events
    print(f"Analyzing events with filter parameter: {args.target_param}")
    print(f"Root ID: {args.root_id}")

    total_events, target_events, target_event_ids, failed_event_ids = analyze_events(
        data, args.target_param, args.root_id
    )

    # Format the output
    output_content = format_output(
        total_events, target_events, target_event_ids, failed_event_ids, args.show_ids
    )

    # Display the results
    print("\n" + "=" * 50)
    print(output_content)
    print("=" * 50)

    # Save to file if requested
    if args.save_file:
        filename = save_to_file(output_content)
        if filename:
            print(f"\nResults saved to: {filename}")
        else:
            print("\nFailed to save results to file.")


if __name__ == "__main__":
    main()
