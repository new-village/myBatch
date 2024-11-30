import os
import sys
import time
import logging
from typing import List, Dict
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq
import keibascraper as ks  # Function to retrieve data from keibascraper library


def setup_logging():
    """
    Configures the logging settings for the script.
    Logs are written to both console and a file named 'keibascraper.log'.
    """
    logging.basicConfig(
        level=logging.INFO,  # Change to DEBUG for more verbosity
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("keibascraper.log")
        ]
    )


def parse_arguments() -> List[str]:
    """
    Parses command-line arguments and returns the list of race IDs.

    The argument can be:
        - 6 digits: representing year and month.
        - 10 digits: a race list ID to be expanded.
        - 12 digits: a single race ID.

    Returns:
        List[str]: A list of race IDs to process.

    Exits:
        If invalid number of arguments or invalid race_id length.
    """
    if len(sys.argv) < 2:
        logging.error("Usage: python -m batch_scripts.keibascraper <parameter: 6 digit or 10 digit or 12 digit>")
        sys.exit(1)

    race_id = sys.argv[1]
    if len(race_id) == 10:
        try:
            race_ids = expand_race_id(race_id)
            logging.info(f"Expanded 10-digit race ID '{race_id}' into {len(race_ids)} race IDs.")
        except ValueError as e:
            logging.error(e)
            sys.exit(1)
    elif len(race_id) == 6:
        year = race_id[:4]
        month = race_id[-2:]
        try:
            race_ids = ks.race_list(year, month)
            if not race_ids:
                logging.error(f"No race IDs found for year {year} and month {month}.")
                sys.exit(1)
            logging.info(f"Loaded {len(race_ids)} race IDs for year {year} and month {month}.")
        except Exception as e:
            logging.error(f"Failed to load race list for year {year} and month {month}: {e}")
            sys.exit(1)
    elif len(race_id) == 12:
        race_ids = [race_id]
        logging.info(f"Single 12-digit race ID provided: {race_id}")
    else:
        logging.error(f"Invalid parameter length: {len(race_id)}. Expected 6, 10, or 12 digits.")
        sys.exit(1)

    logging.info(f"Total race number for collecting: {len(race_ids)}")
    return race_ids


def expand_race_id(race_id: str) -> List[str]:
    """
    Expands a 10-digit race ID by appending '01' to '12', resulting in 12 12-digit race IDs.

    If the race ID is already 12 digits, it returns a list containing the race ID as is.

    Parameters:
        race_id (str): The input race ID.

    Returns:
        list: A list of expanded race IDs.

    Raises:
        ValueError: If the race_id length is not 10 or 12.
    """
    if len(race_id) == 12:
        return [race_id]
    elif len(race_id) == 10:
        return [f"{race_id}{str(i).zfill(2)}" for i in range(1, 13)]
    else:
        raise ValueError(f"Invalid race_id length ({len(race_id)}): {race_id}")


def group_race_ids(race_id_list: List[str]) -> Dict[str, List[str]]:
    """
    Groups a list of 12-digit race IDs into a dictionary based on their first 10 digits.

    Parameters:
        race_id_list (list): A list of 12-digit race ID strings.

    Returns:
        dict: A dictionary with 10-digit race ID prefixes as keys and lists of 12-digit race IDs as values.
    """
    grouped_race_ids = defaultdict(list)
    
    for race_id in race_id_list:
        if not isinstance(race_id, str):
            logging.warning(f"race_id '{race_id}' is not a string. Skipping.")
            continue
        
        if len(race_id) != 12:
            logging.warning(f"race_id '{race_id}' has invalid length ({len(race_id)}). Skipping.")
            continue
        
        prefix = race_id[:10]  # Extract the first 10 digits
        grouped_race_ids[prefix].append(race_id)
    
    logging.info(f"Grouped race IDs into {len(grouped_race_ids)} groups based on 10-digit prefixes.")
    return dict(grouped_race_ids)


def merge_race_and_odds(race: Dict, odds: List[Dict]) -> Dict:
    """
    Merges race data with odds data based on matching 'id' fields.

    Parameters:
        race (dict): The first dataset containing race information with an "entry" key.
        odds (list): The second dataset containing odds information, each with an "id" key.

    Returns:
        dict: The merged race data.
    """
    if 'entry' not in race:
        raise ValueError("The race data does not contain 'entry' key.")

    # Create a dictionary for quick lookup of odds by 'id'
    odds_lookup = {item['id']: item for item in odds if 'id' in item}

    merged_entries = []
    for entry in race.get('entry', []):
        entry_id = entry.get('id')
        if not entry_id:
            logging.warning("An entry in race data does not have an 'id' key. Entry skipped.")
            continue

        if entry_id in odds_lookup:
            # Merge the two dictionaries. Odds data values will overwrite race data if keys overlap.
            merged_entry = {**entry, **odds_lookup[entry_id]}
            merged_entries.append(merged_entry)
        else:
            # If no matching odds data, keep the entry as is
            logging.info(f"No matching odds data for entry ID {entry_id}. Entry added without merging.")
            merged_entries.append(entry)

    # Update the race data with merged entries
    race["entry"] = merged_entries
    return race


def process_group(file_id: str, group_ids: List[str], output_dir: str) -> None:
    """
    Processes a group of race IDs: loads data, merges race and odds, and saves to Parquet.

    Parameters:
        file_id (str): The 10-digit prefix used as the filename.
        group_ids (List[str]): A list of 12-digit race IDs.
        output_dir (str): Directory where Parquet files are saved.
    """
    start_time = time.perf_counter()
    race_path = os.path.join(output_dir, f"{file_id}.parquet")
    races = []

    for race_id in group_ids:
        try:
            # Load data using keibascraper
            race_info = ks.load('result', race_id)
            odds_info = ks.load('odds', race_id)
            merged_race = merge_race_and_odds(race_info, odds_info)
            races.append(merged_race)
        except Exception as e:
            logging.error(f"Error processing race ID {race_id}: {e}")

    if races:
        # Save nested data to Parquet format
        table = pa.Table.from_pylist(races)
        os.makedirs(output_dir, exist_ok=True)  # Ensure directory exists
        pq.write_table(table, race_path)
        logging.info(f"Saved {len(races)} races to {race_path}")
    else:
        logging.warning(f"No valid race data to save for {file_id}.parquet")

    # Record the end time
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    logging.info(f"Completed collecting {race_path}: {elapsed_time:.6f} seconds")


def main():
    """
    Main function that orchestrates the processing of race IDs.
    """
    setup_logging()

    # Parse arguments to get race_ids
    race_ids = parse_arguments()

    # Group race_ids by their first 10 digits
    grouped_race_ids = group_race_ids(race_ids)

    # Define the output directory
    race_dir = "data/race"
    os.makedirs(race_dir, exist_ok=True)  # Ensure directory exists

    # Process each group
    for file_id, group_ids in grouped_race_ids.items():
        process_group(file_id, group_ids, race_dir)


if __name__ == "__main__":
    main()
