import os
import sys
import time
import logging
from typing import List, Dict
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa
import pyarrow.parquet as pq
import scripts.keiba_scraper as ks  # Function to retrieve data from keibascraper library


def setup_logging():
    """
    Configures the logging settings for the script.
    Logs are written to both the console and a file named 'keibascraper.log'.
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
        If an invalid number of arguments or invalid race_id length is provided.
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
        List[str]: A list of expanded race IDs.

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
        race_id_list (List[str]): A list of 12-digit race ID strings.

    Returns:
        Dict[str, List[str]]: A dictionary with 10-digit race ID prefixes as keys and lists of 12-digit race IDs as values.
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
        race (Dict): The first dataset containing race information with an "entry" key.
        odds (List[Dict]): The second dataset containing odds information, each with an "id" key.

    Returns:
        Dict: The merged race data.
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


def process_race_id(race_id: str) -> Dict:
    """
    Processes a single race ID by loading race and odds data, merging them.

    Parameters:
        race_id (str): A 12-digit race ID.

    Returns:
        Dict: The merged race data if successful, None otherwise.
    """
    try:
        # Load data using keibascraper
        race_info = ks.load('result', race_id)
        odds_info = ks.load('odds', race_id)
        merged_race = merge_race_and_odds(race_info, odds_info)
        return merged_race
    except Exception as e:
        logging.error(f"Error processing race ID {race_id}: {e}")
        return None


def collect_race(file_id: str, group_ids: List[str], output_dir: str, max_workers: int = 10) -> List[str]:
    """
    Processes a group of race IDs: loads data, merges race and odds, and saves to Parquet.

    Parameters:
        file_id (str): The 10-digit prefix used as the filename.
        group_ids (List[str]): A list of 12-digit race IDs.
        output_dir (str): Directory where Parquet files are saved.
        max_workers (int): The maximum number of worker threads.

    Returns:
        List[str]: A list of horse_ids extracted from the merged races.
    """
    start_time = time.perf_counter()
    race_path = os.path.join(output_dir, f"{file_id}.parquet")
    races = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all race_ids to the executor
        future_to_race_id = {executor.submit(process_race_id, race_id): race_id for race_id in group_ids}
        for future in as_completed(future_to_race_id):
            race_id = future_to_race_id[future]
            result = future.result()
            if result:
                races.append(result)
    
    if races:
        try:
            # Save nested data to Parquet format
            table = pa.Table.from_pylist(races)
            pq.write_table(table, race_path)
            logging.info(f"Saved {len(races)} races to {race_path}")
        except Exception as e:
            logging.error(f"Error saving Parquet file {race_path}: {e}")
    else:
        logging.warning(f"No valid race data to save for {file_id}.parquet")

    # Extract horse_ids from merged races
    horse_ids = set()
    for race in races:
        entries = race.get("entry", [])
        for entry in entries:
            horse_id = entry.get("horse_id")
            if horse_id:
                horse_ids.add(horse_id)
            else:
                logging.warning(f"Entry missing 'horse_id': {entry}")

    # Record the end time
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    logging.info(f"Completed collecting {race_path}: {elapsed_time:.6f} seconds")
    
    return list(horse_ids)


def process_horse_id(horse_id: str) -> None:
    """
    Processes a single horse ID by loading horse data and saving it to Parquet.

    Parameters:
        horse_id (str): A horse ID.
    """
    horse_path = os.path.join("/data/horse", f"{horse_id}.parquet")
    try:
        # Load data using keibascraper
        horse = ks.load('horse', horse_id)
        if horse:
            # Save nested data to Parquet format
            table = pa.Table.from_pylist([horse])
            pq.write_table(table, horse_path)
            logging.info(f"Saved horse data for {horse_id} to {horse_path}")
        else:
            logging.warning(f"No data found for horse ID {horse_id}.")
    except Exception as e:
        logging.error(f"Error processing horse ID {horse_id}: {e}")


def collect_horse(horses: List[str], output_dir: str, max_workers: int = 10) -> None:
    """
    Collects horse data by race and saves to Parquet files.

    Parameters:
        horses (List[str]): A list of unique horse IDs to collect data for.
        output_dir (str): Directory where Parquet files are saved.
        max_workers (int): The maximum number of worker threads.
    """
    # Skip collecting horse data that already exists
    existing_files = set(os.path.splitext(file)[0] for file in os.listdir(output_dir))    
    horse_ids = list(set(horses) - existing_files)
    logging.info(f"Starting to collect horse data: {len(horse_ids)} records to process.")

    start_time = time.perf_counter()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_horse_id, horse_id): horse_id for horse_id in horse_ids}
        for future in as_completed(futures):
            horse_id = futures[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"Unhandled exception for horse ID {horse_id}: {e}")

    # Record the end time
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    logging.info(f"Completed collecting horse data: {elapsed_time:.6f} seconds")


def main():
    """
    Main function that orchestrates the processing of race IDs.
    """
    setup_logging()

    # Parse arguments to get race_ids
    race_ids = parse_arguments()

    # Group race_ids by their first 10 digits
    grouped_race_ids = group_race_ids(race_ids)

    # Define the output directories
    race_dir = '/data/race'
    horse_dir = '/data/horse'
    os.makedirs(race_dir, exist_ok=True)
    os.makedirs(horse_dir, exist_ok=True)

    # Process each group and collect horse IDs
    horses = []
    for file_id, group_ids in grouped_race_ids.items():
        logging.info(f"Processing group {file_id} with {len(group_ids)} race IDs.")
        horse_ids = collect_race(file_id, group_ids, race_dir)
        horses.extend(horse_ids)

    # Remove duplicates and convert to list
    unique_horses = list(set(horses))
    logging.info(f"Total unique horse IDs to collect: {len(unique_horses)}")

    # Collect horse data
    collect_horse(unique_horses, horse_dir)


if __name__ == "__main__":
    main()