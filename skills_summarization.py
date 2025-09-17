import pandas as pd
import ast
import os
import glob
from collections import Counter, defaultdict
from pathlib import Path


def extract_site_country_from_filename(filename):
    """
    Extract site and country from filename following the pattern:
    site_country_search-term_final_complete_results_date_time

    Args:
        filename (str): The CSV filename

    Returns:
        tuple: (site, country) or (None, None) if pattern doesn't match
    """
    # Remove file extension if present
    base_name = os.path.splitext(filename)[0]

    # Split by underscore and extract first two parts
    parts = base_name.split('_')
    if len(parts) >= 2:
        site = parts[0]
        country = parts[1]
        return site, country
    return None, None


def extract_site_country_from_folder(folder_path):
    """
    Extract site and country from folder name following the pattern:
    processed_site_country

    Args:
        folder_path (str): The folder path

    Returns:
        tuple: (site, country) or (None, None) if pattern doesn't match
    """
    folder_name = os.path.basename(folder_path)

    # Remove 'processed_' prefix if it exists
    if folder_name.startswith('processed_'):
        folder_name = folder_name[10:]  # Remove 'processed_'

    # Split by underscore to get site and country
    parts = folder_name.split('_')
    if len(parts) >= 2:
        site = parts[0]
        country = parts[1]
        return site, country
    return None, None


def extract_skills_and_knowledge(csv_file_path):
    """
    Extract skills and knowledge from CSV file, keeping them separate.

    Args:
        csv_file_path (str): Path to the CSV file

    Returns:
        tuple: (skills_data, knowledge_data, total_rows_processed)
        where skills_data and knowledge_data are dicts with 'unique' and 'frequency' keys
    """

    try:
        # Read the CSV file
        df = pd.read_csv(csv_file_path)

        # Initialize lists to store skills and knowledge separately
        all_skills = []
        all_knowledge = []
        rows_processed = 0

        # Process each row
        for index, row in df.iterrows():
            row_has_data = False

            # Extract skills_list
            if pd.notna(row['skills_list']) and row['skills_list'] != '[]':
                try:
                    # Convert string representation of list to actual list
                    skills = ast.literal_eval(row['skills_list'])
                    if isinstance(skills, list):
                        all_skills.extend(skills)
                        row_has_data = True
                except (ValueError, SyntaxError) as e:
                    print(
                        f"Warning: Error parsing skills_list at row {index} in {csv_file_path}: {e}")

            # Extract knowledge_list
            if pd.notna(row['knowledge_list']) and row['knowledge_list'] != '[]':
                try:
                    # Convert string representation of list to actual list
                    knowledge = ast.literal_eval(row['knowledge_list'])
                    if isinstance(knowledge, list):
                        all_knowledge.extend(knowledge)
                        row_has_data = True
                except (ValueError, SyntaxError) as e:
                    print(
                        f"Warning: Error parsing knowledge_list at row {index} in {csv_file_path}: {e}")

            if row_has_data:
                rows_processed += 1

        # Process skills data
        unique_skills = list(dict.fromkeys(all_skills))
        skills_frequency = Counter(all_skills)
        skills_data = {
            'unique': unique_skills,
            'frequency': skills_frequency
        }

        # Process knowledge data
        unique_knowledge = list(dict.fromkeys(all_knowledge))
        knowledge_frequency = Counter(all_knowledge)
        knowledge_data = {
            'unique': unique_knowledge,
            'frequency': knowledge_frequency
        }

        return skills_data, knowledge_data, rows_processed

    except Exception as e:
        print(f"Error processing file {csv_file_path}: {e}")
        return {'unique': [], 'frequency': Counter()}, {'unique': [], 'frequency': Counter()}, 0


def save_results_to_csv(skills_data, knowledge_data, output_filename, source_info):
    """
    Save results to CSV file with separate columns for skills and knowledge.

    Args:
        skills_data (dict): Dictionary with skills unique list and frequency counter
        knowledge_data (dict): Dictionary with knowledge unique list and frequency counter
        output_filename (str): Output CSV filename
        source_info (dict): Information about source files processed
    """
    # Create DataFrame with separate columns for skills and knowledge
    results_data = []

    # Add skills
    for item in skills_data['unique']:
        results_data.append({
            'item': item,
            'frequency': skills_data['frequency'][item],
            'type': 'skill'
        })

    # Add knowledge
    for item in knowledge_data['unique']:
        results_data.append({
            'item': item,
            'frequency': knowledge_data['frequency'][item],
            'type': 'knowledge'
        })

    df_results = pd.DataFrame(results_data)

    # Sort by type first, then by frequency (descending)
    df_results = df_results.sort_values(
        ['type', 'frequency'], ascending=[True, False])

    # Save to CSV
    df_results.to_csv(output_filename, index=False, encoding='utf-8')

    # Create summary info file
    summary_filename = output_filename.replace('.csv', '_summary.txt')
    with open(summary_filename, 'w', encoding='utf-8') as f:
        f.write("=== PROCESSING SUMMARY ===\n")
        f.write(f"Total files processed: {source_info['files_processed']}\n")
        f.write(f"Total rows with data: {source_info['total_rows']}\n")
        f.write(f"Total unique skills: {len(skills_data['unique'])}\n")
        f.write(f"Total unique knowledge: {len(knowledge_data['unique'])}\n")
        f.write(
            f"Total skills (with duplicates): {sum(skills_data['frequency'].values())}\n")
        f.write(
            f"Total knowledge (with duplicates): {sum(knowledge_data['frequency'].values())}\n\n")

        f.write("=== SOURCE FILES ===\n")
        for file_path in source_info['source_files']:
            f.write(f"{file_path}\n")

        f.write(f"\n=== TOP 10 MOST FREQUENT SKILLS ===\n")
        for item, count in skills_data['frequency'].most_common(10):
            f.write(f"{item}: {count}\n")

        f.write(f"\n=== TOP 10 MOST FREQUENT KNOWLEDGE ===\n")
        for item, count in knowledge_data['frequency'].most_common(10):
            f.write(f"{item}: {count}\n")


def find_csv_files(base_directory):
    """
    Find all CSV files in the folder structure: processed_job_data/processed_site_country/*.csv

    Args:
        base_directory (str): Base directory path (processed_job_data)

    Returns:
        list: List of tuples (csv_file_path, site, country)
    """
    csv_files = []

    # Look for subdirectories that match the pattern processed_*
    pattern = os.path.join(base_directory, "processed_*")
    subdirs = glob.glob(pattern)

    for subdir in subdirs:
        if os.path.isdir(subdir):
            # Extract site and country from folder name
            site, country = extract_site_country_from_folder(subdir)

            # Find all CSV files in this subdirectory
            csv_pattern = os.path.join(subdir, "*.csv")
            csv_files_in_dir = glob.glob(csv_pattern)

            for csv_file in csv_files_in_dir:
                # If we couldn't get site/country from folder, try filename
                if not site or not country:
                    filename_site, filename_country = extract_site_country_from_filename(
                        os.path.basename(csv_file))
                    site = site or filename_site
                    country = country or filename_country

                csv_files.append((csv_file, site, country))

    return csv_files


def combine_data_dictionaries(data_list):
    """
    Combine multiple data dictionaries (skills or knowledge).

    Args:
        data_list (list): List of data dictionaries with 'unique' and 'frequency' keys

    Returns:
        dict: Combined data dictionary
    """
    all_items = []
    combined_frequency = Counter()

    for data in data_list:
        all_items.extend(data['unique'])
        combined_frequency.update(data['frequency'])

    # Remove duplicates while preserving order
    unique_items = list(dict.fromkeys(all_items))

    return {
        'unique': unique_items,
        'frequency': combined_frequency
    }


def process_batch():
    """
    Process all CSV files in batch mode.
    """
    # Get base directory from user
    base_dir = input(
        "Enter the path to your 'processed_job_data' directory (or press Enter for current directory): ").strip()
    if not base_dir:
        base_dir = "processed_job_data"

    if not os.path.exists(base_dir):
        print(f"Error: Directory '{base_dir}' does not exist.")
        return

    print(f"Searching for CSV files in: {base_dir}")

    # Find all CSV files
    csv_files = find_csv_files(base_dir)

    if not csv_files:
        print("No CSV files found in the specified directory structure.")
        return

    print(f"Found {len(csv_files)} CSV file(s) to process.")

    # Group files by site_country for combined processing
    grouped_files = {}
    for csv_file, site, country in csv_files:
        key = f"{site}_{country}"
        if key not in grouped_files:
            grouped_files[key] = []
        grouped_files[key].append(csv_file)

    # Process each group
    for site_country, file_list in grouped_files.items():
        print(f"\n=== Processing {site_country} ===")
        print(f"Files to process: {len(file_list)}")

        # Combine data from all files for this site_country
        skills_data_list = []
        knowledge_data_list = []
        total_rows = 0
        files_processed = 0

        for csv_file in file_list:
            print(f"Processing: {os.path.basename(csv_file)}")
            skills_data, knowledge_data, rows_processed = extract_skills_and_knowledge(
                csv_file)

            if skills_data['unique'] or knowledge_data['unique']:
                skills_data_list.append(skills_data)
                knowledge_data_list.append(knowledge_data)
                total_rows += rows_processed
                files_processed += 1
                print(
                    f"  - Found {len(skills_data['unique'])} unique skills, {len(knowledge_data['unique'])} unique knowledge, {rows_processed} rows processed")
            else:
                print(f"  - No data extracted from this file")

        if skills_data_list or knowledge_data_list:
            # Combine all data
            combined_skills = combine_data_dictionaries(skills_data_list)
            combined_knowledge = combine_data_dictionaries(knowledge_data_list)

            # Create output filename
            output_filename = f"{site_country}_skills.csv"

            # Prepare source info
            source_info = {
                'files_processed': files_processed,
                'total_rows': total_rows,
                'source_files': file_list
            }

            # Save results
            save_results_to_csv(
                combined_skills, combined_knowledge, output_filename, source_info)

            print(f"Results for {site_country}:")
            print(f"  - Total unique skills: {len(combined_skills['unique'])}")
            print(
                f"  - Total unique knowledge: {len(combined_knowledge['unique'])}")
            print(
                f"  - Total skills (with duplicates): {sum(combined_skills['frequency'].values())}")
            print(
                f"  - Total knowledge (with duplicates): {sum(combined_knowledge['frequency'].values())}")
            print(f"  - Files processed: {files_processed}")
            print(f"  - Output saved as: {output_filename}")
        else:
            print(f"No data found for {site_country}")


def process_single_file():
    """
    Process a single CSV file.
    """
    # Get input filename from user
    csv_file_path = input("Enter the path to your CSV file: ").strip()

    if not os.path.exists(csv_file_path):
        print(f"Error: File '{csv_file_path}' does not exist.")
        return

    try:
        # First try to extract from folder structure
        folder_path = os.path.dirname(csv_file_path)
        site, country = extract_site_country_from_folder(folder_path)

        # If that doesn't work, try filename
        if not site or not country:
            filename = os.path.basename(csv_file_path)
            site, country = extract_site_country_from_filename(filename)

        if site and country:
            output_filename = f"{site}_{country}_skills.csv"
            print(f"Detected site: {site}, country: {country}")
            print(f"Output will be saved as: {output_filename}")
        else:
            print("Could not parse site and country from filename or folder.")
            output_filename = "extracted_skills.csv"

        # Extract skills and knowledge
        skills_data, knowledge_data, rows_processed = extract_skills_and_knowledge(
            csv_file_path)

        if not skills_data['unique'] and not knowledge_data['unique']:
            print("No skills or knowledge data found in the file.")
            return

        print("=== EXTRACTION RESULTS ===")
        print(f"Total unique skills found: {len(skills_data['unique'])}")
        print(f"Total unique knowledge found: {len(knowledge_data['unique'])}")
        print(
            f"Total skills (including duplicates): {sum(skills_data['frequency'].values())}")
        print(
            f"Total knowledge (including duplicates): {sum(knowledge_data['frequency'].values())}")
        print(f"Rows processed: {rows_processed}")

        print("\n=== TOP 5 MOST FREQUENT SKILLS ===")
        for item, count in skills_data['frequency'].most_common(5):
            print(f"{item} ({count} times)")

        print("\n=== TOP 5 MOST FREQUENT KNOWLEDGE ===")
        for item, count in knowledge_data['frequency'].most_common(5):
            print(f"{item} ({count} times)")

        # Save results to CSV file
        source_info = {
            'files_processed': 1,
            'total_rows': rows_processed,
            'source_files': [csv_file_path]
        }
        save_results_to_csv(skills_data, knowledge_data,
                            output_filename, source_info)

        # Optionally display all items
        show_all = input(
            "\nDo you want to display all unique items in the console? (y/n): ").lower().strip()
        if show_all == 'y':
            print("\n=== ALL UNIQUE SKILLS ===")
            for i, item in enumerate(skills_data['unique'], 1):
                print(f"{i:3d}. {item}")

            print("\n=== ALL UNIQUE KNOWLEDGE ===")
            for i, item in enumerate(knowledge_data['unique'], 1):
                print(f"{i:3d}. {item}")

    except Exception as e:
        print(f"An error occurred: {e}")


def main():
    """
    Main function with options for single file or batch processing.
    """
    print("=== Skills and Knowledge Extractor ===")
    print("1. Process single file")
    print("2. Process all files (batch mode)")

    choice = input("\nSelect option (1 or 2): ").strip()

    if choice == "1":
        process_single_file()
    elif choice == "2":
        process_batch()
    else:
        print("Invalid choice. Please run the program again and select 1 or 2.")


if __name__ == "__main__":
    # Run the program
    main()
