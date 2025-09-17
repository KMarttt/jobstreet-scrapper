import pandas as pd
import ast
import os
import glob
from collections import Counter
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
    Extract skills and knowledge from CSV file and compile into one big list.

    Args:
        csv_file_path (str): Path to the CSV file

    Returns:
        tuple: (unique_items, frequency_count, total_rows_processed)
    """

    try:
        # Read the CSV file
        df = pd.read_csv(csv_file_path)

        # Initialize list to store all skills and knowledge
        all_items = []
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
                        all_items.extend(skills)
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
                        all_items.extend(knowledge)
                        row_has_data = True
                except (ValueError, SyntaxError) as e:
                    print(
                        f"Warning: Error parsing knowledge_list at row {index} in {csv_file_path}: {e}")

            if row_has_data:
                rows_processed += 1

        # Remove duplicates while preserving order
        unique_items = list(dict.fromkeys(all_items))

        # Create frequency counter
        item_frequency = Counter(all_items)

        return unique_items, item_frequency, rows_processed

    except Exception as e:
        print(f"Error processing file {csv_file_path}: {e}")
        return [], Counter(), 0


def save_results_to_csv(unique_items, frequency_count, output_filename, source_info):
    """
    Save results to CSV file with skills/knowledge and their frequencies.

    Args:
        unique_items (list): List of unique skills and knowledge
        frequency_count (Counter): Counter object with frequencies
        output_filename (str): Output CSV filename
        source_info (dict): Information about source files processed
    """
    # Create DataFrame with skills/knowledge and frequencies
    results_data = []
    for item in unique_items:
        results_data.append({
            'skill_knowledge': item,
            'frequency': frequency_count[item],
            'type': 'skill/knowledge'
        })

    df_results = pd.DataFrame(results_data)

    # Sort by frequency (descending)
    df_results = df_results.sort_values('frequency', ascending=False)

    # Save to CSV
    df_results.to_csv(output_filename, index=False, encoding='utf-8')

    # Create summary info file
    summary_filename = output_filename.replace('.csv', '_summary.txt')
    with open(summary_filename, 'w', encoding='utf-8') as f:
        f.write("=== PROCESSING SUMMARY ===\n")
        f.write(f"Total files processed: {source_info['files_processed']}\n")
        f.write(f"Total rows with data: {source_info['total_rows']}\n")
        f.write(f"Total unique skills/knowledge: {len(unique_items)}\n")
        f.write(
            f"Total items (with duplicates): {sum(frequency_count.values())}\n\n")

        f.write("=== SOURCE FILES ===\n")
        for file_path in source_info['source_files']:
            f.write(f"{file_path}\n")

        f.write(f"\n=== TOP 20 MOST FREQUENT ITEMS ===\n")
        for item, count in frequency_count.most_common(20):
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
        all_unique_items = []
        combined_frequency = Counter()
        total_rows = 0
        files_processed = 0

        for csv_file in file_list:
            print(f"Processing: {os.path.basename(csv_file)}")
            unique_items, frequency_count, rows_processed = extract_skills_and_knowledge(
                csv_file)

            if unique_items:
                all_unique_items.extend(unique_items)
                combined_frequency.update(frequency_count)
                total_rows += rows_processed
                files_processed += 1
                print(
                    f"  - Found {len(unique_items)} unique items, {rows_processed} rows processed")
            else:
                print(f"  - No data extracted from this file")

        if all_unique_items:
            # Remove duplicates from combined list
            final_unique_items = list(dict.fromkeys(all_unique_items))

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
                final_unique_items, combined_frequency, output_filename, source_info)

            print(f"Results for {site_country}:")
            print(
                f"  - Total unique skills/knowledge: {len(final_unique_items)}")
            print(
                f"  - Total items (with duplicates): {sum(combined_frequency.values())}")
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
        unique_items, frequency_count, rows_processed = extract_skills_and_knowledge(
            csv_file_path)

        if not unique_items:
            print("No skills or knowledge data found in the file.")
            return

        print("=== EXTRACTION RESULTS ===")
        print(
            f"Total unique skills and knowledge items found: {len(unique_items)}")
        print(
            f"Total items (including duplicates): {sum(frequency_count.values())}")
        print(f"Rows processed: {rows_processed}")

        print("\n=== TOP 10 MOST FREQUENT ITEMS ===")
        for item, count in frequency_count.most_common(10):
            print(f"{item} ({count} times)")

        # Save results to CSV file
        source_info = {
            'files_processed': 1,
            'total_rows': rows_processed,
            'source_files': [csv_file_path]
        }
        save_results_to_csv(unique_items, frequency_count,
                            output_filename, source_info)

        # Optionally display all items
        show_all = input(
            "\nDo you want to display all unique items in the console? (y/n): ").lower().strip()
        if show_all == 'y':
            print("\n=== ALL UNIQUE SKILLS AND KNOWLEDGE ===")
            for i, item in enumerate(unique_items, 1):
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
