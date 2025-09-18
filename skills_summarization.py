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


def clean_and_normalize_value(value):
    """
    Clean and normalize job-related values for consistent grouping.

    Args:
        value: The value to clean (job_title, job_level, or year_of_experience)

    Returns:
        str: Cleaned and normalized value
    """
    if pd.isna(value) or value is None:
        return "Not Specified"

    # Convert to string and strip whitespace
    cleaned = str(value).strip()

    # Handle empty strings
    if not cleaned or cleaned.lower() in ['', 'na', 'n/a', 'null', 'none']:
        return "Not Specified"

    # For years of experience, normalize common patterns
    if cleaned.lower().replace(' ', '').replace('-', '').replace('+', '').isdigit():
        # Extract numeric part for years
        import re
        numbers = re.findall(r'\d+', cleaned)
        if numbers:
            return f"{numbers[0]} years" if len(numbers) == 1 else f"{numbers[0]}-{numbers[-1]} years"

    return cleaned


def extract_skills_and_knowledge(csv_file_path):
    """
    Extract skills and knowledge from CSV file with associated job information.

    Args:
        csv_file_path (str): Path to the CSV file

    Returns:
        tuple: (skills_associations, knowledge_associations, total_rows_processed)
        where associations are dicts mapping items to job info
    """

    try:
        # Read the CSV file
        df = pd.read_csv(csv_file_path)

        # Initialize dictionaries to store associations
        skills_associations = defaultdict(lambda: {
            'frequency': 0,
            'job_titles': [],
            'job_levels': [],
            'years_of_experience': []
        })

        knowledge_associations = defaultdict(lambda: {
            'frequency': 0,
            'job_titles': [],
            'job_levels': [],
            'years_of_experience': []
        })

        rows_processed = 0

        # Process each row
        for index, row in df.iterrows():
            row_has_data = False

            # Extract job information for this row
            job_title = clean_and_normalize_value(row.get('title'))
            job_level = clean_and_normalize_value(row.get('job_level'))
            year_of_experience = clean_and_normalize_value(
                row.get('year_of_experience'))

            # Extract skills_list
            if pd.notna(row['skills_list']) and row['skills_list'] != '[]':
                try:
                    # Convert string representation of list to actual list
                    skills = ast.literal_eval(row['skills_list'])
                    if isinstance(skills, list):
                        for skill in skills:
                            if skill:  # Only process non-empty skills
                                skills_associations[skill]['frequency'] += 1
                                skills_associations[skill]['job_titles'].append(
                                    job_title)
                                skills_associations[skill]['job_levels'].append(
                                    job_level)
                                skills_associations[skill]['years_of_experience'].append(
                                    year_of_experience)
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
                        for knowledge_item in knowledge:
                            if knowledge_item:  # Only process non-empty knowledge
                                knowledge_associations[knowledge_item]['frequency'] += 1
                                knowledge_associations[knowledge_item]['job_titles'].append(
                                    job_title)
                                knowledge_associations[knowledge_item]['job_levels'].append(
                                    job_level)
                                knowledge_associations[knowledge_item]['years_of_experience'].append(
                                    year_of_experience)
                        row_has_data = True
                except (ValueError, SyntaxError) as e:
                    print(
                        f"Warning: Error parsing knowledge_list at row {index} in {csv_file_path}: {e}")

            if row_has_data:
                rows_processed += 1

        return dict(skills_associations), dict(knowledge_associations), rows_processed

    except Exception as e:
        print(f"Error processing file {csv_file_path}: {e}")
        return {}, {}, 0


def summarize_associations(associations_list):
    """
    Summarize and deduplicate job associations across multiple files.

    Args:
        associations_list (list): List of association dictionaries

    Returns:
        dict: Combined associations with deduplicated job information
    """
    combined_associations = defaultdict(lambda: {
        'frequency': 0,
        'job_titles': [],
        'job_levels': [],
        'years_of_experience': []
    })

    for associations in associations_list:
        for item, data in associations.items():
            combined_associations[item]['frequency'] += data['frequency']
            combined_associations[item]['job_titles'].extend(
                data['job_titles'])
            combined_associations[item]['job_levels'].extend(
                data['job_levels'])
            combined_associations[item]['years_of_experience'].extend(
                data['years_of_experience'])

    # Deduplicate and summarize job information for each item
    for item, data in combined_associations.items():
        # Get unique values and their counts
        job_titles_counter = Counter(data['job_titles'])
        job_levels_counter = Counter(data['job_levels'])
        years_counter = Counter(data['years_of_experience'])

        # Create summary lists (top 5 most common, with counts)
        combined_associations[item]['job_titles'] = [
            f"{title} ({count})" for title, count in job_titles_counter.most_common(10)
        ]
        combined_associations[item]['job_levels'] = [
            f"{level} ({count})" for level, count in job_levels_counter.most_common(10)
        ]
        combined_associations[item]['years_of_experience'] = [
            f"{years} ({count})" for years, count in years_counter.most_common(10)
        ]

    return dict(combined_associations)


def save_results_to_csv(skills_associations, knowledge_associations, output_filename, source_info):
    """
    Save results to CSV file with the specified column structure.

    Args:
        skills_associations (dict): Dictionary with skills and their job associations
        knowledge_associations (dict): Dictionary with knowledge and their job associations
        output_filename (str): Output CSV filename
        source_info (dict): Information about source files processed
    """
    # Create DataFrame with the exact columns requested
    results_data = []

    # Add skills
    for item, data in skills_associations.items():
        results_data.append({
            'type': 'skill',
            'item': item,
            'frequency': data['frequency'],
            'job_title': data['job_titles'] if data['job_titles'] else ['Not Available'],
            'years of experience': data['years_of_experience'] if data['years_of_experience'] else ['Not Available'],
            'job_levels': data['job_levels'] if data['job_levels'] else ['Not Available']
        })

    # Add knowledge
    for item, data in knowledge_associations.items():
        results_data.append({
            'type': 'knowledge',
            'item': item,
            'frequency': data['frequency'],
            'job_title': data['job_titles'] if data['job_titles'] else ['Not Available'],
            'years of experience': data['years_of_experience'] if data['years_of_experience'] else ['Not Available'],
            'job_levels': data['job_levels'] if data['job_levels'] else ['Not Available']
        })

    df_results = pd.DataFrame(results_data)

    # Sort by type first, then by frequency (descending)
    df_results = df_results.sort_values(
        ['type', 'frequency'], ascending=[True, False])

    # Save to CSV
    df_results.to_csv(output_filename, index=False, encoding='utf-8')
    print(f"Saved detailed results to: {output_filename}")

    # Create summary info file
    summary_filename = output_filename.replace('.csv', '_summary.txt')
    with open(summary_filename, 'w', encoding='utf-8') as f:
        f.write("=== PROCESSING SUMMARY ===\n")
        f.write(f"Total files processed: {source_info['files_processed']}\n")
        f.write(f"Total rows with data: {source_info['total_rows']}\n")
        f.write(f"Total unique skills: {len(skills_associations)}\n")
        f.write(f"Total unique knowledge: {len(knowledge_associations)}\n")

        total_skills_freq = sum(data['frequency']
                                for data in skills_associations.values())
        total_knowledge_freq = sum(data['frequency']
                                   for data in knowledge_associations.values())
        f.write(f"Total skills (with duplicates): {total_skills_freq}\n")
        f.write(
            f"Total knowledge (with duplicates): {total_knowledge_freq}\n\n")

        f.write("=== SOURCE FILES ===\n")
        for file_path in source_info['source_files']:
            f.write(f"{file_path}\n")

        f.write(f"\n=== TOP 10 MOST FREQUENT SKILLS ===\n")
        skills_by_freq = sorted(skills_associations.items(
        ), key=lambda x: x[1]['frequency'], reverse=True)
        for item, data in skills_by_freq[:10]:
            f.write(f"{item}: {data['frequency']} occurrences\n")

        f.write(f"\n=== TOP 10 MOST FREQUENT KNOWLEDGE ===\n")
        knowledge_by_freq = sorted(knowledge_associations.items(
        ), key=lambda x: x[1]['frequency'], reverse=True)
        for item, data in knowledge_by_freq[:10]:
            f.write(f"{item}: {data['frequency']} occurrences\n")

    print(f"Saved summary to: {summary_filename}")


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
        skills_associations_list = []
        knowledge_associations_list = []
        total_rows = 0
        files_processed = 0

        for csv_file in file_list:
            print(f"Processing: {os.path.basename(csv_file)}")
            skills_assoc, knowledge_assoc, rows_processed = extract_skills_and_knowledge(
                csv_file)

            if skills_assoc or knowledge_assoc:
                skills_associations_list.append(skills_assoc)
                knowledge_associations_list.append(knowledge_assoc)
                total_rows += rows_processed
                files_processed += 1
                print(
                    f"  - Found {len(skills_assoc)} unique skills, {len(knowledge_assoc)} unique knowledge, {rows_processed} rows processed")
            else:
                print(f"  - No data extracted from this file")

        if skills_associations_list or knowledge_associations_list:
            # Combine all associations
            combined_skills = summarize_associations(skills_associations_list)
            combined_knowledge = summarize_associations(
                knowledge_associations_list)

            # Create output filename
            output_filename = f"{site_country}_skills_with_jobs.csv"

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
            print(f"  - Total unique skills: {len(combined_skills)}")
            print(f"  - Total unique knowledge: {len(combined_knowledge)}")
            total_skills_freq = sum(data['frequency']
                                    for data in combined_skills.values())
            total_knowledge_freq = sum(data['frequency']
                                       for data in combined_knowledge.values())
            print(f"  - Total skills (with duplicates): {total_skills_freq}")
            print(
                f"  - Total knowledge (with duplicates): {total_knowledge_freq}")
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
            output_filename = f"{site}_{country}_skills_with_jobs.csv"
            print(f"Detected site: {site}, country: {country}")
            print(f"Output will be saved as: {output_filename}")
        else:
            print("Could not parse site and country from filename or folder.")
            output_filename = "extracted_skills_with_jobs.csv"

        # Extract skills and knowledge with job associations
        skills_associations, knowledge_associations, rows_processed = extract_skills_and_knowledge(
            csv_file_path)

        if not skills_associations and not knowledge_associations:
            print("No skills or knowledge data found in the file.")
            return

        print("=== EXTRACTION RESULTS ===")
        print(f"Total unique skills found: {len(skills_associations)}")
        print(f"Total unique knowledge found: {len(knowledge_associations)}")

        total_skills_freq = sum(data['frequency']
                                for data in skills_associations.values())
        total_knowledge_freq = sum(data['frequency']
                                   for data in knowledge_associations.values())
        print(f"Total skills (including duplicates): {total_skills_freq}")
        print(
            f"Total knowledge (including duplicates): {total_knowledge_freq}")
        print(f"Rows processed: {rows_processed}")

        print("\n=== TOP 5 MOST FREQUENT SKILLS ===")
        skills_by_freq = sorted(skills_associations.items(
        ), key=lambda x: x[1]['frequency'], reverse=True)
        for item, data in skills_by_freq[:5]:
            print(f"{item} ({data['frequency']} times)")

        print("\n=== TOP 5 MOST FREQUENT KNOWLEDGE ===")
        knowledge_by_freq = sorted(knowledge_associations.items(
        ), key=lambda x: x[1]['frequency'], reverse=True)
        for item, data in knowledge_by_freq[:5]:
            print(f"{item} ({data['frequency']} times)")

        # Save results to CSV file
        source_info = {
            'files_processed': 1,
            'total_rows': rows_processed,
            'source_files': [csv_file_path]
        }
        save_results_to_csv(
            skills_associations, knowledge_associations, output_filename, source_info)

        # Optionally display detailed info for specific items
        show_details = input(
            "\nDo you want to see job associations for top skills/knowledge? (y/n): ").lower().strip()
        if show_details == 'y':
            print("\n=== TOP 3 SKILLS WITH JOB ASSOCIATIONS ===")
            for item, data in skills_by_freq[:3]:
                print(f"\n{item} (appears {data['frequency']} times):")
                print(f"  Job Titles: {'; '.join(data['job_titles'][:5])}")
                print(f"  Job Levels: {'; '.join(data['job_levels'][:5])}")
                print(
                    f"  Experience: {'; '.join(data['years_of_experience'][:5])}")

            print("\n=== TOP 3 KNOWLEDGE WITH JOB ASSOCIATIONS ===")
            for item, data in knowledge_by_freq[:3]:
                print(f"\n{item} (appears {data['frequency']} times):")
                print(f"  Job Titles: {'; '.join(data['job_titles'][:5])}")
                print(f"  Job Levels: {'; '.join(data['job_levels'][:5])}")
                print(
                    f"  Experience: {'; '.join(data['years_of_experience'][:5])}")

    except Exception as e:
        print(f"An error occurred: {e}")


def main():
    """
    Main function with options for single file or batch processing.
    """
    print("=== Skills and Knowledge Extractor with Job Associations ===")
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
