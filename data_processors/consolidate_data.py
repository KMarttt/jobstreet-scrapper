import csv
from collections import defaultdict
from datetime import datetime


def consolidate_duplicates(input_csv, output_csv):
    consolidated_map = {}

    try:
        with open(input_csv, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            values = list(reader)

        if len(values) <= 1:
            print("Input file is empty or contains only headers")
            with open(output_csv, 'w', encoding='utf-8', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["Type", "Item", "Frequency", "Job Title"])
            return

        start_row = 0  # Change to 1 if your source has headers

        for i in range(start_row, len(values)):
            row = values[i]

            if len(row) < 4:
                row.extend([''] * (4 - len(row)))

            type_val = row[0]
            item = row[1]

            try:
                frequency = float(row[2]) if row[2] else 0
            except ValueError:
                frequency = 0

            job_title = row[3] if len(row) > 3 else ''

            key = item

            if key in consolidated_map:
                # Add to existing entry
                consolidated_map[key]['totalFrequency'] += frequency
                if job_title and job_title not in consolidated_map[key]['jobTitles']:
                    consolidated_map[key]['jobTitles'].add(job_title)
            else:
                # Create new entry
                consolidated_map[key] = {
                    'type': type_val,
                    'item': item,
                    'totalFrequency': frequency,
                    'jobTitles': {job_title} if job_title else set()
                }

        # Convert map to list for output
        results = [["Type", "Item", "Frequency", "Job Title"]]  # Header row

        for key, entry in consolidated_map.items():
            results.append([
                entry['type'],
                entry['item'],
                entry['totalFrequency'],
                # Join job titles with comma
                ", ".join(sorted(entry['jobTitles']))
            ])

        # Write consolidated data to output CSV
        with open(output_csv, 'w', encoding='utf-8', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(results)

        print(f"Consolidation complete! Output saved to: {output_csv}")
        print(
            f"Processed {len(values) - start_row} rows into {len(results) - 1} consolidated entries")

    except FileNotFoundError:
        print(f"Error: Input file '{input_csv}' not found")
    except Exception as e:
        print(f"Error processing file: {str(e)}")


# Example usage
if __name__ == "__main__":
    input_file = "Summary.csv"  # Replace with your input file path
    output_file = "consolidated_data.csv"  # Replace with desired output file path

    consolidate_duplicates(input_file, output_file)
