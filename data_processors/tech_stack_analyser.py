import ollama
import pandas as pd
import numpy as np
import re
import ast
from collections import defaultdict, Counter


class TechStackAnalyzer:
    def __init__(self, model_name="llama3.1:8b"):
        self.model_name = model_name

    def query_llm(self, prompt, temperature=0.1):
        """Query the local LLM via Ollama"""
        try:
            response = ollama.chat(
                model=self.model_name,
                messages=[{'role': 'user', 'content': prompt}],
                options={'temperature': temperature}
            )
            return response['message']['content']
        except Exception as e:
            print(f"Error querying LLM: {e}")
            return None

    def load_and_clean_data(self, csv_path):
        """Load CSV and perform basic cleaning"""
        try:
            df = pd.read_csv(csv_path)
            print(f"Loaded {len(df)} rows and {len(df.columns)} columns")

            # Clean list-like string columns
            for col in df.columns:
                if df[col].dtype == 'object':
                    sample = str(df[col].iloc[0]) if len(df) > 0 else ""
                    if sample.startswith('[') and sample.endswith(']'):
                        df[col] = df[col].apply(self._parse_list_string)

            return df
        except Exception as e:
            print(f"Error loading CSV: {e}")
            return None

    def _parse_list_string(self, list_str):
        """Parse string representations of lists"""
        try:
            # Remove extra parentheses and numbers in parentheses
            cleaned = re.sub(r'\s*\(\d+\)', '', str(list_str))
            return ast.literal_eval(cleaned)
        except:
            return [str(list_str)]

    def extract_items_with_metadata(self, df):
        """Extract all items with their frequency and job titles"""
        items_data = []

        for idx, row in df.iterrows():
            item = row.get('item', '')
            frequency = row.get('frequency', 0)
            job_title = row.get('job_title', [])

            if pd.isna(item) or not item:
                continue

            # Clean the item text
            item_clean = str(item).strip()
            item_clean = re.sub(r'^[&\-\s]+', '', item_clean).strip()

            if item_clean and len(item_clean) > 1:
                # Extract job titles from list
                job_titles_list = []
                if isinstance(job_title, list):
                    job_titles_list = job_title
                elif isinstance(job_title, str):
                    try:
                        job_titles_list = ast.literal_eval(job_title)
                    except:
                        job_titles_list = [job_title]

                items_data.append({
                    'item': item_clean,
                    'frequency': int(frequency) if not pd.isna(frequency) else 0,
                    'job_title': job_titles_list
                })

        return items_data

    def classify_tech_items_batch(self, items_list, batch_size=15):
        """Classify items into tools, languages, or platforms using LLM"""
        tools = []
        languages = []
        platforms = []
        other = []

        # Process in batches
        for i in range(0, len(items_list), batch_size):
            batch = items_list[i:i + batch_size]
            batch_items = [item['item'] for item in batch]

            # Create a numbered list format instead of JSON
            items_text = "\n".join(
                [f"{idx+1}. {item}" for idx, item in enumerate(batch_items)])

            prompt = f"""
Classify each of the following items into ONE category: TOOL, LANGUAGE, PLATFORM, or OTHER.

DEFINITIONS:
- TOOL: Development tools, software tools, testing tools, design tools, productivity tools
  Examples: Git, Docker, Jenkins, Jira, Postman, Tableau, Visual Studio Code
  
- LANGUAGE: Programming languages, markup languages, query languages
  Examples: Python, Java, JavaScript, SQL, HTML, CSS, R, C++, Ruby
  
- PLATFORM: Platforms, frameworks, cloud platforms, operating systems, databases
  Examples: AWS, Azure, React, Django, Linux, MySQL, MongoDB, TensorFlow, Spring
  
- OTHER: Items that don't clearly fit above or are not tech-related
  Examples: Business strategy, Communication skills, Marketing

ITEMS TO CLASSIFY:
{items_text}

INSTRUCTIONS:
For each item, respond with ONLY the item number followed by the category.
Format each line exactly like this:
1. TOOL
2. LANGUAGE
3. PLATFORM
4. OTHER

Be strict - only classify clearly technology-related items.
"""

            print(
                f"Classifying batch {i//batch_size + 1}/{(len(items_list) + batch_size - 1)//batch_size}...", end='\r')

            response = self.query_llm(prompt)
            if response:
                try:
                    # Parse the response line by line
                    lines = response.strip().split('\n')
                    classifications = {}

                    for line in lines:
                        line = line.strip()
                        # Match patterns like "1. TOOL" or "1: LANGUAGE" or "1 - PLATFORM"
                        match = re.match(r'(\d+)[\.\:\-\s]+(\w+)', line)
                        if match:
                            item_num = int(match.group(1)) - \
                                1  # Convert to 0-indexed
                            category = match.group(2).upper()
                            if item_num < len(batch):
                                classifications[item_num] = category

                    # Assign items to categories
                    for idx, item in enumerate(batch):
                        category = classifications.get(idx, 'OTHER')

                        if category == 'TOOL':
                            tools.append(item)
                        elif category == 'LANGUAGE':
                            languages.append(item)
                        elif category == 'PLATFORM':
                            platforms.append(item)
                        else:
                            other.append(item)

                except Exception as e:
                    print(f"\nError parsing response: {e}")
                    other.extend(batch)
            else:
                other.extend(batch)

        print("\nClassification complete!")
        return {
            'tool': tools,
            'language': languages,
            'platform': platforms,
            'other': other
        }

    def aggregate_duplicates(self, items_list):
        """Aggregate items with same name, summing frequencies and combining job titles"""
        aggregated = defaultdict(lambda: {'frequency': 0, 'job_titles': []})

        for item in items_list:
            name = item['item']
            aggregated[name]['frequency'] += item['frequency']
            if isinstance(item['job_title'], list):
                aggregated[name]['job_titles'].extend(item['job_title'])
            else:
                aggregated[name]['job_titles'].append(str(item['job_title']))

        # Convert back to list format
        result = []
        for name, data in aggregated.items():
            result.append({
                'item': name,
                'frequency': data['frequency'],
                'job_title': list(set(data['job_titles']))  # Remove duplicates
            })

        return result

    def get_top_n(self, items_list, n=50):
        """Get top N items sorted by frequency"""
        # Aggregate duplicates first
        aggregated = self.aggregate_duplicates(items_list)

        # Sort by frequency
        sorted_items = sorted(
            aggregated, key=lambda x: x['frequency'], reverse=True)

        # Return top N
        return sorted_items[:n]

    def format_job_titles(self, job_titles_list):
        """Format job titles list into a readable string"""
        if not job_titles_list:
            return "Not Specified"

        # Clean and deduplicate
        cleaned = [str(title).strip()
                   for title in job_titles_list if str(title).strip()]
        # Limit to top 5 to keep readable
        unique_titles = list(set(cleaned))[:5]

        if len(unique_titles) == 0:
            return "Not Specified"
        elif len(unique_titles) == 1:
            return unique_titles[0]
        else:
            return "; ".join(unique_titles[:3]) + (f" (+{len(unique_titles)-3} more)" if len(unique_titles) > 3 else "")

    def create_output_dataframe(self, classified_items):
        """Create final output dataframe with all categories combined"""
        all_rows = []

        categories = ['tool', 'language', 'platform']

        for category in categories:
            items = classified_items.get(category, [])
            top_items = self.get_top_n(items, n=50)

            for rank, item_data in enumerate(top_items, start=1):
                all_rows.append({
                    'rank': rank,
                    'type': category,
                    'item': item_data['item'],
                    'frequency': item_data['frequency'],
                    'job_title': self.format_job_titles(item_data['job_title'])
                })

        return pd.DataFrame(all_rows)

    def generate_summary_statistics(self, classified_items):
        """Generate summary statistics for the analysis"""
        summary = {
            'total_tools': len(classified_items.get('tool', [])),
            'total_languages': len(classified_items.get('language', [])),
            'total_platforms': len(classified_items.get('platform', [])),
            'total_other': len(classified_items.get('other', []))
        }

        # Calculate total frequencies
        for category in ['tool', 'language', 'platform']:
            items = classified_items.get(category, [])
            aggregated = self.aggregate_duplicates(items)
            total_freq = sum(item['frequency'] for item in aggregated)
            summary[f'{category}_total_frequency'] = total_freq

        return summary

    def save_full_classification(self, classified_items, output_file):
        """Save full classification to a text file instead of JSON"""
        with open(output_file, 'w', encoding='utf-8') as f:
            for category in ['tool', 'language', 'platform', 'other']:
                items = classified_items.get(category, [])
                f.write(f"\n{'='*70}\n")
                f.write(f"{category.upper()}S ({len(items)} items)\n")
                f.write(f"{'='*70}\n\n")

                # Aggregate and sort
                aggregated = self.aggregate_duplicates(items)
                sorted_items = sorted(
                    aggregated, key=lambda x: x['frequency'], reverse=True)

                # Limit to top 100 per category
                for item in sorted_items[:100]:
                    job_titles = self.format_job_titles(item['job_title'])
                    f.write(f"{item['item']}\n")
                    f.write(f"  Frequency: {item['frequency']}\n")
                    f.write(f"  Job Titles: {job_titles}\n\n")

    def analyze_dataset(self, input_csv, output_csv):
        """Main analysis function"""
        print("="*70)
        print("TOP 50 TECH STACK ANALYZER")
        print("="*70)

        # Load data
        print("\n[1/5] Loading dataset...")
        df = self.load_and_clean_data(input_csv)
        if df is None:
            return None

        # Extract items with metadata
        print("[2/5] Extracting items with metadata...")
        items_data = self.extract_items_with_metadata(df)
        print(f"   Found {len(items_data)} items to analyze")

        # Classify items
        print("[3/5] Classifying items using LLM...")
        classified_items = self.classify_tech_items_batch(items_data)

        print(f"\n   Classification results:")
        print(f"   - Tools: {len(classified_items['tool'])}")
        print(f"   - Languages: {len(classified_items['language'])}")
        print(f"   - Platforms: {len(classified_items['platform'])}")
        print(f"   - Other/Non-tech: {len(classified_items['other'])}")

        # Create output dataframe
        print("\n[4/5] Creating ranked output...")
        output_df = self.create_output_dataframe(classified_items)

        # Save to CSV
        print("[5/5] Saving results...")
        output_df.to_csv(output_csv, index=False)
        print(f"   ✅ Results saved to: {output_csv}")

        # Generate and save summary statistics
        summary = self.generate_summary_statistics(classified_items)
        summary_file = output_csv.replace('.csv', '_summary.txt')

        summary_text = f"""
TECH STACK ANALYSIS SUMMARY
{'='*70}

CLASSIFICATION BREAKDOWN:
- Total Tools Identified: {summary['total_tools']}
- Total Languages Identified: {summary['total_languages']}
- Total Platforms Identified: {summary['total_platforms']}
- Non-tech Items: {summary['total_other']}

FREQUENCY TOTALS:
- Tools Total Frequency: {summary.get('tool_total_frequency', 0):,}
- Languages Total Frequency: {summary.get('language_total_frequency', 0):,}
- Platforms Total Frequency: {summary.get('platform_total_frequency', 0):,}

OUTPUT:
- Top 50 Tools (ranked by frequency)
- Top 50 Languages (ranked by frequency)
- Top 50 Platforms (ranked by frequency)

SAMPLE TOP 5 FROM EACH CATEGORY:
"""

        # Add top 5 samples
        for category in ['tool', 'language', 'platform']:
            category_data = output_df[output_df['type'] == category].head(5)
            summary_text += f"\nTop 5 {category.upper()}S:\n"
            for _, row in category_data.iterrows():
                summary_text += f"  {row['rank']}. {row['item']} (freq: {row['frequency']})\n"

        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(summary_text)

        print(f"   ✅ Summary saved to: {summary_file}")

        # Save full classification details as text file
        detail_file = output_csv.replace('.csv', '_full_classification.txt')
        self.save_full_classification(classified_items, detail_file)

        print(f"   ✅ Full classification saved to: {detail_file}")

        print("\n" + "="*70)
        print("ANALYSIS COMPLETE!")
        print("="*70)
        print(summary_text)

        return output_df, classified_items


# Example usage
if __name__ == "__main__":
    analyzer = TechStackAnalyzer()

    # Configuration
    # Your input CSV file
    input_file = "skills_summary/cleaned/cleaned_2/sg_filtered.csv"
    output_file = "top50_tech_stack_analysis_sg.csv"  # Output file

    print("\n🚀 Starting Tech Stack Analysis...")
    print(f"📁 Input: {input_file}")
    print(f"📊 Output: {output_file}\n")

    try:
        # Run the analysis
        results_df, classification = analyzer.analyze_dataset(
            input_csv=input_file,
            output_csv=output_file
        )

        if results_df is not None:
            print("\n✅ SUCCESS! Files generated:")
            print(
                f"   1. {output_file} - Main results (top 50 in each category)")
            print(
                f"   2. {output_file.replace('.csv', '_summary.txt')} - Summary statistics")
            print(
                f"   3. {output_file.replace('.csv', '_full_classification.txt')} - Complete classification data")

            print("\n📈 Quick Preview:")
            print(results_df.head(15).to_string(index=False))

    except Exception as e:
        print(f"\n❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()
