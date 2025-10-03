import ollama
import pandas as pd
import numpy as np
import re
import ast
from collections import defaultdict, Counter
import json


class SkillsFilter:
    def __init__(self, model_name="llama3.1:8b"):
        self.model_name = model_name
        self.programming_skills = set()
        self.data_analytics_skills = set()

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
                    sample = str(df[col].iloc[0])
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

    def extract_all_skills(self, df, item_column='item'):
        """Extract all unique skills/items from the dataset"""
        all_skills = set()

        for idx, row in df.iterrows():
            item = row[item_column]
            if pd.isna(item):
                continue

            # Clean the item text
            item_clean = str(item).strip()

            # Remove common prefixes
            item_clean = re.sub(r'^[&\-\s]+', '', item_clean)
            item_clean = item_clean.strip()

            if item_clean and len(item_clean) > 2:
                all_skills.add(item_clean)

        return list(all_skills)

    def classify_skills_batch(self, skills_list, batch_size=20):
        """Classify skills in batches using LLM"""
        programming_skills = []
        data_analytics_skills = []
        unrelated_skills = []

        # Process in batches to avoid token limits
        for i in range(0, len(skills_list), batch_size):
            batch = skills_list[i:i + batch_size]

            prompt = f"""
            Classify each of the following skills/items as one of these categories:
            - PROGRAMMING: Hard/soft skills related to programming, software development, coding languages, frameworks, development tools, version control, etc.
            - DATA_ANALYTICS: Hard/soft skills related to data analysis, data science, statistics, machine learning, data visualization, databases, business intelligence, etc.
            - UNRELATED: Skills not related to programming or data analytics (marketing, sales, general business, etc.)
            
            Skills to classify:
            {json.dumps(batch, indent=2)}
            
            Respond in this exact JSON format:
            {{
                "programming": ["skill1", "skill2"],
                "data_analytics": ["skill3", "skill4"],
                "unrelated": ["skill5", "skill6"]
            }}
            
            Be strict - only include items that are clearly technical programming or data analytics skills.
            """

            print(
                f"Processing batch {i//batch_size + 1}/{(len(skills_list) + batch_size - 1)//batch_size}")

            response = self.query_llm(prompt)
            if response:
                try:
                    # Extract JSON from response
                    json_match = re.search(r'\{.*\}', response, re.DOTALL)
                    if json_match:
                        result = json.loads(json_match.group())
                        programming_skills.extend(
                            result.get('programming', []))
                        data_analytics_skills.extend(
                            result.get('data_analytics', []))
                        unrelated_skills.extend(result.get('unrelated', []))
                    else:
                        print(
                            f"Could not parse JSON from response: {response}")
                        # Default to unrelated if parsing fails
                        unrelated_skills.extend(batch)
                except json.JSONDecodeError as e:
                    print(f"JSON parsing error: {e}")
                    print(f"Response was: {response}")
                    # Default to unrelated if parsing fails
                    unrelated_skills.extend(batch)
            else:
                # Default to unrelated if LLM fails
                unrelated_skills.extend(batch)

        return {
            'programming': programming_skills,
            'data_analytics': data_analytics_skills,
            'unrelated': unrelated_skills
        }

    def validate_classification(self, classified_skills):
        """Double-check classification with LLM for quality assurance"""
        print("\nValidating skill classifications...")

        # Sample some skills for validation
        sample_prog = classified_skills['programming'][:10]
        sample_data = classified_skills['data_analytics'][:10]

        if sample_prog:
            prompt = f"""
            Review these skills classified as PROGRAMMING skills. Are they correct?
            Skills: {sample_prog}
            
            Respond with:
            - CORRECT: if all are valid programming skills
            - INCORRECT: if some are misclassified, then list which ones
            """

            validation = self.query_llm(prompt)
            print("Programming skills validation:", validation)

        if sample_data:
            prompt = f"""
            Review these skills classified as DATA ANALYTICS skills. Are they correct?
            Skills: {sample_data}
            
            Respond with:
            - CORRECT: if all are valid data analytics skills  
            - INCORRECT: if some are misclassified, then list which ones
            """

            validation = self.query_llm(prompt)
            print("Data analytics skills validation:", validation)

    def filter_dataframe(self, df, valid_skills_set, item_column='item'):
        """Filter dataframe to keep only rows with valid skills"""
        def is_valid_skill(item):
            if pd.isna(item):
                return False

            item_clean = str(item).strip()
            item_clean = re.sub(r'^[&\-\s]+', '', item_clean).strip()

            return item_clean in valid_skills_set

        filtered_df = df[df[item_column].apply(is_valid_skill)].copy()
        return filtered_df

    def generate_summary_report(self, original_df, filtered_df, classified_skills):
        """Generate a summary report of the filtering process"""
        report = f"""
        SKILLS FILTERING SUMMARY REPORT
        ================================
        
        Original dataset: {len(original_df)} rows
        Filtered dataset: {len(filtered_df)} rows
        Removed: {len(original_df) - len(filtered_df)} rows ({((len(original_df) - len(filtered_df)) / len(original_df) * 100):.1f}%)
        
        IDENTIFIED SKILLS:
        Programming Skills: {len(classified_skills['programming'])}
        Data Analytics Skills: {len(classified_skills['data_analytics'])}
        Unrelated Skills: {len(classified_skills['unrelated'])}
        
        TOP PROGRAMMING SKILLS:
        {classified_skills['programming'][:10]}
        
        TOP DATA ANALYTICS SKILLS:
        {classified_skills['data_analytics'][:10]}
        
        SAMPLE REMOVED (UNRELATED) SKILLS:
        {classified_skills['unrelated'][:10]}
        """

        return report

    def process_dataset(self, input_csv, output_csv, item_column='item'):
        """Main function to process the entire dataset"""
        print("Starting skills filtering process...")

        # Load data
        df = self.load_and_clean_data(input_csv)
        if df is None:
            return None

        print(f"Processing column: {item_column}")

        # Extract all skills
        print("Extracting all skills from dataset...")
        all_skills = self.extract_all_skills(df, item_column)
        print(f"Found {len(all_skills)} unique skills/items")

        # Classify skills using LLM
        print("Classifying skills using LLM...")
        classified_skills = self.classify_skills_batch(all_skills)

        # Validate classification
        self.validate_classification(classified_skills)

        # Create set of valid skills (programming + data analytics)
        valid_skills = set(
            classified_skills['programming'] + classified_skills['data_analytics'])
        print(f"Total valid skills identified: {len(valid_skills)}")

        # Filter the dataframe
        print("Filtering dataset...")
        filtered_df = self.filter_dataframe(df, valid_skills, item_column)

        # Save filtered dataset
        filtered_df.to_csv(output_csv, index=False)
        print(f"Filtered dataset saved to: {output_csv}")

        # Generate and save summary report
        report = self.generate_summary_report(
            df, filtered_df, classified_skills)

        report_file = output_csv.replace('.csv', '_report.txt')
        with open(report_file, 'w') as f:
            f.write(report)

        print(f"Summary report saved to: {report_file}")
        print("\n" + report)

        # Save classified skills as JSON for reference
        skills_file = output_csv.replace('.csv', '_classified_skills.json')
        with open(skills_file, 'w') as f:
            json.dump(classified_skills, f, indent=2)

        print(f"Classified skills saved to: {skills_file}")

        return filtered_df, classified_skills


# Example usage and main execution
if __name__ == "__main__":
    # Initialize the skills filter
    filter_tool = SkillsFilter()

    # Configuration
    # Your input CSV file
    input_file = "my_data_reduced.csv"
    # Output file name
    output_file = "skills_summary/cleaned/cleaned_2/my_filtered.csv"

    # Process the dataset
    print("="*60)
    print("PROGRAMMING & DATA ANALYTICS SKILLS FILTER")
    print("="*60)

    try:
        filtered_data, skills_classification = filter_tool.process_dataset(
            input_csv=input_file,
            output_csv=output_file,
            item_column='item'  # Change this to your actual column name
        )

        if filtered_data is not None:
            print("\n✅ Processing completed successfully!")
            print(f"📁 Filtered data: {output_file}")
            print(f"📊 Report: {output_file.replace('.csv', '_report.txt')}")
            print(
                f"📋 Skills list: {output_file.replace('.csv', '_classified_skills.json')}")

    except Exception as e:
        print(f"❌ Error during processing: {e}")

    # Optional: Interactive mode for testing specific skills
    print("\n" + "="*60)
    print("INTERACTIVE TESTING (Optional)")
    print("="*60)
    print("You can test individual skills by uncommenting the code below:")

    """
    # Uncomment to test individual skills
    test_skills = [
        "Python programming",
        "Data visualization", 
        "Marketing strategy",
        "Machine learning",
        "Customer service"
    ]
    
    test_result = filter_tool.classify_skills_batch(test_skills, batch_size=5)
    print("Test classification:")
    for category, skills in test_result.items():
        print(f"{category}: {skills}")
    """
