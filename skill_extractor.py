import pandas as pd
from transformers import pipeline
import json
from typing import List, Dict
import torch
import shutil
import os
from datetime import datetime

# Initialize both extraction pipelines
print("Loading skill extraction model...")
skill_extractor = pipeline(
    "token-classification",
    model="jjzha/jobbert_skill_extraction",
    tokenizer="jjzha/jobbert_skill_extraction",
    aggregation_strategy="simple"
)
print("Skill extraction model loaded successfully!")

print("Loading knowledge extraction model...")
knowledge_extractor = pipeline(
    "token-classification",
    model="jjzha/jobbert_knowledge_extraction",
    tokenizer="jjzha/jobbert_knowledge_extraction",
    aggregation_strategy="average"
)
print("Knowledge extraction model loaded successfully!")


def copy_original_file(input_file_path: str, output_dir: str = "processed_files") -> str:
    """
    Create a backup copy of the original file before processing

    Args:
        input_file_path: Path to the original file
        output_dir: Directory to store the copy

    Returns:
        Path to the copied file
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Generate timestamped filename for the copy
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.basename(input_file_path)
        name, ext = os.path.splitext(filename)
        copied_filename = f"{name}_original_{timestamp}{ext}"
        copied_file_path = os.path.join(output_dir, copied_filename)

        # Copy the file
        shutil.copy2(input_file_path, copied_file_path)
        print(f"Original file copied to: {copied_file_path}")

        return copied_file_path
    except Exception as e:
        print(f"Error copying original file: {e}")
        return ""


def extract_skills_from_text_chunk(text: str, confidence_threshold: float = 0.9) -> List[Dict]:
    """
    Extract skills from a single text chunk (guaranteed to be within token limits)

    Args:
        text: Input text chunk to extract skills from
        confidence_threshold: Minimum confidence score to include a skill

    Returns:
        List of dictionaries with extracted skills and their confidence scores
    """
    try:
        # Convert to string and clean
        text = str(text).strip()
        if not text:
            return []

        # Run the skill model on the text chunk
        results = skill_extractor(text)

        # Filter by confidence threshold and clean up
        skills = []
        for result in results:
            if result['score'] >= confidence_threshold:
                skills.append({
                    'item': result['word'],
                    'confidence': result['score'],
                    'start': result['start'],
                    'end': result['end'],
                    'type': 'skill'
                })

        return skills
    except Exception as e:
        print(f"Error processing text chunk for skills: {e}")
        return []


def extract_knowledge_from_text_chunk(text: str, confidence_threshold: float = 0.9) -> List[Dict]:
    """
    Extract knowledge from a single text chunk (guaranteed to be within token limits)

    Args:
        text: Input text chunk to extract knowledge from
        confidence_threshold: Minimum confidence score to include a knowledge item

    Returns:
        List of dictionaries with extracted knowledge and their confidence scores
    """
    try:
        # Convert to string and clean
        text = str(text).strip()
        if not text:
            return []

        # Run the knowledge model on the text chunk
        results = knowledge_extractor(text)

        # Filter by confidence threshold and clean up
        knowledge = []
        for result in results:
            if result['score'] >= confidence_threshold:
                knowledge.append({
                    'item': result['word'],
                    'confidence': result['score'],
                    'start': result['start'],
                    'end': result['end'],
                    'type': 'knowledge'
                })

        return knowledge
    except Exception as e:
        print(f"Error processing text chunk for knowledge: {e}")
        return []


def extract_skills_and_knowledge_from_text(text: str, confidence_threshold: float = 0.9, max_words: int = 280) -> Dict[str, List[Dict]]:
    """
    Extract both skills and knowledge from text, splitting into chunks if necessary

    Args:
        text: Input text to extract skills and knowledge from
        confidence_threshold: Minimum confidence score to include an item
        max_words: Maximum words per chunk (roughly 512 tokens)

    Returns:
        Dictionary with 'skills' and 'knowledge' keys containing lists of extracted items
    """
    try:
        # Convert to string and clean
        text = str(text).strip()
        if not text:
            return {'skills': [], 'knowledge': []}

        words = text.split()

        # If text is short enough, process as single chunk
        if len(words) <= max_words:
            skills = extract_skills_from_text_chunk(text, confidence_threshold)
            knowledge = extract_knowledge_from_text_chunk(
                text, confidence_threshold)
            return {'skills': skills, 'knowledge': knowledge}

        # Split long text into overlapping chunks
        overlap_words = 50  # Overlap to catch items that might be split between chunks
        chunks = []

        for i in range(0, len(words), max_words - overlap_words):
            chunk_words = words[i:i + max_words]
            chunk_text = ' '.join(chunk_words)
            chunks.append(chunk_text)

            # Break if we've covered all words
            if i + max_words >= len(words):
                break

        print(
            f"Processing text in {len(chunks)} chunks (original: {len(words)} words)")

        # Extract both skills and knowledge from all chunks
        all_skills = []
        all_knowledge = []
        seen_skills = set()  # To avoid duplicates from overlapping chunks
        seen_knowledge = set()

        for chunk_idx, chunk in enumerate(chunks):
            chunk_skills = extract_skills_from_text_chunk(
                chunk, 0.9)
            chunk_knowledge = extract_knowledge_from_text_chunk(
                chunk, 0.7)

            # Add unique skills (avoid duplicates from overlap)
            for skill in chunk_skills:
                skill_key = skill['item'].lower().strip()
                if skill_key not in seen_skills:
                    seen_skills.add(skill_key)
                    # Adjust start/end positions for the full text context
                    if chunk_idx > 0:
                        words_before = chunk_idx * (max_words - overlap_words)
                        char_offset = len(' '.join(words[:words_before])) + 1
                        skill['start'] += char_offset
                        skill['end'] += char_offset
                    all_skills.append(skill)

            # Add unique knowledge items (avoid duplicates from overlap)
            for knowledge_item in chunk_knowledge:
                knowledge_key = knowledge_item['item'].lower().strip()
                if knowledge_key not in seen_knowledge:
                    seen_knowledge.add(knowledge_key)
                    # Adjust start/end positions for the full text context
                    if chunk_idx > 0:
                        words_before = chunk_idx * (max_words - overlap_words)
                        char_offset = len(' '.join(words[:words_before])) + 1
                        knowledge_item['start'] = int(
                            knowledge_item['start']) + char_offset
                        knowledge_item['end'] = int(
                            knowledge_item['end']) + char_offset
                    all_knowledge.append(knowledge_item)

        print(
            f"Found {len(all_skills)} unique skills and {len(all_knowledge)} unique knowledge items across all chunks")
        return {'skills': all_skills, 'knowledge': all_knowledge}

    except Exception as e:
        print(f"Error processing text: {e}")
        return {'skills': [], 'knowledge': []}


def process_dataset(df: pd.DataFrame, text_column: str, confidence_threshold: float = 0.9) -> pd.DataFrame:
    """
    Process entire dataset to extract both skills and knowledge

    Args:
        df: DataFrame containing the text data
        text_column: Name of the column containing text to analyze
        confidence_threshold: Minimum confidence score to include an item

    Returns:
        DataFrame with added columns for extracted skills and knowledge
    """
    print(f"Processing {len(df)} rows...")

    # Lists to store results
    all_skills = []
    all_knowledge = []
    skill_counts = []
    knowledge_counts = []
    skill_text = []
    knowledge_text = []
    combined_counts = []
    combined_text = []

    # Store detailed extraction data as JSON strings for complete preservation
    skills_detailed = []
    knowledge_detailed = []
    combined_detailed = []

    for idx, row in df.iterrows():
        if idx % 10 == 0:  # Progress indicator
            print(f"Processing row {idx}/{len(df)}")

        text = str(row[text_column])
        results = extract_skills_and_knowledge_from_text(
            text, confidence_threshold)

        skills = results['skills']
        knowledge = results['knowledge']

        # Store results
        all_skills.append(skills)
        all_knowledge.append(knowledge)
        skill_counts.append(len(skills))
        knowledge_counts.append(len(knowledge))
        skill_text.append([skill['item'] for skill in skills])
        knowledge_text.append([knowledge_item['item']
                              for knowledge_item in knowledge])

        # Combined results
        combined_items = skills + knowledge
        combined_counts.append(len(combined_items))
        combined_text.append([item['item'] for item in combined_items])

    # Add new columns to dataframe
    df_result = df.copy()
    df_result['extracted_skills'] = all_skills
    df_result['extracted_knowledge'] = all_knowledge
    df_result['skill_count'] = skill_counts
    df_result['knowledge_count'] = knowledge_counts
    df_result['skills_list'] = skill_text
    df_result['knowledge_list'] = knowledge_text
    df_result['total_items_count'] = combined_counts
    df_result['all_items_list'] = combined_text

    return df_result


def save_results_with_backups(df: pd.DataFrame, input_file_path: str, output_dir: str = "processed_files") -> Dict[str, str]:
    """
    Save processing results with multiple formats and create backups

    Args:
        df: Processed DataFrame with extracted skills and knowledge
        input_file_path: Original input file path
        output_dir: Directory to save all outputs

    Returns:
        Dictionary with paths to all saved files
    """
    try:
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Generate timestamp for unique filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = os.path.splitext(os.path.basename(input_file_path))[0]

        saved_files = {}

        # 1. Copy original file
        if os.path.exists(input_file_path):
            original_copy = os.path.join(
                output_dir, f"{base_name}_original_{timestamp}.csv")
            shutil.copy2(input_file_path, original_copy)
            saved_files['original_copy'] = original_copy
            print(f"✓ Original file backed up: {original_copy}")

        # 2. Save complete results (all columns)
        complete_results = os.path.join(
            output_dir, f"{base_name}_complete_results_{timestamp}.csv")
        df.to_csv(complete_results, index=False, encoding='utf-8')
        saved_files['complete_results'] = complete_results
        print(f"✓ Complete results saved: {complete_results}")

        # 3. Save skills-focused dataset
        skills_columns = ['id', 'title', 'company', 'location', 'description',
                          'skill_count', 'skills_list', 'skills_detailed_json']
        # Check if basic columns exist
        if all(col in df.columns for col in skills_columns[:5]):
            skills_df = df[skills_columns].copy()
            skills_results = os.path.join(
                output_dir, f"{base_name}_skills_focused_{timestamp}.csv")
            skills_df.to_csv(skills_results, index=False, encoding='utf-8')
            saved_files['skills_focused'] = skills_results
            print(f"✓ Skills-focused dataset saved: {skills_results}")

        # 4. Save knowledge-focused dataset
        knowledge_columns = ['id', 'title', 'company', 'location', 'description',
                             'knowledge_count', 'knowledge_list', 'knowledge_detailed_json']
        # Check if basic columns exist
        if all(col in df.columns for col in knowledge_columns[:5]):
            knowledge_df = df[knowledge_columns].copy()
            knowledge_results = os.path.join(
                output_dir, f"{base_name}_knowledge_focused_{timestamp}.csv")
            knowledge_df.to_csv(knowledge_results,
                                index=False, encoding='utf-8')
            saved_files['knowledge_focused'] = knowledge_results
            print(f"✓ Knowledge-focused dataset saved: {knowledge_results}")

        # 5. Save summary statistics
        summary_stats = {
            'processing_timestamp': timestamp,
            'total_records_processed': len(df),
            'records_with_skills': len(df[df['skill_count'] > 0]) if 'skill_count' in df.columns else 0,
            'records_with_knowledge': len(df[df['knowledge_count'] > 0]) if 'knowledge_count' in df.columns else 0,
            'avg_skills_per_record': df['skill_count'].mean() if 'skill_count' in df.columns else 0,
            'avg_knowledge_per_record': df['knowledge_count'].mean() if 'knowledge_count' in df.columns else 0,
            'max_skills_in_record': df['skill_count'].max() if 'skill_count' in df.columns else 0,
            'max_knowledge_in_record': df['knowledge_count'].max() if 'knowledge_count' in df.columns else 0,
        }

        summary_file = os.path.join(
            output_dir, f"{base_name}_processing_summary_{timestamp}.json")
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary_stats, f, indent=2, ensure_ascii=False)
        saved_files['summary'] = summary_file
        print(f"✓ Processing summary saved: {summary_file}")

        # 6. Save top skills and knowledge as separate files
        if 'skills_list' in df.columns:
            top_skills = get_top_skills(df, top_n=50)
            top_skills_file = os.path.join(
                output_dir, f"{base_name}_top_skills_{timestamp}.csv")
            top_skills.to_csv(top_skills_file, index=False)
            saved_files['top_skills'] = top_skills_file
            print(f"✓ Top skills saved: {top_skills_file}")

        if 'knowledge_list' in df.columns:
            top_knowledge = get_top_knowledge(df, top_n=50)
            top_knowledge_file = os.path.join(
                output_dir, f"{base_name}_top_knowledge_{timestamp}.csv")
            top_knowledge.to_csv(top_knowledge_file, index=False)
            saved_files['top_knowledge'] = top_knowledge_file
            print(f"✓ Top knowledge saved: {top_knowledge_file}")

        print(f"\n✓ All files saved to directory: {output_dir}")
        return saved_files

    except Exception as e:
        print(f"Error saving results: {e}")
        return {}


def get_top_skills(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """
    Get the most frequently mentioned skills across all texts

    Args:
        df: DataFrame with extracted skills
        top_n: Number of top skills to return

    Returns:
        DataFrame with skill frequencies
    """
    skill_freq = {}

    for skills_list in df['skills_list']:
        for skill in skills_list:
            skill_clean = skill.strip().lower()
            skill_freq[skill_clean] = skill_freq.get(skill_clean, 0) + 1

    # Convert to DataFrame and sort
    freq_df = pd.DataFrame(list(skill_freq.items()),
                           columns=['skill', 'frequency'])
    freq_df = freq_df.sort_values('frequency', ascending=False).head(top_n)

    return freq_df


def get_top_knowledge(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """
    Get the most frequently mentioned knowledge items across all texts

    Args:
        df: DataFrame with extracted knowledge
        top_n: Number of top knowledge items to return

    Returns:
        DataFrame with knowledge frequencies
    """
    knowledge_freq = {}

    for knowledge_list in df['knowledge_list']:
        for knowledge_item in knowledge_list:
            knowledge_clean = knowledge_item.strip().lower()
            knowledge_freq[knowledge_clean] = knowledge_freq.get(
                knowledge_clean, 0) + 1

    # Convert to DataFrame and sort
    freq_df = pd.DataFrame(list(knowledge_freq.items()),
                           columns=['knowledge', 'frequency'])
    freq_df = freq_df.sort_values('frequency', ascending=False).head(top_n)

    return freq_df


# Example usage:
if __name__ == "__main__":
    # Load your job dataset
    # Replace with your actual file path
    csv_file_path = 'data/jobsdb_th__AI-Engineer.csv'
    output_directory = "processed_job_data"  # Directory for all outputs

    print(f"Loading dataset from {csv_file_path}...")

    try:
        df = pd.read_csv(csv_file_path)
        print(f"Dataset loaded successfully! Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")

        # Check if description column exists
        if 'description' not in df.columns:
            print("Error: 'description' column not found in dataset!")
            print("Available columns:", list(df.columns))
            exit(1)

        # Remove rows with empty descriptions
        initial_count = len(df)
        df = df.dropna(subset=['description'])
        df = df[df['description'].str.strip() != '']
        final_count = len(df)

        if final_count == 0:
            print("Error: No valid descriptions found in dataset!")
            exit(1)

        print(
            f"Removed {initial_count - final_count} rows with empty descriptions")
        print(f"Processing {final_count} job descriptions...")

        # Process the dataset (focusing on description column)
        results_df = process_dataset(
            df, 'description', confidence_threshold=0.7)

        # Save all results with backups and multiple formats
        saved_files = save_results_with_backups(
            results_df, csv_file_path, output_directory)

    except FileNotFoundError:
        print(f"Error: File {csv_file_path} not found!")
        print("Creating sample with your data format for demonstration...")

        # Using your actual sample data format
        sample_data = {
            'id': [86357160, 86313613],
            'title': ['Data Analyst', 'Operations Data Analyst,Manager (Business Controller)'],
            'company': ['Smooth E Co., Ltd.', 'Eve And Boy Co., Ltd.'],
            'location': ['Sathon, Bangkok', 'Bangkok'],
            'description': [
                'Data Visualization Report Python Microsoft Excel Microsoft SQL Oracle- Model MIS-  Phyton ,Excel, Micro SQL, Power BI- DATABASE',
                'As the Operations Data Analyst (Business Controller), you will be the key analytical partner for the Store Operations team, responsible for developing data models, dashboards, and performance indices. Strong proficiency in Excel, SQL, Power BI (or Tableau), and Google Sheets/Data Studio. Bachelor degree in Data Analytics, Statistics, Business Intelligence, or a related field. 3â€"5 years of experience in data analysis, preferably in retail, operations, or performance management.'
            ]
        }
        df = pd.DataFrame(sample_data)
        results_df = process_dataset(
            df, 'description', confidence_threshold=0.6)

        # Save sample results
        saved_files = save_results_with_backups(
            results_df, "sample_data.csv", output_directory)

    # Display results
    print("\n=== EXTRACTION RESULTS ===")
    for idx, row in results_df.iterrows():
        print(
            f"\n--- Job {idx + 1}: {row.get('title', 'N/A')} at {row.get('company', 'N/A')} ---")
        print(f"Description preview: {str(row['description'])[:150]}...")
        print(
            f"Skills found ({len(row['skills_list'])}): {row['skills_list']}")
        print(
            f"Knowledge found ({len(row['knowledge_list'])}): {row['knowledge_list']}")

    # Get top skills and knowledge across all jobs
    print("\n=== TOP SKILLS ACROSS ALL JOBS ===")
    top_skills = get_top_skills(results_df, top_n=15)
    print(top_skills)

    print("\n=== TOP KNOWLEDGE ACROSS ALL JOBS ===")
    top_knowledge = get_top_knowledge(results_df, top_n=15)
    print(top_knowledge)

    # Additional analysis for your job data
    print("\n=== ADDITIONAL INSIGHTS ===")
    print(f"Total jobs processed: {len(results_df)}")
    jobs_with_skills = len(results_df[results_df['skill_count'] > 0])
    jobs_with_knowledge = len(results_df[results_df['knowledge_count'] > 0])
    jobs_with_both = len(results_df[(results_df['skill_count'] > 0) &
                                    (results_df['knowledge_count'] > 0)])

    print(f"Jobs with skills found: {jobs_with_skills}")
    print(f"Jobs with knowledge found: {jobs_with_knowledge}")
    print(f"Jobs with both skills and knowledge: {jobs_with_both}")

    if len(results_df) > 0:
        print(
            f"Average skills per job: {results_df['skill_count'].mean():.1f}")
        print(
            f"Average knowledge items per job: {results_df['knowledge_count'].mean():.1f}")
        print(
            f"Average total items per job: {results_df['total_items_count'].mean():.1f}")
        print(f"Max skills in a single job: {results_df['skill_count'].max()}")
        print(
            f"Max knowledge items in a single job: {results_df['knowledge_count'].max()}")

        # Show distribution of skill and knowledge counts
        print(f"\nSkill count distribution:")
        skill_distribution = results_df['skill_count'].value_counts(
        ).sort_index()
        for count, frequency in skill_distribution.head(8).items():
            print(f"  {count} skills: {frequency} jobs")

        print(f"\nKnowledge count distribution:")
        knowledge_distribution = results_df['knowledge_count'].value_counts(
        ).sort_index()
        for count, frequency in knowledge_distribution.head(8).items():
            print(f"  {count} knowledge items: {frequency} jobs")

    # Print summary of saved files
    print("\n=== FILES CREATED ===")
    for file_type, file_path in saved_files.items():
        print(f"{file_type}: {file_path}")
