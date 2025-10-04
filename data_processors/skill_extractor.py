import pandas as pd
from transformers import pipeline
import json
from typing import List, Dict, Tuple
import torch
import shutil
import os
import glob
from datetime import datetime
from pathlib import Path

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


def get_csv_files_in_folder(folder_path: str) -> List[str]:
    """
    Get all CSV files in the specified folder

    Args:
        folder_path: Path to the folder containing CSV files

    Returns:
        List of CSV file paths
    """
    folder_path = Path(folder_path)
    if not folder_path.exists():
        print(f"Error: Folder {folder_path} does not exist!")
        return []

    # Get all CSV files in the folder
    csv_files = list(folder_path.glob("*.csv"))

    if not csv_files:
        print(f"No CSV files found in {folder_path}")
        return []

    print(f"Found {len(csv_files)} CSV files in {folder_path}:")
    for file in csv_files:
        print(f"  - {file.name}")

    return [str(file) for file in csv_files]


def detect_text_column(df: pd.DataFrame) -> str:
    """
    Automatically detect the main text column to analyze

    Args:
        df: Input DataFrame

    Returns:
        Name of the text column to analyze
    """
    # Common column names for job descriptions
    text_columns = ['description', 'job_description',
                    'desc', 'content', 'text', 'details']

    # Check if any of the common names exist
    for col in text_columns:
        if col in df.columns:
            print(f"Found text column: '{col}'")
            return col

    # If no common names found, look for columns with long text content
    for col in df.columns:
        if df[col].dtype == 'object':  # String columns
            # Check average text length
            avg_length = df[col].astype(str).str.len().mean()
            if avg_length > 100:  # Assume descriptions are longer than 100 chars
                print(
                    f"Auto-detected text column: '{col}' (avg length: {avg_length:.0f})")
                return col

    print("Warning: Could not automatically detect text column. Using first string column.")
    # Return first string column as fallback
    for col in df.columns:
        if df[col].dtype == 'object':
            return col

    raise ValueError("No suitable text column found in the dataset!")


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

        # # 1. Copy original file
        # if os.path.exists(input_file_path):
        #     original_copy = os.path.join(
        #         output_dir, f"{base_name}_original_{timestamp}.csv")
        #     shutil.copy2(input_file_path, original_copy)
        #     saved_files['original_copy'] = original_copy
        #     print(f"✓ Original file backed up: {original_copy}")

        # 2. Save complete results (all columns)
        complete_results = os.path.join(
            output_dir, f"{base_name}_complete_results_{timestamp}.csv")
        df.to_csv(complete_results, index=False, encoding='utf-8')
        saved_files['complete_results'] = complete_results
        print(f"✓ Complete results saved: {complete_results}")

        # # 3. Save skills-focused dataset
        # basic_columns = [col for col in ['id', 'title', 'company',
        #                                  'location', 'description'] if col in df.columns]
        # skills_columns = basic_columns + \
        #     ['skill_count', 'skills_list', 'extracted_skills']
        # available_skills_columns = [
        #     col for col in skills_columns if col in df.columns]

        # if len(available_skills_columns) > 1:
        #     skills_df = df[available_skills_columns].copy()
        #     skills_results = os.path.join(
        #         output_dir, f"{base_name}_skills_focused_{timestamp}.csv")
        #     skills_df.to_csv(skills_results, index=False, encoding='utf-8')
        #     saved_files['skills_focused'] = skills_results
        #     print(f"✓ Skills-focused dataset saved: {skills_results}")

        # # 4. Save knowledge-focused dataset
        # knowledge_columns = basic_columns + \
        #     ['knowledge_count', 'knowledge_list', 'extracted_knowledge']
        # available_knowledge_columns = [
        #     col for col in knowledge_columns if col in df.columns]

        # if len(available_knowledge_columns) > 1:
        #     knowledge_df = df[available_knowledge_columns].copy()
        #     knowledge_results = os.path.join(
        #         output_dir, f"{base_name}_knowledge_focused_{timestamp}.csv")
        #     knowledge_df.to_csv(knowledge_results,
        #                         index=False, encoding='utf-8')
        #     saved_files['knowledge_focused'] = knowledge_results
        #     print(f"✓ Knowledge-focused dataset saved: {knowledge_results}")

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

        # # 6. Save top skills and knowledge as separate files
        # if 'skills_list' in df.columns:
        #     top_skills = get_top_skills(df, top_n=50)
        #     top_skills_file = os.path.join(
        #         output_dir, f"{base_name}_top_skills_{timestamp}.csv")
        #     top_skills.to_csv(top_skills_file, index=False)
        #     saved_files['top_skills'] = top_skills_file
        #     print(f"✓ Top skills saved: {top_skills_file}")

        # if 'knowledge_list' in df.columns:
        #     top_knowledge = get_top_knowledge(df, top_n=50)
        #     top_knowledge_file = os.path.join(
        #         output_dir, f"{base_name}_top_knowledge_{timestamp}.csv")
        #     top_knowledge.to_csv(top_knowledge_file, index=False)
        #     saved_files['top_knowledge'] = top_knowledge_file
        #     print(f"✓ Top knowledge saved: {top_knowledge_file}")

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


def process_single_file(file_path: str, output_directory: str, confidence_threshold: float = 0.7) -> Tuple[bool, Dict]:
    """
    Process a single CSV file

    Args:
        file_path: Path to the CSV file
        output_directory: Directory to save results
        confidence_threshold: Confidence threshold for extraction

    Returns:
        Tuple of (success, results_summary)
    """
    try:
        print(f"\n{'='*60}")
        print(f"Processing file: {os.path.basename(file_path)}")
        print(f"{'='*60}")

        # Load dataset
        df = pd.read_csv(file_path)
        print(f"Dataset loaded successfully! Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")

        # Auto-detect text column
        text_column = detect_text_column(df)

        # Remove rows with empty descriptions
        initial_count = len(df)
        df = df.dropna(subset=[text_column])
        df = df[df[text_column].astype(str).str.strip() != '']
        final_count = len(df)

        if final_count == 0:
            print(f"Error: No valid {text_column} found in {file_path}!")
            return False, {'error': 'No valid text content'}

        print(
            f"Removed {initial_count - final_count} rows with empty {text_column}")
        print(f"Processing {final_count} records...")

        # Process the dataset
        results_df = process_dataset(df, text_column, confidence_threshold)

        # Create file-specific output directory
        file_output_dir = os.path.join(
            output_directory, f"processed_{os.path.splitext(os.path.basename(file_path))[0]}")

        # Save results
        saved_files = save_results_with_backups(
            results_df, file_path, file_output_dir)

        # Generate summary
        summary = {
            'file_name': os.path.basename(file_path),
            'total_records': len(results_df),
            'records_with_skills': len(results_df[results_df['skill_count'] > 0]),
            'records_with_knowledge': len(results_df[results_df['knowledge_count'] > 0]),
            'avg_skills_per_record': results_df['skill_count'].mean(),
            'avg_knowledge_per_record': results_df['knowledge_count'].mean(),
            'max_skills': results_df['skill_count'].max(),
            'max_knowledge': results_df['knowledge_count'].max(),
            'saved_files': saved_files,
            'output_directory': file_output_dir
        }

        print(f"\n✓ Successfully processed {os.path.basename(file_path)}")
        print(f"  - Records processed: {summary['total_records']}")
        print(f"  - Records with skills: {summary['records_with_skills']}")
        print(
            f"  - Records with knowledge: {summary['records_with_knowledge']}")
        print(
            f"  - Average skills per record: {summary['avg_skills_per_record']:.1f}")
        print(
            f"  - Average knowledge per record: {summary['avg_knowledge_per_record']:.1f}")

        return True, summary

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False, {'error': str(e)}


def process_all_files_in_folder(folder_path: str, output_directory: str = "batch_processed_files", confidence_threshold: float = 0.7) -> Dict:
    """
    Process all CSV files in a given folder

    Args:
        folder_path: Path to folder containing CSV files
        output_directory: Directory to save all results
        confidence_threshold: Confidence threshold for extraction

    Returns:
        Dictionary with processing results for all files
    """
    print(f"\n{'='*80}")
    print(f"BATCH PROCESSING: Processing all CSV files in {folder_path}")
    print(f"{'='*80}")

    # Get all CSV files
    csv_files = get_csv_files_in_folder(folder_path)

    if not csv_files:
        return {'error': 'No CSV files found'}

    # Create main output directory
    os.makedirs(output_directory, exist_ok=True)

    # Track processing results
    batch_results = {
        'start_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'total_files': len(csv_files),
        'successful_files': 0,
        'failed_files': 0,
        'file_results': {},
        'batch_summary': {},
        'output_directory': output_directory
    }

    # Process each file
    for i, file_path in enumerate(csv_files, 1):
        print(f"\n[{i}/{len(csv_files)}] Processing: {os.path.basename(file_path)}")

        success, result = process_single_file(
            file_path, output_directory, confidence_threshold)

        batch_results['file_results'][os.path.basename(file_path)] = result

        if success:
            batch_results['successful_files'] += 1
        else:
            batch_results['failed_files'] += 1

    # Generate batch summary
    batch_results['end_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Aggregate statistics across all successful files
    successful_results = [
        r for r in batch_results['file_results'].values() if 'error' not in r]

    if successful_results:
        batch_results['batch_summary'] = {
            'total_records_across_all_files': sum(r['total_records'] for r in successful_results),
            'total_records_with_skills': sum(r['records_with_skills'] for r in successful_results),
            'total_records_with_knowledge': sum(r['records_with_knowledge'] for r in successful_results),
            'average_skills_per_record_overall': sum(r['avg_skills_per_record'] * r['total_records'] for r in successful_results) / sum(r['total_records'] for r in successful_results),
            'average_knowledge_per_record_overall': sum(r['avg_knowledge_per_record'] * r['total_records'] for r in successful_results) / sum(r['total_records'] for r in successful_results),
            'highest_skills_in_single_record': max(r['max_skills'] for r in successful_results),
            'highest_knowledge_in_single_record': max(r['max_knowledge'] for r in successful_results)
        }

    # Save batch processing summary
    summary_file = os.path.join(
        output_directory, f"batch_processing_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(batch_results, f, indent=2, ensure_ascii=False, default=str)

    print(f"\n{'='*80}")
    print(f"BATCH PROCESSING COMPLETE")
    print(f"{'='*80}")
    print(f"Total files: {batch_results['total_files']}")
    print(f"Successful: {batch_results['successful_files']}")
    print(f"Failed: {batch_results['failed_files']}")

    if successful_results:
        print(f"\nOverall Statistics:")
        print(
            f"Total records processed: {batch_results['batch_summary']['total_records_across_all_files']}")
        print(
            f"Records with skills: {batch_results['batch_summary']['total_records_with_skills']}")
        print(
            f"Records with knowledge: {batch_results['batch_summary']['total_records_with_knowledge']}")
        print(
            f"Average skills per record: {batch_results['batch_summary']['average_skills_per_record_overall']:.1f}")
        print(
            f"Average knowledge per record: {batch_results['batch_summary']['average_knowledge_per_record_overall']:.1f}")

    print(f"\nBatch summary saved to: {summary_file}")
    print(f"All results saved in: {output_directory}")

    return batch_results


# Example usage and main execution
if __name__ == "__main__":
    # Configuration
    # Change this to your folder containing CSV files
    INPUT_FOLDER = "raw_job_post_scrapings"
    OUTPUT_FOLDER = "processed_job_data"
    CONFIDENCE_THRESHOLD = 0.7

    print("=== BATCH SKILL & KNOWLEDGE EXTRACTOR ===")
    print(f"Input folder: {INPUT_FOLDER}")
    print(f"Output folder: {OUTPUT_FOLDER}")
    print(f"Confidence threshold: {CONFIDENCE_THRESHOLD}")

    # Check if input folder exists
    if not os.path.exists(INPUT_FOLDER):
        print(f"\nError: Input folder '{INPUT_FOLDER}' does not exist!")
        print("Creating sample folder with demo data...")

        # Create sample data for demonstration
        os.makedirs(INPUT_FOLDER, exist_ok=True)

        # Sample data 1
        sample_data_1 = {
            'id': [1, 2, 3],
            'title': ['Data Scientist', 'Machine Learning Engineer', 'AI Researcher'],
            'company': ['Tech Corp', 'AI Startup', 'Research Lab'],
            'description': [
                'Python programming, machine learning, pandas, scikit-learn, deep learning, TensorFlow',
                'Python, PyTorch, computer vision, natural language processing, AWS, Docker',
                'Research experience, Python, R, statistical analysis, machine learning algorithms'
            ]
        }

        # Sample data 2
        sample_data_2 = {
            'id': [4, 5, 6],
            'title': ['Software Engineer', 'DevOps Engineer', 'Data Analyst'],
            'company': ['StartupXYZ', 'CloudCorp', 'Analytics Inc'],
            'description': [
                'Java, Spring framework, microservices, REST APIs, database design, MySQL',
                'Kubernetes, Docker, CI/CD, Jenkins, AWS, monitoring, Terraform',
                'SQL, Excel, Power BI, data visualization, statistical analysis, reporting'
            ]
        }

        # Save sample files
        pd.DataFrame(sample_data_1).to_csv(os.path.join(
            INPUT_FOLDER, "ai_jobs.csv"), index=False)
        pd.DataFrame(sample_data_2).to_csv(os.path.join(
            INPUT_FOLDER, "tech_jobs.csv"), index=False)

        print(f"Created sample files in {INPUT_FOLDER}/")

    # Process all files in the folder
    results = process_all_files_in_folder(
        folder_path=INPUT_FOLDER,
        output_directory=OUTPUT_FOLDER,
        confidence_threshold=CONFIDENCE_THRESHOLD
    )

    print("\n" + "="*80)
    print("PROCESSING COMPLETED!")
    print("="*80)
