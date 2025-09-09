import pandas as pd
from transformers import pipeline
import json
from typing import List, Dict

# Initialize the skill extraction pipeline
print("Loading skill extraction model...")
skill_extractor = pipeline(
    "token-classification",
    model="jjzha/jobbert_skill_extraction",
    tokenizer="jjzha/jobbert_skill_extraction",
    aggregation_strategy="simple"
)
print("Model loaded successfully!")


def extract_skills_from_text_chunk(text: str, confidence_threshold: float = 0.7) -> List[Dict]:
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

        # Run the model on the text chunk
        results = skill_extractor(text)

        # Filter by confidence threshold and clean up
        skills = []
        for result in results:
            if result['score'] >= confidence_threshold:
                skills.append({
                    'skill': result['word'],
                    'confidence': result['score'],
                    'start': result['start'],
                    'end': result['end']
                })

        return skills
    except Exception as e:
        print(f"Error processing text chunk: {e}")
        return []


def extract_skills_from_text(text: str, confidence_threshold: float = 0.5, max_words: int = 280) -> List[Dict]:
    """
    Extract skills from text, splitting into chunks if necessary

    Args:
        text: Input text to extract skills from
        confidence_threshold: Minimum confidence score to include a skill
        max_words: Maximum words per chunk (roughly 512 tokens)

    Returns:
        List of dictionaries with extracted skills and their confidence scores
    """
    try:
        # Convert to string and clean
        text = str(text).strip()
        if not text:
            return []

        words = text.split()

        # If text is short enough, process as single chunk
        if len(words) <= max_words:
            return extract_skills_from_text_chunk(text, confidence_threshold)

        # Split long text into overlapping chunks
        overlap_words = 50  # Overlap to catch skills that might be split between chunks
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

        # Extract skills from all chunks
        all_skills = []
        seen_skills = set()  # To avoid duplicates from overlapping chunks

        for chunk_idx, chunk in enumerate(chunks):
            chunk_skills = extract_skills_from_text_chunk(
                chunk, confidence_threshold)

            # Add unique skills (avoid duplicates from overlap)
            for skill in chunk_skills:
                skill_key = skill['skill'].lower().strip()
                if skill_key not in seen_skills:
                    seen_skills.add(skill_key)
                    # Adjust start/end positions for the full text context
                    if chunk_idx > 0:
                        words_before = chunk_idx * (max_words - overlap_words)
                        char_offset = len(' '.join(words[:words_before])) + 1
                        skill['start'] += char_offset
                        skill['end'] += char_offset
                    all_skills.append(skill)

        print(f"Found {len(all_skills)} unique skills across all chunks")
        return all_skills

    except Exception as e:
        print(f"Error processing text: {e}")
        return []


def process_dataset(df: pd.DataFrame, text_column: str, confidence_threshold: float = 0.5) -> pd.DataFrame:
    """
    Process entire dataset to extract skills

    Args:
        df: DataFrame containing the text data
        text_column: Name of the column containing text to analyze
        confidence_threshold: Minimum confidence score to include a skill

    Returns:
        DataFrame with added columns for extracted skills
    """
    print(f"Processing {len(df)} rows...")

    # Lists to store results
    all_skills = []
    skill_counts = []
    skill_text = []

    for idx, row in df.iterrows():
        if idx % 10 == 0:  # Progress indicator
            print(f"Processing row {idx}/{len(df)}")

        text = str(row[text_column])
        skills = extract_skills_from_text(text, confidence_threshold)

        # Store results
        all_skills.append(skills)
        skill_counts.append(len(skills))
        skill_text.append([skill['skill'] for skill in skills])

    # Add new columns to dataframe
    df_result = df.copy()
    df_result['extracted_skills'] = all_skills
    df_result['skill_count'] = skill_counts
    df_result['skills_list'] = skill_text

    return df_result


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


# Example usage:
if __name__ == "__main__":
    # Example: Load your dataset (replace with your actual data loading)
    df = pd.read_csv('data/jobsdb_th__Data-Analyst_final.csv')

    # For demonstration with sample data:
    # sample_data = {
    #     'job_description': [
    #         "We are looking for a Python developer with experience in machine learning, SQL, and Django.",
    #         "Seeking a data scientist with skills in R, statistics, and data visualization.",
    #         "Frontend developer needed with JavaScript, React, and CSS expertise."
    #     ]
    # }
    # df = pd.DataFrame(sample_data)

    # Process the dataset
    results_df = process_dataset(
        df, 'description', confidence_threshold=0.7)

    # Display results
    print("\n=== EXTRACTION RESULTS ===")
    for idx, row in results_df.iterrows():
        print(
            f"\n--- Job {idx + 1}: {row.get('title', 'N/A')} at {row.get('company', 'N/A')} ---")
        print(f"Description preview: {str(row['description'])[:150]}...")
        print(
            f"Skills found ({len(row['skills_list'])}): {row['skills_list']}")
        if row['extracted_skills']:
            confidence_scores = [s['confidence']
                                 for s in row['extracted_skills']]
            print(
                f"Top confidence scores: {sorted(confidence_scores, reverse=True)[:3]}")

    # Get top skills across all jobs
    print("\n=== TOP SKILLS ACROSS ALL JOBS ===")
    top_skills = get_top_skills(results_df, top_n=15)
    print(top_skills)

    # Save results with original data + skills
    output_file = 'job_data_with_extracted_skills.csv'
    try:
        results_df.to_csv(output_file, index=False)
        print(f"\n=== Results saved to {output_file} ===")
    except Exception as e:
        print(f"Error saving results: {e}")

    # Additional analysis for your job data
    print("\n=== ADDITIONAL INSIGHTS ===")
    print(f"Total jobs processed: {len(results_df)}")
    jobs_with_skills = len(results_df[results_df['skill_count'] > 0])
    print(f"Jobs with skills found: {jobs_with_skills}")
    if len(results_df) > 0:
        print(
            f"Average skills per job: {results_df['skill_count'].mean():.1f}")
        print(f"Max skills in a single job: {results_df['skill_count'].max()}")

        # Show distribution of skill counts
        skill_distribution = results_df['skill_count'].value_counts(
        ).sort_index()
        print(f"\nSkill count distribution:")
        for count, frequency in skill_distribution.head(10).items():
            print(f"  {count} skills: {frequency} jobs")
