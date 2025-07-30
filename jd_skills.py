import pandas as pd
import re
from collections import Counter

def get_skills(csv_file="jobstreet_jobs.csv"):
    # Load the CSV 
    df = pd.read_csv(csv_file)

    def extract_capitalized_words(text):
        # Match one or more consecutive capitalized words
        pattern = r'\b(?:[A-Z][a-z]+|[A-Z]{2,})(?:\s(?:[A-Z][a-z]+|[A-Z]{2,}))*\b'
        return re.findall(pattern, str(text))

    capitalized_phrases = Counter()
    # A Counter is like a dictionary, but it’s specially designed to count how many times each item appears.
    # Ex: Counter(["Python", "Excel", "Python"]) Counter({'Python': 2, 'Excel': 1})

    for description in df['job_description_raw']:
        phrases = extract_capitalized_words(description)
        unique_phrases = set(phrases)
        capitalized_phrases.update(unique_phrases)

    return capitalized_phrases


if __name__ == "__main__":
    csv_file = "jobstreet_jobs.csv"
    skill_set = get_skills(csv_file)

    for phrase, count in skill_set.most_common(80):
            print(f"{phrase}: {count}")


