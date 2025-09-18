import pandas as pd
import re


def clean_skills_data(input_file, output_file):
    """
    Clean skills data by removing non-skill entries while preserving legitimate technical terms
    """

    # Load the data
    print("Loading data...")
    df = pd.read_csv(input_file)

    # Step 1: Remove entries with only punctuation or symbols
    print("Step 1: Removing pure punctuation/symbols...")
    punctuation_patterns = [
        r'^[^a-zA-Z0-9]*$',  # Only punctuation/symbols
        r'^[,\.\)\(\-\+\*\/\#\&\%\$\@]+$',  # Pure punctuation combinations
        r'^\s*$',  # Empty or whitespace only
        r'^[\)\(\,\.\-\+\*\/\#]{1,3}$',  # Short punctuation combinations
    ]

    mask = df['item'].str.contains(
        '|'.join(punctuation_patterns), regex=True, na=True)
    df = df[~mask]

    # Step 2: Remove common non-skill words
    print("Step 2: Removing common non-skill words...")
    non_skills = {
        'of', 'and', 'the', 'in', 'to', 'for', 'with', 'on', 'at', 'by',
        'from', 'as', 'or', 'but', 'if', 'when', 'where', 'how', 'what',
        'that', 'this', 'these', 'those', 'all', 'any', 'some', 'more',
        'most', 'other', 'such', 'no', 'nor', 'not', 'only', 'own', 'same',
        'so', 'than', 'too', 'very', 'can', 'will', 'just', 'should', 'now',
        'up', 'out', 'down', 'off', 'over', 'under', 'again', 'further',
        'then', 'once', 'here', 'there', 'why', 'stress', 'different',
        'language', 'word', 'a', 'an', 'be', 'been', 'being', 'have', 'has',
        'had', 'do', 'does', 'did', 'get', 'got', 'make', 'made', 'take',
        'taken', 'come', 'came', 'go', 'went', 'see', 'seen', 'know', 'knew',
        'think', 'thought', 'say', 'said', 'work', 'worked', 'look', 'looked',
        'use', 'used', 'find', 'found', 'give', 'gave', 'tell', 'told',
        'become', 'became', 'leave', 'left', 'feel', 'felt', 'put', 'bring',
        'brought', 'begin', 'began', 'keep', 'kept', 'hold', 'held', 'write',
        'wrote', 'stand', 'stood', 'hear', 'heard', 'let', 'mean', 'meant',
        'set', 'move', 'moved', 'try', 'tried', 'change', 'changed', 'play',
        'played', 'run', 'ran', 'turn', 'turned', 'start', 'started', 'show',
        'showed', 'grow', 'grew', 'open', 'opened', 'walk', 'walked', 'win',
        'won', 'talk', 'talked', 'sit', 'sat', 'stop', 'stopped', 'lie', 'lay',
        'buy', 'bought', 'spend', 'spent', 'cut', 'hit', 'eat', 'ate', 'fall',
        'fell', 'reach', 'reached', 'kill', 'killed', 'remain', 'remained'
    }

    # Remove common words but preserve them if they appear as legitimate technical skills
    df = df[~df['item'].str.lower().isin(non_skills)]

    # Step 3: Remove very short entries that are likely noise (but preserve known technical acronyms)
    print("Step 3: Handling short entries...")

    # Known legitimate technical terms that might be short
    known_tech_terms = {
        'AI', 'ML', 'BI', 'UI', 'UX', 'IT', 'HR', 'QA', 'R', 'C', 'GO',
        'SQL', 'API', 'AWS', 'GCP', 'CDP', 'ERP', 'CRM', 'ETL', 'UAT', 'SIT',
        'CI', 'CD', 'IoT', 'VR', 'AR', 'NLP', 'OCR', 'PDF', 'CSV', 'XML',
        'JSON', 'HTML', 'CSS', 'PHP', 'ASP', 'NET', 'IDE', 'SDK', 'JVM',
        'CPU', 'GPU', 'RAM', 'SSD', 'HDD', 'USB', 'LAN', 'WAN', 'VPN',
        'SSL', 'TLS', 'SSH', 'FTP', 'HTTP', 'HTTPS', 'TCP', 'UDP', 'DNS',
        'DHCP', 'SMTP', 'POP', 'IMAP', 'LDAP', 'SAML', 'OAuth', 'JWT',
        'REST', 'SOAP', 'GraphQL', 'NoSQL', 'ACID', 'CRUD', 'ORM', 'MVC',
        'MVP', 'MVVM', 'SPA', 'PWA', 'RPA', 'BPM', 'CMS', 'LMS', 'ELT',
        'KPI', 'ROI', 'SLA', 'SOP', 'P2P', 'B2B', 'B2C', 'CPC', 'CPM',
        'CTR', 'CPA', 'LTV', 'CAC', 'MRR', 'ARR', 'EBITDA', 'NPV', 'IRR'
    }

    # Keep entries that are either:
    # 1. Known technical terms (regardless of length)
    # 2. Longer than 2 characters
    # 3. Match technical patterns (like containing numbers, dots, etc.)
    technical_patterns = [
        r'^\w+\d+',  # Terms with numbers like HTML5, Python3
        r'^\w*\.\w+',  # Terms with dots like .NET, node.js
        r'^\w*#\w*',  # Terms with # like C#, F#
        r'^\w*\+\+$',  # Terms like C++
        r'^[A-Z]{2,}$',  # All caps acronyms
    ]

    # Create mask for entries to keep
    keep_mask = (
        df['item'].isin(known_tech_terms) |  # Known technical terms
        (df['item'].str.len() > 2) |  # Longer than 2 characters
        df['item'].str.contains(
            '|'.join(technical_patterns), regex=True)  # Technical patterns
    )

    df = df[keep_mask]

    # Step 4: Remove entries that are fragments or incomplete words
    print("Step 4: Removing word fragments...")

    # Remove entries that start or end with common prefixes/suffixes that suggest fragments
    fragment_patterns = [
        r'^##\w+',  # Starts with ##
        r'^\w+##',  # Ends with ##
        r'^-\w+',   # Starts with dash (likely fragment)
        r'^\w+-$',  # Ends with dash (likely fragment)
        r'^\w+,$',  # Ends with comma
        r'^,\w+',   # Starts with comma
        r'^\(\w+',  # Starts with parenthesis
        r'^\w+\)$',  # Ends with parenthesis
    ]

    # But preserve legitimate lowercase technical terms
    lowercase_exceptions = {
        'python', 'java', 'javascript', 'typescript', 'kotlin', 'swift',
        'go', 'rust', 'scala', 'ruby', 'php', 'perl', 'bash', 'shell',
        'linux', 'ubuntu', 'centos', 'debian', 'windows', 'macos',
        'docker', 'kubernetes', 'jenkins', 'ansible', 'terraform',
        'mysql', 'postgresql', 'mongodb', 'redis', 'elasticsearch',
        'apache', 'nginx', 'tomcat', 'node', 'react', 'angular', 'vue',
        'jquery', 'bootstrap', 'sass', 'less', 'webpack', 'babel',
        'git', 'svn', 'mercurial', 'jira', 'confluence', 'slack',
        'tableau', 'powerbi', 'excel', 'word', 'outlook', 'teams',
        'photoshop', 'illustrator', 'sketch', 'figma', 'autocad',
        'matlab', 'spss', 'stata', 'sas', 'r', 'rstudio'
    }

    fragment_mask = df['item'].str.contains(
        '|'.join(fragment_patterns), regex=True)
    lowercase_mask = df['item'].str.lower().isin(lowercase_exceptions)

    # Remove fragments but keep lowercase exceptions
    df = df[~fragment_mask | lowercase_mask]
    print(f"After removing fragments: {df.shape}")

    # Step 5: Remove pure numbers
    print("Step 5: Removing pure numbers...")
    df = df[~df['item'].str.match(r'^\d+$')]
    print(f"After removing pure numbers: {df.shape}")

    # Step 6: Clean up whitespace
    print("Step 6: Cleaning whitespace...")
    df['item'] = df['item'].str.strip()
    df = df[df['item'] != '']
    print(f"After cleaning whitespace: {df.shape}")

    # Step 7: Remove exact duplicates first
    print("Step 7: Removing exact duplicates...")
    original_size = len(df)
    df = df.drop_duplicates()
    print(f"Removed {original_size - len(df)} exact duplicate rows")

    # Step 8: Consolidate case-insensitive duplicates and sum frequencies
    print("Step 8: Consolidating case-insensitive duplicates...")

    # Create a mapping from lowercase to the most common casing
    def get_preferred_casing(group):
        """Return the most frequent casing, or the one with most uppercase if tied"""
        casing_counts = group['item'].value_counts()
        if len(casing_counts) == 1:
            return casing_counts.index[0]

        # If multiple casings exist, prefer the one that appears most frequently
        # If tied, prefer the one with proper capitalization (first letter uppercase)
        most_common = casing_counts.index[0]

        # Check if there's a properly capitalized version
        proper_cases = [
            case for case in casing_counts.index if case[0].isupper() and case[1:].islower()]
        if proper_cases:
            return proper_cases[0]

        return most_common

    # Group by lowercase version and consolidate
    consolidation_stats = []

    # Create lowercase grouping key
    df['item_lower'] = df['item'].str.lower()

    # Group by lowercase and aggregate
    consolidated_rows = []

    for lower_item, group in df.groupby('item_lower'):
        # Get preferred casing
        preferred_casing = get_preferred_casing(group)

        # Sum frequencies
        total_frequency = group['frequency'].sum()

        # For other columns, take values from the most frequent casing
        most_frequent_row = group.loc[group['item'] ==
                                      preferred_casing].iloc[0] if preferred_casing in group['item'].values else group.iloc[0]

        # Create consolidated row
        consolidated_row = most_frequent_row.copy()
        consolidated_row['item'] = preferred_casing
        consolidated_row['frequency'] = total_frequency

        consolidated_rows.append(consolidated_row)

        # Track consolidation stats
        if len(group) > 1:
            original_items = group['item'].tolist()
            original_freqs = group['frequency'].tolist()
            consolidation_stats.append({
                'consolidated_to': preferred_casing,
                'original_items': original_items,
                'original_frequencies': original_freqs,
                'total_frequency': total_frequency
            })

    # Create new dataframe from consolidated rows
    df = pd.DataFrame(consolidated_rows)
    df = df.drop('item_lower', axis=1)  # Remove helper column

    # Save cleaned data
    print(f"\nSaving cleaned data to {output_file}...")
    df.to_csv(output_file, index=False)

    return df


if __name__ == "__main__":
    # Usage
    # Change this to your input file
    input_filename = "skills summary/careerviet_vn_skills_with_jobs.csv"
    # Change this to your desired output file
    output_filename = "cleaned_skills_data.csv"

    try:
        cleaned_data = clean_skills_data(input_filename, output_filename)
        print(f"\nData cleaning completed successfully!")
        print(f"Cleaned data saved to: {output_filename}")

    except FileNotFoundError:
        print(f"Error: Could not find the input file '{input_filename}'")
        print("Please make sure the file exists and the path is correct.")
    except Exception as e:
        print(f"An error occurred: {e}")
