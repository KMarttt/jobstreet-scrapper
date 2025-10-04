from matplotlib.patches import Patch
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import glob

# ===== CONFIGURATION =====
INPUT_FOLDER = 'final_data_for_analysis'
OUTPUT_BASE_FOLDER = 'figures'  # Base folder for all outputs
# =========================

# Set style for better-looking plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)


def process_csv_file(file_path, output_folder):
    """Process a single CSV file and generate all visualizations"""

    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Load the data
    df = pd.read_csv(file_path)

    # Get file name for titles
    file_name = os.path.basename(file_path)
    country_code = file_name.split('_')[0].upper()

    # Display basic info
    print(f"\n{'='*60}")
    print(f"Processing: {file_name}")
    print(f"{'='*60}")
    print("Dataset Overview:")
    print(df.head())
    print("\nDataset Shape:", df.shape)
    print("\nSkill Types Distribution:")
    print(df['Type'].value_counts())

    # Define colors
    colors = {'tool': '#3498db', 'language': '#e74c3c',
              'platform': '#2ecc71', 'Concept': '#f39c12'}

    # 1. Top 20 Skills by Frequency (All Types)
    plt.figure(figsize=(14, 8))
    top_20 = df.nlargest(20, 'Frequency')
    bar_colors = [colors.get(t, '#95a5a6') for t in top_20['Type']]

    plt.barh(range(len(top_20)), top_20['Frequency'], color=bar_colors)
    plt.yticks(range(len(top_20)), top_20['Item'])
    plt.xlabel('Frequency in Job Postings', fontsize=12, fontweight='bold')
    plt.title(f'Top 20 Most In-Demand Skills in {country_code} Job Market',
              fontsize=14, fontweight='bold')
    plt.gca().invert_yaxis()

    # Add legend
    legend_elements = [Patch(facecolor=colors[t], label=t)
                       for t in colors.keys()]
    plt.legend(handles=legend_elements, loc='lower right')
    plt.tight_layout()
    plt.savefig(os.path.join(output_folder, 'top_20_skills.png'),
                dpi=300, bbox_inches='tight')
    plt.close()

    # 2. Skills by Type - Separate Bar Charts
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    skill_types = df['Type'].unique()

    for idx, skill_type in enumerate(skill_types):
        ax = axes[idx // 2, idx % 2]
        type_data = df[df['Type'] == skill_type].nlargest(10, 'Frequency')

        ax.barh(range(len(type_data)),
                type_data['Frequency'], color=colors.get(skill_type, '#95a5a6'))
        ax.set_yticks(range(len(type_data)))
        ax.set_yticklabels(type_data['Item'])
        ax.set_xlabel('Frequency', fontweight='bold')
        ax.set_title(f'Top 10 {skill_type.capitalize()} Skills',
                     fontsize=12, fontweight='bold')
        ax.invert_yaxis()

    plt.tight_layout()
    plt.savefig(os.path.join(output_folder, 'skills_by_type.png'),
                dpi=300, bbox_inches='tight')
    plt.close()

    # 3. Frequency Distribution by Type (Box Plot)
    plt.figure(figsize=(10, 6))
    df.boxplot(column='Frequency', by='Type', ax=plt.gca())
    plt.xlabel('Skill Type', fontsize=12, fontweight='bold')
    plt.ylabel('Frequency', fontsize=12, fontweight='bold')
    plt.title('Frequency Distribution by Skill Type',
              fontsize=14, fontweight='bold')
    plt.suptitle('')  # Remove default title
    plt.tight_layout()
    plt.savefig(os.path.join(output_folder, 'frequency_distribution.png'),
                dpi=300, bbox_inches='tight')
    plt.close()

    # 4. Pie Chart - Total Frequency by Type
    plt.figure(figsize=(10, 8))
    type_totals = df.groupby('Type')['Frequency'].sum()
    plt.pie(type_totals, labels=type_totals.index, autopct='%1.1f%%',
            colors=[colors.get(t, '#95a5a6') for t in type_totals.index],
            startangle=90, textprops={'fontsize': 12, 'fontweight': 'bold'})
    plt.title('Market Share: Total Demand by Skill Type',
              fontsize=14, fontweight='bold', pad=20)
    plt.tight_layout()
    plt.savefig(os.path.join(output_folder, 'skill_type_distribution.png'),
                dpi=300, bbox_inches='tight')
    plt.close()

    # 5. Ranking vs Frequency Scatter Plot
    plt.figure(figsize=(12, 6))
    for skill_type in df['Type'].unique():
        type_data = df[df['Type'] == skill_type]
        plt.scatter(type_data['Ranking'], type_data['Frequency'],
                    label=skill_type, alpha=0.7, s=100, color=colors.get(skill_type, '#95a5a6'))

    plt.xlabel('Ranking', fontsize=12, fontweight='bold')
    plt.ylabel('Frequency', fontsize=12, fontweight='bold')
    plt.title('Skill Ranking vs Frequency Distribution',
              fontsize=14, fontweight='bold')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(output_folder, 'ranking_vs_frequency.png'),
                dpi=300, bbox_inches='tight')
    plt.close()

    # 6. Summary Statistics
    print("\n" + "="*60)
    print(f"SUMMARY STATISTICS - {file_name}")
    print("="*60)
    print("\nFrequency Statistics by Type:")
    print(df.groupby('Type')['Frequency'].describe())

    print("\n\nTop 5 Most In-Demand Skills Overall:")
    top_5 = df.nlargest(5, 'Frequency')[
        ['Ranking', 'Type', 'Item', 'Frequency']]
    print(top_5.to_string(index=False))

    print("\n\nTop Skill in Each Category:")
    for skill_type in df['Type'].unique():
        top_skill = df[df['Type'] == skill_type].nlargest(1, 'Frequency')
        print(
            f"{skill_type.capitalize()}: {top_skill['Item'].values[0]} (Frequency: {top_skill['Frequency'].values[0]})")

    print(f"\nFigures saved to: {output_folder}")
    print("="*60)


def main():
    """Main function to process all CSV files in the input folder"""

    # Find all CSV files in the input folder
    csv_files = glob.glob(os.path.join(INPUT_FOLDER, '*.csv'))

    if not csv_files:
        print(f"No CSV files found in '{INPUT_FOLDER}' folder.")
        return

    print(f"Found {len(csv_files)} CSV file(s) to process:")
    for file in csv_files:
        print(f"  - {os.path.basename(file)}")

    # Process each CSV file
    for csv_file in csv_files:
        # Extract the base name without extension (e.g., 'vn_summary' from 'vn_summary.csv')
        base_name = os.path.splitext(os.path.basename(csv_file))[0]

        # Create output folder path (e.g., 'figures/vn_summary')
        output_folder = os.path.join(OUTPUT_BASE_FOLDER, base_name)

        try:
            # Process the file
            process_csv_file(csv_file, output_folder)
        except Exception as e:
            print(f"\nError processing {csv_file}: {str(e)}")
            continue

    print(f"\n{'='*60}")
    print("All files processed successfully!")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
