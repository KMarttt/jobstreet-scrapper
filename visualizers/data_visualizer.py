from matplotlib.patches import Patch
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
import os

# ===== CONFIGURATION =====
OUTPUT_FOLDER = 'figures/vn'  # Change this to your desired folder path
# Change this to your data file path
DATA_FILE = 'final_data_for_analysis/vn_summary.csv'
# =========================

# Create output folder if it doesn't exist
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Set style for better-looking plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

# Load the data
df = pd.read_csv(DATA_FILE)

# Display basic info
print("Dataset Overview:")
print(df.head())
print("\nDataset Shape:", df.shape)
print("\nSkill Types Distribution:")
print(df['Type'].value_counts())

# 1. Top 20 Skills by Frequency (All Types)
plt.figure(figsize=(14, 8))
top_20 = df.nlargest(20, 'Frequency')
colors = {'tool': '#3498db', 'language': '#e74c3c',
          'platform': '#2ecc71', 'Concept': '#f39c12'}
bar_colors = [colors[t] for t in top_20['Type']]

plt.barh(range(len(top_20)), top_20['Frequency'], color=bar_colors)
plt.yticks(range(len(top_20)), top_20['Item'])
plt.xlabel('Frequency in Job Postings', fontsize=12, fontweight='bold')
plt.title('Top 20 Most In-Demand Skills in PH Job Market',
          fontsize=14, fontweight='bold')
plt.gca().invert_yaxis()

# Add legend
legend_elements = [Patch(facecolor=colors[t], label=t) for t in colors.keys()]
plt.legend(handles=legend_elements, loc='lower right')
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_FOLDER, 'top_20_skills.png'),
            dpi=300, bbox_inches='tight')
# plt.show()

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
plt.savefig(os.path.join(OUTPUT_FOLDER, 'skills_by_type.png'),
            dpi=300, bbox_inches='tight')
# plt.show()

# 3. Frequency Distribution by Type (Box Plot)
plt.figure(figsize=(10, 6))
df.boxplot(column='Frequency', by='Type', ax=plt.gca())
plt.xlabel('Skill Type', fontsize=12, fontweight='bold')
plt.ylabel('Frequency', fontsize=12, fontweight='bold')
plt.title('Frequency Distribution by Skill Type',
          fontsize=14, fontweight='bold')
plt.suptitle('')  # Remove default title
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_FOLDER, 'frequency_distribution.png'),
            dpi=300, bbox_inches='tight')
# plt.show()

# 4. Pie Chart - Total Frequency by Type
plt.figure(figsize=(10, 8))
type_totals = df.groupby('Type')['Frequency'].sum()
plt.pie(type_totals, labels=type_totals.index, autopct='%1.1f%%',
        colors=[colors.get(t, '#95a5a6') for t in type_totals.index],
        startangle=90, textprops={'fontsize': 12, 'fontweight': 'bold'})
plt.title('Market Share: Total Demand by Skill Type',
          fontsize=14, fontweight='bold', pad=20)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_FOLDER, 'skill_type_distribution.png'),
            dpi=300, bbox_inches='tight')
# plt.show()

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
plt.savefig(os.path.join(OUTPUT_FOLDER, 'ranking_vs_frequency.png'),
            dpi=300, bbox_inches='tight')
# plt.show()

# 6. Summary Statistics
print("\n" + "="*60)
print("SUMMARY STATISTICS")
print("="*60)
print("\nFrequency Statistics by Type:")
print(df.groupby('Type')['Frequency'].describe())

print("\n\nTop 5 Most In-Demand Skills Overall:")
top_5 = df.nlargest(5, 'Frequency')[['Ranking', 'Type', 'Item', 'Frequency']]
print(top_5.to_string(index=False))

print("\n\nTop Skill in Each Category:")
for skill_type in df['Type'].unique():
    top_skill = df[df['Type'] == skill_type].nlargest(1, 'Frequency')
    print(
        f"{skill_type.capitalize()}: {top_skill['Item'].values[0]} (Frequency: {top_skill['Frequency'].values[0]})")

print("\n" + "="*60)
