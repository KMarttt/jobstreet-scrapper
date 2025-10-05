from matplotlib.patches import Patch
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import glob

# ===== CONFIGURATION =====
INPUT_FOLDER = 'final_data_for_analysis'
OUTPUT_FOLDER = 'figures/combined'
# =========================

# Create output folder if it doesn't exist
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Set style for better-looking plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (16, 10)

# Find all CSV files
csv_files = glob.glob(os.path.join(INPUT_FOLDER, '*.csv'))

if not csv_files:
    print(f"No CSV files found in '{INPUT_FOLDER}' folder.")
    exit()

print(f"Found {len(csv_files)} CSV file(s) to combine:")

# Load and combine all CSV files
all_data = []
for csv_file in csv_files:
    df = pd.read_csv(csv_file)
    # Extract country code from filename
    country = os.path.splitext(os.path.basename(csv_file))[
        0].split('_')[0].upper()
    df['Country'] = country
    all_data.append(df)
    print(f"  - {os.path.basename(csv_file)} ({len(df)} skills)")

# Combine all dataframes
combined_df = pd.concat(all_data, ignore_index=True)

print(f"\nCombined Dataset Shape: {combined_df.shape}")
print(f"Countries included: {', '.join(combined_df['Country'].unique())}")

# Define colors
colors = {'tool': '#3498db', 'language': '#e74c3c',
          'platform': '#2ecc71', 'Concept': '#f39c12'}

country_colors = {
    'VN': '#e74c3c',    # Vietnam - Red
    'ID': '#3498db',    # Indonesia - Blue
    'PH': '#2ecc71',    # Philippines - Green
    'SG': '#9b59b6',    # Singapore - Purple
    'MY': '#f39c12',    # Malaysia - Orange
    'TH': '#e67e22',    # Thailand - Dark Orange
    'MM': '#1abc9c',    # Myanmar - Turquoise
    'KH': '#34495e',    # Cambodia - Dark Gray
    'LA': '#16a085',    # Laos - Teal
    'BN': '#c0392b'     # Brunei - Dark Red
}

# ===== 1. TOP 20 SKILLS ACROSS ALL COUNTRIES =====
plt.figure(figsize=(16, 10))
# Aggregate frequency by Item across all countries
skill_totals = combined_df.groupby(['Item', 'Type'])[
    'Frequency'].sum().reset_index()
top_20_overall = skill_totals.nlargest(20, 'Frequency')

bar_colors = [colors.get(t, '#95a5a6') for t in top_20_overall['Type']]
plt.barh(range(len(top_20_overall)),
         top_20_overall['Frequency'], color=bar_colors)
plt.yticks(range(len(top_20_overall)), top_20_overall['Item'])
plt.xlabel('Total Frequency Across All Countries',
           fontsize=14, fontweight='bold')
plt.title('Top 20 Most In-Demand Skills',
          fontsize=16, fontweight='bold')
plt.gca().invert_yaxis()

legend_elements = [Patch(facecolor=colors[t], label=t) for t in colors.keys()]
plt.legend(handles=legend_elements, loc='lower right', fontsize=11)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_FOLDER, '01_top_20_skills_asean.png'),
            dpi=300, bbox_inches='tight')
plt.close()

# ===== 2. SKILLS BY TYPE - COMBINED =====
fig, axes = plt.subplots(2, 2, figsize=(18, 14))
skill_types = combined_df['Type'].unique()

for idx, skill_type in enumerate(skill_types):
    ax = axes[idx // 2, idx % 2]
    type_data = combined_df[combined_df['Type'] == skill_type].groupby(
        'Item')['Frequency'].sum().reset_index()
    type_data = type_data.nlargest(15, 'Frequency')

    ax.barh(range(len(type_data)), type_data['Frequency'],
            color=colors.get(skill_type, '#95a5a6'))
    ax.set_yticks(range(len(type_data)))
    ax.set_yticklabels(type_data['Item'])
    ax.set_xlabel('Total Frequency', fontweight='bold', fontsize=11)
    ax.set_title(f'Top 15 {skill_type.capitalize()} Skills (ASEAN)',
                 fontsize=13, fontweight='bold')
    ax.invert_yaxis()

plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_FOLDER, '02_skills_by_type_asean.png'),
            dpi=300, bbox_inches='tight')
plt.close()

# ===== 3. SKILL TYPE DISTRIBUTION BY COUNTRY =====
plt.figure(figsize=(14, 8))
country_type_totals = combined_df.groupby(['Country', 'Type'])[
    'Frequency'].sum().reset_index()
country_type_pivot = country_type_totals.pivot(
    index='Country', columns='Type', values='Frequency')

country_type_pivot.plot(kind='bar', stacked=True,
                        color=[colors.get(t, '#95a5a6')
                               for t in country_type_pivot.columns],
                        ax=plt.gca(), width=0.7)
plt.xlabel('Country', fontsize=13, fontweight='bold')
plt.ylabel('Total Frequency', fontsize=13, fontweight='bold')
plt.title('Skill Type Distribution by Country', fontsize=15, fontweight='bold')
plt.legend(title='Skill Type', fontsize=11, title_fontsize=12)
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_FOLDER, '03_skill_distribution_by_country.png'),
            dpi=300, bbox_inches='tight')
plt.close()

# ===== 4. TOP SKILLS COMPARISON ACROSS COUNTRIES =====
top_10_skills = skill_totals.nlargest(10, 'Frequency')['Item'].tolist()

plt.figure(figsize=(16, 10))
comparison_data = combined_df[combined_df['Item'].isin(top_10_skills)]
comparison_pivot = comparison_data.pivot_table(index='Item', columns='Country',
                                               values='Frequency', aggfunc='sum', fill_value=0)

comparison_pivot.plot(kind='barh', ax=plt.gca(), width=0.8,
                      color=[country_colors.get(c, '#95a5a6') for c in comparison_pivot.columns])
plt.xlabel('Frequency', fontsize=13, fontweight='bold')
plt.ylabel('Skill', fontsize=13, fontweight='bold')
plt.title('Top 10 ASEAN Skills: Country Comparison',
          fontsize=15, fontweight='bold')
plt.legend(title='Country', bbox_to_anchor=(
    1.05, 1), loc='upper left', fontsize=10)
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_FOLDER, '04_top_skills_by_country.png'),
            dpi=300, bbox_inches='tight')
plt.close()

# ===== 5. HEATMAP: TOP 20 SKILLS ACROSS COUNTRIES =====
plt.figure(figsize=(14, 12))
top_20_skills = skill_totals.nlargest(20, 'Frequency')['Item'].tolist()
heatmap_data = combined_df[combined_df['Item'].isin(top_20_skills)]
heatmap_pivot = heatmap_data.pivot_table(index='Item', columns='Country',
                                         values='Frequency', aggfunc='sum', fill_value=0)

sns.heatmap(heatmap_pivot, annot=True, fmt='.0f', cmap='YlOrRd',
            cbar_kws={'label': 'Frequency'}, linewidths=0.5)
plt.title('Top 20 Skills Heatmap: Demand Across Countries',
          fontsize=15, fontweight='bold', pad=20)
plt.xlabel('Country', fontsize=13, fontweight='bold')
plt.ylabel('Skill', fontsize=13, fontweight='bold')
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_FOLDER, '05_skills_heatmap.png'),
            dpi=300, bbox_inches='tight')
plt.close()

# ===== 6. MARKET SHARE PIE CHARTS =====
fig, axes = plt.subplots(1, 2, figsize=(18, 8))

# By Type (ASEAN)
type_totals = combined_df.groupby('Type')['Frequency'].sum()
axes[0].pie(type_totals, labels=type_totals.index, autopct='%1.1f%%',
            colors=[colors.get(t, '#95a5a6') for t in type_totals.index],
            startangle=90, textprops={'fontsize': 12, 'fontweight': 'bold'})
axes[0].set_title('ASEAN Market Share by Skill Type',
                  fontsize=14, fontweight='bold', pad=20)

# By Country
country_totals = combined_df.groupby('Country')['Frequency'].sum()
axes[1].pie(country_totals, labels=country_totals.index, autopct='%1.1f%%',
            colors=[country_colors.get(c, '#95a5a6')
                    for c in country_totals.index],
            startangle=90, textprops={'fontsize': 12, 'fontweight': 'bold'})
axes[1].set_title('Total Demand by Country',
                  fontsize=14, fontweight='bold', pad=20)

plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_FOLDER, '06_market_share.png'),
            dpi=300, bbox_inches='tight')
plt.close()

# ===== 7. COUNTRY-SPECIFIC TOP 5 SKILLS =====
countries = combined_df['Country'].unique()
n_countries = len(countries)
cols = 3
rows = (n_countries + cols - 1) // cols

fig, axes = plt.subplots(rows, cols, figsize=(18, rows * 4))
axes = axes.flatten() if n_countries > 1 else [axes]

for idx, country in enumerate(countries):
    ax = axes[idx]
    country_data = combined_df[combined_df['Country'] == country]
    top_5_country = country_data.groupby(['Item', 'Type'])[
        'Frequency'].sum().reset_index()
    top_5_country = top_5_country.nlargest(5, 'Frequency')

    bar_colors = [colors.get(t, '#95a5a6') for t in top_5_country['Type']]
    ax.barh(range(len(top_5_country)),
            top_5_country['Frequency'], color=bar_colors)
    ax.set_yticks(range(len(top_5_country)))
    ax.set_yticklabels(top_5_country['Item'])
    ax.set_xlabel('Frequency', fontweight='bold')
    ax.set_title(f'Top 5 Skills in {country}', fontsize=12, fontweight='bold')
    ax.invert_yaxis()

# Hide extra subplots
for idx in range(n_countries, len(axes)):
    axes[idx].axis('off')

plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_FOLDER, '07_top_5_by_country.png'),
            dpi=300, bbox_inches='tight')
plt.close()

# ===== 8. SUMMARY STATISTICS =====
print("\n" + "="*80)
print("COMBINED SUMMARY STATISTICS")
print("="*80)

print("\n1. Overall Statistics:")
print(f"   Total unique skills: {combined_df['Item'].nunique()}")
print(f"   Total data points: {len(combined_df)}")
print(f"   Countries analyzed: {len(combined_df['Country'].unique())}")

print("\n2. ASEAN Top 10 Skills:")
print(skill_totals.nlargest(10, 'Frequency')[
      ['Item', 'Type', 'Frequency']].to_string(index=False))

print("\n3. Skill Type Distribution (ASEAN):")
print(combined_df.groupby('Type')[
      'Frequency'].sum().sort_values(ascending=False))

print("\n4. Top Skill per Country:")
for country in sorted(combined_df['Country'].unique()):
    country_data = combined_df[combined_df['Country'] == country]
    country_skills = country_data.groupby('Item')['Frequency'].sum()
    top_skill = country_skills.idxmax()
    top_freq = country_skills.max()
    print(f"   {country}: {top_skill} (Frequency: {top_freq:.0f})")

print("\n5. Most Consistent Skills (appearing in most countries):")
skill_countries = combined_df.groupby(
    'Item')['Country'].nunique().sort_values(ascending=False)
print(skill_countries.head(10))

print("\n" + "="*80)
print(f"All visualizations saved to: {OUTPUT_FOLDER}")
print("="*80)
