import pandas as pd

# Read the data
df = pd.read_csv(
    'sg_data_reduced.csv')

# Calculate current counts
total = len(df)
knowledge_count = len(df[df['type'] == 'knowledge'])
skill_count = len(df[df['type'] == 'skill'])

print(f"Original dataset: {total} rows")
print(f"Knowledge: {knowledge_count} ({knowledge_count/total*100:.1f}%)")
print(f"Skill: {skill_count} ({skill_count/total*100:.1f}%)")

# Separate by type
knowledge_df = df[df['type'] == 'knowledge']
skill_df = df[df['type'] == 'skill']

# Take half of each type
knowledge_half = knowledge_df.sample(n=knowledge_count//2, random_state=42)
skill_half = skill_df.sample(n=skill_count//2, random_state=42)

# Combine the halves
df_half = pd.concat([knowledge_half, skill_half], ignore_index=True)

# Shuffle the result
df_half = df_half.sample(frac=1, random_state=42).reset_index(drop=True)

# Verify the results
print(f"\nHalved dataset: {len(df_half)} rows")
print(
    f"Knowledge: {len(df_half[df_half['type'] == 'knowledge'])} ({len(df_half[df_half['type'] == 'knowledge'])/len(df_half)*100:.1f}%)")
print(
    f"Skill: {len(df_half[df_half['type'] == 'skill'])} ({len(df_half[df_half['type'] == 'skill'])/len(df_half)*100:.1f}%)")

# Save the result
df_half.to_csv('sg_data_reduced_2x.csv', index=False)
