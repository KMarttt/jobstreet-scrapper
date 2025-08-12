import pandas as pd
from deep_translator import GoogleTranslator

# === CONFIG === 
input_file = "data/vietnamworks_vn_data-analyst.csv" # Your csv file
output_file = "data/vietnamworks_vn_data-analyst_translated.csv" # Your output file

# Column to translate
translate_cols = ["title", "location", "skill", "description","requirement", "company_description"]
long_text_cols = ["description", "requirement", "company_description"]

# Max length for each chunk
max_len = 4500

# === READ CSV ===
df = pd.read_csv(input_file, encoding="utf-8-sig")

#  === TRANSLATION ===
translator = GoogleTranslator(source='auto', target='en')    

def translate_text(text):
    # If the text is empty or NaN, return it as is
    if pd.isna(text) or str(text).strip() == "":
        return text
    
    text = str(text)
    text_bytes = text.encode('utf-8')
    total_text_byte = len(text_bytes)
    
    # If text fits in one chunk, just translate and return
    if total_text_byte <= max_len:
        return translator.translate(text)
    
    # Otherwise, split into multiple chunks
    print("Translating long text...")
    translated_chunks = []
    start = 0
    

    while start < total_text_byte:
        # Pick an end position for this chunk
        end = min(start + max_len, total_text_byte)

        # Avoid cutting a UTF-8 character in half
        # UTF-8 continuation bytes start with bits '10'; end at the first byte that starts with '11' (leading byte)
        while end > start and end < total_text_byte and (text_bytes[end] & 0b11000000) == 0b10000000:
            end -= 1

        # Decode bytes into text so we can work with characters
        chunk_bytes = text_bytes[start:end]
        chunk_text = chunk_bytes.decode("utf-8")
        last_space = chunk_text.rfind(" ")

        # Try to split at the last space in this chunk (there must be a space and it's not the last character)
        if last_space != -1 and last_space != len(chunk_text) - 1:
            prefix = chunk_text[: last_space + 1] # Include the space
            end = start + len(prefix.encode("utf-8")) # The new end is the end of the prefix in bytes
            chunk_bytes = text_bytes[start:end] 
            chunk_text = chunk_bytes.decode("utf-8")
        
        # Translate this chunk and add to results to the list
        translated_chunks.append(translator.translate(chunk_text))

        # Move start to the end of this chunk for the next loop
        start = end

    # Join translated chunks into one string
    return " ".join(translated_chunks)


for idx, row in df.iterrows():
    # Get short texts from certain columns (short texts)
    short_cols = [col for col in translate_cols if col not in long_text_cols]
    short_text = " || ".join(
        [str(row[col]) for col in short_cols if not pd.isna(row[col])]
    )
    # Get long texts from long columns (long texts)
    long_cols = [col for col in long_text_cols if not pd.isna(row[col])]
    long_cols_texts = {col: str(row[col]) for col in long_cols}
    
    # Calculate total length of the text
    total_len = len(short_text) + sum(len(text) for text in long_cols_texts.values())

    
    if total_len <= max_len:
        # If total length is less than or equal to max_len(4900), translate all together
        combined_text = " || ".join([str(row[col]) for col in translate_cols if not pd.isna(row[col])])
        translated = translate_text(combined_text)
        translated_parts = translated.split(" || ")
        for col, val in zip(translate_cols, translated_parts):
            df.at[idx, col] = val
    else:
        # Translate the combined short texts, and insert them into the dataframe
        translated_short_text = translate_text(short_text)
        translated_short_parts = translated_short_text.split(" || ")
        for col, val in zip(short_cols, translated_short_parts):
            df.at[idx, col] = val

        # Translate long text columns individually, and insert them into the dataframe
        for col in long_text_cols:
            df.at[idx, col] = translate_text(long_cols_texts[col])
        
    print(f"Translated row {idx+1}/{len(df)}")

# === WRITE CSV ===
df.to_csv(output_file, index=False, encoding="utf-8-sig")
print(f"Translation complete! Saved to {output_file}")

