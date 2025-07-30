# Libraly
from transformers import AutoTokenizer, AutoModelForTokenClassification, pipeline
from pprint import pprint

# Load the pre-trained model and tokenizer
model_name = "Nucha/Nucha_SkillNER_BERT"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForTokenClassification.from_pretrained(model_name)

# Create a NER pipeline
ner_pipeline = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="simple")
# ner_pipeline = pipeline("ner", model=model, tokenizer=tokenizer)

# Sample text
text = "Knowledge of R, SQL and Python"

# Run the pipeline on the text
ner_results = ner_pipeline(text)

# Display the results
# pprint(ner_results)
for entity in ner_results:
    print(f"Entity: {entity['word']}, Label: {entity['entity_group']}, Score: {entity['score']:.4f}")