# 🕸️ JobStreet & JobsDB Web Scraper

A **Python-based asynchronous web scraper** for extracting job listings from **JobStreet** and **JobsDB** portals using **Playwright**.

> ⚠️ **Note:** This script is still under development. Expect messy console output and various print statements used for debugging and issue tracking.

---

## ⚙️ Setup Instructions (Windows, Command Prompt)

1. **Clone the Repository**

   ```bash
   git clone https://github.com/KMarttt/jobstreet-scrapper.git
   cd jobstreet-scrapper
   ```

2. **Create and Activate a Virtual Environment**

   ```bash
   python -m venv .venv
   .venv\Scripts\activate
   ```

3. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Install Playwright Browser**
   ```bash
   playwright install
   ```

---

## 🚀 Usage

Run the scraper:

```bash
python job_street_scraper.py
```

You will be prompted for the following inputs:

- **Portal** — Choose one of: `id`, `my`, `ph`, `sg`, or `th`
- **Location** — Enter a city or leave blank to scrape all locations
- **Job Position** — Specify the job title or keyword
- **Number of Pages** — Define how many result pages to scrape

---

## 🧩 Additional Notes

- **Install Python Requirements**  
  Ensure all dependencies are installed via `requirements.txt` before running any scripts.

- **Skill Filters (Optional)**  
  To use the `tech_stack_analyser` and `skills_extractor` features:

  1. Install **Ollama**
  2. Pull and install the **Llama 3.1:8B** LLM model

- **Script Configuration**  
  For many scripts, you may need to modify input files or directories.  
  These are typically defined in the **main function** (found at the bottom of the script).

- **Web Scraping Tips**  
  For best performance and compatibility, use **Google Chrome** as your default browser during scraping.

---

## 🧱 Future Improvements

- Clean and structure console logs
- Add better exception handling and retry logic
- Implement structured output (CSV/JSON export)
- Add support for more filters (salary range, company, etc.)

### 💡 Author

Developed by **KMarttt** and **Renato Tan**

---

> Made with ❤️ using Python & Playwright
