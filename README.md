# JobStreet Web Scraper

A Python-based asynchronous web scraper for extracting job listings from JobStreet portals using Playwright.

## Setup Instructions

1. Clone the Repository:

```
git clone https://github.com/KMarttt/jobstreet-scrapper.git
cd jobstreet-scrapper
```

2. Create and active a virtual environment:

```
python -m venv .venv
.venv\Scripts\activate
```

3. Install dependencies:

```
pip install -r requirements.txt
```

4. Install Playwright browser

```
playwright install
```

## Usage

Run the scraper:

```
python job_street_scraper.py
```

You will be prompted for:

-   Portal (e.g., my, ph, sg)
-   Location (city or leave blank for all)
-   Job Position
-   Number of pages to scrape
