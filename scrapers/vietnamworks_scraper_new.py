# A VietnamWorks Scraper with Batch Processing and Auto-Retry Logic
from playwright.async_api import async_playwright
import asyncio
import pandas as pd
from pandas import NA
from datetime import datetime, timedelta
import re
import sys
import os

# -----------------------------
# Logging setup for cmd terminal
class Tee:
    def __init__(self, *files):
        self.files = files
    def write(self, obj):
        for f in self.files:
            f.write(obj)
            f.flush()
    def flush(self):
        for f in self.files:
            f.flush()

def setup_logging(is_rescraping, keyword, retry_attempt=0):
    os.makedirs("data", exist_ok=True)
    if is_rescraping:
        log_file_name = f"terminal_output_vietnamworks_vn_{keyword}_rescraped.txt"
    elif retry_attempt > 0:
        log_file_name = f"terminal_output_vietnamworks_vn_{keyword}_retry_{retry_attempt}.txt"
    else:
        log_file_name = f"terminal_output_vietnamworks_vn_{keyword}.txt"
    log_file = open(f"data/{log_file_name}", "w", encoding="utf-8")
    sys.stdout = sys.stderr = Tee(sys.stdout, log_file)
    return log_file

# -----------------------------

CONCURRENCY_LIMIT = 10
BATCH_LIMIT = 3000

async def scroll_to_bottom(page, pause=1):
    last_height = await page.evaluate("document.body.scrollHeight")
    while True:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(pause)
        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

async def parse_text_content(page, selector):
    locator = page.locator(selector)
    if await locator.count() > 0:
        return (await locator.text_content()).strip()
    else:
        return NA

async def parse_location(page, selector):
    location_div = page.locator(selector)
    location_count = await location_div.count()
    if location_count > 0:
        locations = []
        for i in range(location_count):
            location_text = (await location_div.nth(i).locator("p").text_content()).strip()
            locations.append(location_text)
        return locations
    else:
        return NA

async def parse_date_posted(page, selector):
    date_posted_text = await parse_text_content(page, selector)
    if not pd.isna(date_posted_text):
        date_posted_text = date_posted_text.lower()
        try:
            date_posted_object = datetime.strptime(date_posted_text, "%d %b %Y")
            return date_posted_object.strftime("%Y-%m-%d")
        except ValueError:
            pass
        match = re.search(
            r"(\d+)\s*(second|minute|hour|day|week|month|year)",
            date_posted_text
        )
        if match:
            num = int(match.group(1))
            unit = match.group(2)
            delta = timedelta()
            if unit == "second":
                delta = timedelta(seconds = num)
            elif unit == "minute":
                delta = timedelta(minutes = num)
            elif unit == "hour":
                delta = timedelta(hours = num)
            elif unit == "day":
                delta = timedelta(days = num)
            elif unit == "week":
                delta = timedelta(weeks = num)
            elif unit == "month":
                delta = timedelta(days = 30 * num)
            elif unit == "year":
                delta = timedelta(days = 365 * num)
            date_posted_object = datetime.today() - delta
            return date_posted_object.strftime("%Y-%m-%d")
        if "yesterday" in date_posted_text:
            return (datetime.today() - timedelta(days = 1)).strftime("%Y-%m-%d")
        elif "today" in date_posted_text:
            return datetime.today().strftime("%Y-%m-%d")
        return date_posted_text
    else:
        return NA

async def parse_salary(page, currency_values):
    try:
        salary_text = (await page.locator(
            "//h1[@name='title']/parent::div/parent::div/following-sibling::div[1]/div/span"
        ).text_content()).strip().lower()
    except Exception:
        return NA, NA, NA, NA, NA

    if salary_text != "negotiable" and salary_text != "competitive":
        salary_source = "direct_data"
        numbers = re.findall(
            r"\d+(?:,\d+)*(?:\.\d+)?\s*(?:million|mil|m|k|thousand|thousands)?\b",
            salary_text,
            re.IGNORECASE
        )
        parsed_numbers = []
        for n in numbers:
            n = n.replace(",","").strip()
            if re.search(r"(million|mil|m)\b", n):
                num_part = re.sub(r"(million|mil|m)\b", "", n).strip()
                parsed_numbers.append(int(float(num_part) * 1_000_000))
            elif re.search(r"(k|thousand|thousands)\b", n):
                num_part = re.sub(r"(k|thousand|thousands)\b", "", n).strip()
                parsed_numbers.append(int(float(num_part) * 1_000))
            else:
                parsed_numbers.append(int(float(n)))
        if len(parsed_numbers) > 1:
            min_amount = min(parsed_numbers)
            max_amount = max(parsed_numbers)
        elif "up to" in salary_text:
            min_amount = 0
            max_amount = parsed_numbers[0]
        elif "starting from" in salary_text:
            min_amount = parsed_numbers[0]
            max_amount = 0
        elif len(parsed_numbers) == 1:
            min_amount = parsed_numbers[0]
            max_amount = parsed_numbers[0]
        else:
            min_amount = NA
            max_amount = NA
        currency = NA
        for c in currency_values:
            if c.lower() in salary_text:
                currency = currency_values[c]
                break
        if "year" in salary_text:
            interval = "yearly"
        elif "month" in salary_text:
            interval = "monthly"
        elif "week" in salary_text:
            interval = "weekly"
        elif "day" in salary_text:
            interval = "daily"
        elif "hour" in salary_text:
            interval = "hourly"
        else:
            interval = NA
        return salary_source, interval, min_amount, max_amount, currency
    else:
        return NA, NA, NA, NA, NA

async def parse_other_job_data(page):
    try:
        year_of_experience_text = (await page.locator(
            "//label[contains(., 'YEAR OF EXPERIENCE')]/following-sibling::p[1]"
        ).text_content()).strip()
        year_of_experience = year_of_experience_text if year_of_experience_text != "Not shown" else NA

        education_level_text = (await page.locator(
            "//label[contains(., 'EDUCATION LEVEL')]/following-sibling::p[1]"
        ).text_content()).strip()
        education_level = education_level_text if education_level_text != "Not shown"  else NA

        age_preference_text = (await page.locator(
            "//label[contains(., 'AGE PREFERENCE')]/following-sibling::p[1]"
        ).text_content()).strip()
        age_preference = age_preference_text if age_preference_text != "Not shown" else NA

        skill_text = (await page.locator(
            "//label[contains(., 'SKILL')]/following-sibling::p[1]"
        ).text_content()).strip()
        skill = skill_text if skill_text != "Not shown" or pd.isna(skill_text) else NA

        preferred_language_text = (await page.locator(
            "//label[contains(., 'PREFERRED LANGUAGE')]/following-sibling::p[1]"
        ).text_content()).strip()
        preferred_language = preferred_language_text if preferred_language_text != "Not shown" else NA

        nationality_text = (await page.locator(
            "//label[contains(., 'NATIONALITY')]/following-sibling::p[1]"
        ).text_content()).strip()
        nationality = nationality_text if nationality_text != "Not shown" else NA
    except Exception:
        year_of_experience = education_level = age_preference = skill = preferred_language = nationality = NA
    return year_of_experience, education_level, age_preference, skill, preferred_language, nationality

async def parse_company_info(page):
    company_locator = page.locator(
        "//p[contains(., 'Scam detection')]/parent::div/parent::div/preceding-sibling::div[1]/div[2]/a")
    if await company_locator.count() > 0:
        company = (await company_locator.text_content()).strip()
        company_logo = "https://www.vietnamworks.com" + (await page.locator(
            "//p[contains(., 'Scam detection')]/parent::div/parent::div/preceding-sibling::div[1]/div[1]/div[2]/span/img"
        ).get_attribute("src"))
        company_url = await page.locator(
            "//p[contains(., 'Scam detection')]/parent::div/parent::div/preceding-sibling::div[1]/div[2]/a"
        ).get_attribute("href")
        await page.goto(company_url)
        read_more_button = page.locator(
            "//h2[contains(., 'About Us')]/following-sibling::div[1]/div/span[contains(., 'Read more')]")
        while await read_more_button.count() > 0:
            await read_more_button.click(force=True)
            await page.wait_for_timeout(500)
        company_industry = await parse_text_content(
            page,
            "//p[contains(@class, 'type') and contains(., 'Industry')]/following-sibling::p[1]"
        )
        company_addresses = await parse_text_content(
            page,
            "//p[contains(@class, 'type') and contains(., 'Address')]/following-sibling::div/div"
        )
        company_num_emp = await parse_text_content(
            page,
            "//p[contains(@class, 'type') and contains(., 'Size')]/following-sibling::p[1]"
        )
        company_description = await parse_text_content(
            page,
            "//h2[contains(., 'About Us')]/following-sibling::div[1]/div/p"
        )
    else:
        return NA, NA, NA, NA, NA, NA, NA
    return company, company_industry, company_url, company_logo, company_addresses, company_num_emp, company_description

def save_error_links(error_links, keyword, retry_attempt=0):
    if not error_links:
        return None
    os.makedirs("data", exist_ok=True)
    if retry_attempt > 0:
        filename = f"data/vietnamworks_vn_{keyword}_retry_{retry_attempt}_errors.csv"
    else:
        filename = f"data/vietnamworks_vn_{keyword}_errors.csv"
    error_df = pd.DataFrame(error_links, columns=['job_link'])
    error_df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"Error links saved to: {filename}")
    return filename

def save_job_data(job_data, keyword, retry_attempt=0, is_rescraping=False):
    if not job_data:
        print("No job data to save")
        return None
    data_frame = pd.DataFrame(job_data)
    if is_rescraping:
        filename = f"data/vietnamworks_vn_{keyword}_rescraped.csv"
    elif retry_attempt > 0:
        filename = f"data/vietnamworks_vn_{keyword}_retry_{retry_attempt}.csv"
    else:
        filename = f"data/vietnamworks_vn_{keyword}.csv"
    data_frame.to_csv(
        filename,
        index=False,
        quotechar='"',
        escapechar='\\',
        encoding='utf-8-sig'
    )
    print(f"Data saved to: {filename} ({len(job_data)} records)")
    return filename

async def scrape_single_job(page, link, currency_values):
    try:
        job_id = re.search(r"-(\d+)-jd", link).group(1)
        job_url = f"https://www.vietnamworks.com/{link}"
        print(job_url)
        await page.goto(job_url, wait_until="domcontentloaded")
        
        # Ensure page is fully loaded
        await scroll_to_bottom(page, pause=2)
        await page.wait_for_selector("//h2[contains(., 'Job Information')]/following-sibling::div[last()]/div[1]//button[contains(., 'View more')]")

        site_buttons = [
            page.locator("//h2[contains(., 'Job Information')]/following-sibling::div[last()]/div[1]//button[contains(., 'View more')]"),
            page.locator("//button[contains(., 'View full job description')]")
        ]
        for button in site_buttons:
            while await button.is_visible():
                await button.hover()
                await button.click(force=True)
                await page.wait_for_timeout(500)

        title = await parse_text_content(page, "h1[name='title']")
        location = await parse_location(page, "//h2[contains(., 'Job Locations')]/following-sibling::div[1]/div")

        # Ensure key data are loaded
        await page.wait_for_selector("//label[contains(., 'POSTED DATE')]/following-sibling::p[1]", timeout=15000)
        await page.wait_for_selector("//label[contains(., 'WORKING TYPE')]/following-sibling::p[1]", timeout=15000)

        date_posted = await parse_date_posted(page, "//label[contains(., 'POSTED DATE')]/following-sibling::p[1]")
        job_type = await parse_text_content(page, "//label[contains(., 'WORKING TYPE')]/following-sibling::p[1]")
        salary_source, interval, min_amount, max_amount, currency = await parse_salary(page, currency_values)
        job_level = await parse_text_content(page, "//label[contains(., 'JOB LEVEL')]/following-sibling::p[1]")
        job_function = await parse_text_content(page, "//label[contains(., 'JOB FUNCTION')]/following-sibling::p[1]")
        year_of_experience, education_level, age_preference, skill, preferred_language, nationality = await parse_other_job_data(page)
        description = await parse_text_content(page, "//h2[contains(., 'Job description')]/parent::div")
        requirement = await parse_text_content(page, "//h2[contains(., 'Job requirements')]/parent::div")
        company, company_industry, company_url, company_logo, company_addresses, company_num_emp, company_description = await parse_company_info(page)
        return {
            "id": job_id,
            "site": "vietnamworks",
            "job_url": job_url,
            "job_url_direct": NA,
            "title": title,
            "company": company,
            "location": location,
            "date_posted": date_posted,
            "job_type": job_type,
            "salary_source": salary_source,
            "interval": interval,
            "min_amount": min_amount,
            "max_amount": max_amount,
            "currency": currency,
            "is_remote": NA,
            "work_setup": NA,
            "job_level": job_level,
            "job_function": job_function,
            "year_of_experience": year_of_experience,
            "education_level": education_level,
            "age_preference": age_preference,
            "skill": skill,
            "preferred_language": preferred_language,
            "nationality": nationality,
            "listing_type": NA,
            "emails": NA,
            "description": description,
            "requirement": requirement,
            "company_industry": company_industry,
            "company_url": company_url,
            "company_logo": company_logo,
            "company_url_direct": NA,
            "company_addresses": company_addresses,
            "company_num_emp": company_num_emp,
            "company_description": company_description,
        }
    except Exception as e:
        print(f"Error scraping {link}: {e}")
        return None

async def process_job_links(job_links, currency_values, retry_attempt=0):
    job_data = []
    error_links = []
    total_processed = 0
    async with async_playwright() as pw:
        async def start_browser():
            browser = await pw.chromium.launch(headless=False)
            context = await browser.new_context()
            return browser, context
        browser, context = await start_browser()
        while total_processed < len(job_links):
            batch_links = job_links[total_processed: total_processed + BATCH_LIMIT]
            print(f"\n--- Processing batch: {len(batch_links)} jobs ---")
            sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
            async def bound_scrape(link):
                async with sem:
                    page = await context.new_page()
                    data = await scrape_single_job(page, link, currency_values)
                    await page.close()
                    if data:
                        job_data.append(data)
                    else:
                        error_links.append(link)
            await asyncio.gather(*(bound_scrape(link) for link in batch_links))
            total_processed += len(batch_links)
            await browser.close()
            if total_processed < len(job_links):
                print(f"\n--- Restarting browser after {total_processed} jobs ---")
                browser, context = await start_browser()
        await browser.close()
    return job_data, error_links

async def extract_job_links(keyword, max_pages):
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        job_links = []
        seen_links = set()
        current_page = 1
        consecutive_empty_pages = 0
        max_consecutive_empty = 3
        print("Extracting Job Links")
        while current_page <= max_pages and consecutive_empty_pages < max_consecutive_empty:
            url = f"https://www.vietnamworks.com/jobs?q={keyword}&page={current_page}&sorting=relevant"
            print(f"Scraping page {current_page}: {url}")
            try:
                await page.goto(url, timeout = 30000)
                await scroll_to_bottom(page, pause=2)
                try:
                    await page.wait_for_selector(
                        "a.img_job_card",
                        timeout=10000
                    )
                except Exception:
                    print(f"Timeout waiting for results on page {current_page}")
                    consecutive_empty_pages += 1
                    current_page += 1
                    continue
                card_links = page.locator("a.img_job_card")
                link_count = await card_links.count()
                if link_count == 0:
                    consecutive_empty_pages += 1
                    print(f"No job links found on page {current_page}. Empty pages count: {consecutive_empty_pages}")
                    no_result_indicator = page.locator(
                        "//div[@class='noResultWrapper animated fadeIn']/div/h2[contains(., 'We have not found jobs for this search at the moment')]"
                    )
                    page_reached_end = False
                    if await no_result_indicator.count() > 0:
                        consecutive_empty_pages += 1
                        print(f"End of results detected on page {current_page}")
                        page_reached_end = True
                    if page_reached_end:
                        print("Reached end of job listings")
                        break
                else:
                    consecutive_empty_pages = 0
                    page_job_links = []
                    for link_index in range(link_count):
                        link_href = await card_links.nth(link_index).get_attribute("href")
                        if link_href and link_href not in seen_links:
                            job_links.append(link_href)
                            seen_links.add(link_href)
                            page_job_links.append(link_href)
                    print(f"Found {len(page_job_links)} new job links on page {current_page}")
            except Exception as e:
                print(f"Error scraping page {current_page}: {str(e)}")
                consecutive_empty_pages += 1
            current_page += 1
        print(f"\nTotal Job Links collected: {len(job_links)} from {current_page - 1} pages")
        print(f"Unique Job Links collected: {len(seen_links)}")
        await browser.close()
    return job_links

async def web_scraper(is_rescraping, link_file_name, keyword="data-analyst", max_pages=2, max_retries=2):
    print("Initiating VietnamWorks Scraper with Batch/Retry Logic")
    print(f"{keyword} {max_pages}")
    os.makedirs("data", exist_ok=True)
    currency_values = {
        "đ": "VND",
        "₫": "VND",
        "VND": "VND",
        "$": "USD",
        "USD": "USD"
    }
    if is_rescraping:
        job_links_df = pd.read_csv(f"data/{link_file_name}", header=None)
        job_links = job_links_df[0].tolist()
        print(f"Loaded {len(job_links)} links from file for re-scraping")
    else:
        job_links = await extract_job_links(keyword, max_pages)
        if not job_links:
            print("No job links found. Exiting.")
            return
    all_job_data = []
    current_links = job_links
    retry_attempt = 0
    while current_links and retry_attempt <= max_retries:
        print(f"\n{'='*60}")
        if retry_attempt == 0:
            print("INITIAL SCRAPING ATTEMPT")
        else:
            print(f"RETRY ATTEMPT {retry_attempt}")
        print(f"{'='*60}")
        job_data, error_links = await process_job_links(
            current_links, currency_values, retry_attempt
        )
        all_job_data.extend(job_data)
        if job_data:
            save_job_data(job_data, keyword, retry_attempt, is_rescraping)
        if error_links:
            print(f"\nFound {len(error_links)} failed links")
            error_file = save_error_links(error_links, keyword, retry_attempt)
            if retry_attempt < max_retries:
                print(f"Will retry {len(error_links)} failed links in next attempt...")
                current_links = error_links
                retry_attempt += 1
            else:
                print(f"Max retries ({max_retries}) reached. {len(error_links)} links still failed.")
                break
        else:
            print("No error links found. All jobs processed successfully!")
            break
    if all_job_data:
        print(f"\n{'='*60}")
        print("FINAL CONSOLIDATION")
        print(f"{'='*60}")
        if is_rescraping:
            final_filename = f"data/vietnamworks_vn_{keyword}_rescraped_final.csv"
        else:
            final_filename = f"data/vietnamworks_vn_{keyword}_final.csv"
        final_df = pd.DataFrame(all_job_data)
        final_df.to_csv(
            final_filename,
            index=False,
            quotechar='"',
            escapechar='\\',
            encoding='utf-8-sig'
        )
        print(f"Final consolidated data saved to: {final_filename}")
        print(f"Total successful jobs scraped: {len(all_job_data)}")
        initial_links = len(job_links)
        successful_jobs = len(all_job_data)
        failed_jobs = initial_links - successful_jobs
        success_rate = (successful_jobs / initial_links) * 100 if initial_links > 0 else 0
        print(f"\nSCRAPING SUMMARY:")
        print(f"Initial job links: {initial_links}")
        print(f"Successfully scraped: {successful_jobs}")
        print(f"Failed to scrape: {failed_jobs}")
        print(f"Success rate: {success_rate:.1f}%")
        print(f"Total retry attempts: {retry_attempt}")

if __name__ == "__main__":
    print("| = | = | = | Vietnam Works Web Scraper with Batch/Retry | = | = | = |")
    rescrape_input = input("\nAre you here to rescrape? (Y/N): ").lower().strip()
    is_rescraping = True if rescrape_input == "y" else False
    if is_rescraping:
        print("Note: The filename format is vietnamworks_vn_{keyword}_errors.csv")
        link_file_name = input("File name the CSV file from the data folder: ").strip()
        keyword = link_file_name.replace("vietnamworks_vn_", "").replace("_errors.csv", "").replace("_retry_", "_")
        max_pages = 0
        max_retries = int(input("Maximum retry attempts (default 2): ") or "2")
    else:
        keyword = input("Keyword (e.g., data analyst): ").strip().replace(" ", "-")
        max_pages = int(input("Maximum pages to scrape (default 50): ") or "50")
        max_retries = int(input("Maximum retry attempts for failed links (default 2): ") or "2")
        link_file_name = ""
    log_file = setup_logging(is_rescraping, keyword)
    asyncio.run(web_scraper(is_rescraping, link_file_name, keyword, max_pages, max_retries))