# A Job Net Scraper with Batch Processing and Auto-Retry Logic
from playwright.async_api import async_playwright
import asyncio
import pandas as pd
from pandas import NA
from datetime import datetime, timedelta
import re
import sys
import os

# --- Logging for terminal ---
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

def setup_logging(is_rescraping, portal, keyword, retry_attempt=0):
    os.makedirs("data", exist_ok=True)
    if is_rescraping:
        log_file_name = f"terminal_output_jobnet_{portal}_{keyword}_rescraped.txt"
    elif retry_attempt > 0:
        log_file_name = f"terminal_output_jobnet_{portal}_{keyword}_retry_{retry_attempt}.txt"
    else:
        log_file_name = f"terminal_output_jobnet_{portal}_{keyword}.txt"
    log_file = open(f"data/{log_file_name}", "w", encoding="utf-8")
    sys.stdout = sys.stderr = Tee(sys.stdout, log_file)
    return log_file

CONCURRENCY_LIMIT = 10
BATCH_LIMIT = 3000

async def parse_text_content(page, selector):
    locator = page.locator(selector).first
    if await locator.count() > 0:
        return (await locator.text_content()).strip()
    else:
        return NA

async def parse_date_posted(page):
    date_posted_text = await parse_text_content(
        page, 
        "//div[@class='job-details__card-footer']/div/div[2]/div[1]/i[@class='fa fa-calendar-check-o']/following-sibling::span"
    )
    if not pd.isna(date_posted_text):
        date_posted_text = date_posted_text.lower()
        try:
            date_posted_object = datetime.strptime(date_posted_text, "%d %b %Y")
            return date_posted_object.strftime("%Y-%m-%d")
        except ValueError:
            pass 
        match = re.search(r"(\d+)\s*(second|minutes|day|week|month|year)", date_posted_text)
        if match:
            num = int(match.group(1))
            unit = match.group(2)
            match unit:
                case "second":
                    delta = timedelta(seconds = num)
                case "minute":
                    delta = timedelta(minutes = num)
                case "hour":
                    delta = timedelta(hours = num)
                case "day":
                    delta = timedelta(days = num)
                case "week":
                    delta = timedelta(weeks = num)
                case "month":
                    delta = timedelta(days = 30 * num)
                case "year":
                    delta = timedelta(days = 365 * num)
            date_posted_object = datetime.today() - delta
            return date_posted_object.strftime("%Y-%m-%d")
        if "yesterday" in date_posted_text:
            return (datetime.today() - timedelta(days = 1)).strftime("%Y-%m-%d")
        elif "today" in date_posted_text:
            return
        return date_posted_text
    else:
        return NA

async def parse_salary(page, selector, currency_values, currency_dictionary):
    salary_text = (await parse_text_content(page, selector)).lower()
    print(salary_text)
    if salary_text not in ["negotiable", "competitive"]:
        salary_source = "direct_data"
        numbers = re.findall(r"\d+(?:,\d+)*(?:\.\d+)?\s*(?:million|mil|m|k|thousand|thousands)?\b", salary_text, re.IGNORECASE)
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
        print(min_amount, max_amount)
        currency = NA
        for c in currency_values:
            if re.search(c.lower(), salary_text):
                currency = currency_values[0]
                break
        if "currency" not in locals():
            for c in currency_dictionary:
                if c.lower() in salary_text:
                    currency = currency_dictionary[c]
                    break
        if "currency" not in locals():
            currency = NA
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
        print(f"Salary: {salary_text}")
        print(f"Min Amount: {min_amount}")
        print(f"Max Amount: {max_amount}")
        print(f"Currency: {currency}")
        print(f"Interval: {interval}")
    else:
        return NA, NA, NA, NA, NA
    return salary_source, interval, min_amount, max_amount, currency

async def parse_company_info(page, portal):
    company = await parse_text_content(
        page, 
        "//div[@class='job-details__card-header']/div[1]/a[@class='job-details__card-subtitle ClickTrack-EmpProfile']"
    )
    if not pd.isna(company):
        partial_company_url = await page.locator(
            "//div[@class='job-details__card-header']/div[1]/a[@class='job-details__card-subtitle ClickTrack-EmpProfile']"
        ).first.get_attribute("href")
        company_url = f"https://www.jobnet.com.{portal}{partial_company_url}"
        await page.goto(company_url, wait_until="domcontentloaded", timeout=40000)
        print(f"Company URL: {company_url}")
        company_logo_src = page.locator(
            "//div[@class='career-main__box-left']/img"
        )
        company_logo = await company_logo_src.get_attribute("src") if await company_logo_src.count() > 0 else NA
        company_addresses = await parse_text_content(
            page, 
            "//p[contains(., 'Address')]/parent::div/following-sibling::div[@class='career-details__block-desc']/span"
        )
        company_industry = await parse_text_content(
            page, 
            "//div[@class='career-details__block-desc']/ul/li/span[contains(., 'Industry')]/following-sibling::span"
        )
        company_num_emp = await parse_text_content(
            page, 
            "//div[@class='career-details__block-desc']/ul/li/span[contains(., 'No. Employees:')]/following-sibling::span"
        )
        company_description = await parse_text_content(
            page, 
            "//p[contains(., 'What we do')]/parent::div/following-sibling::span[@class='career-details__block-desc']"
        )
    else:
        return NA, NA, NA, NA, NA, NA, NA
    return company, company_industry, company_url, company_logo, company_addresses,  company_num_emp, company_description

def save_error_links(error_links, portal, keyword, retry_attempt=0):
    if not error_links:
        return None
    os.makedirs("data", exist_ok=True)
    if retry_attempt > 0:
        filename = f"data/jobnet_{portal}_{keyword}_retry_{retry_attempt}_errors.csv"
    else:
        filename = f"data/jobnet_{portal}_{keyword}_errors.csv"
    error_df = pd.DataFrame(error_links, columns=['job_link'])
    error_df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"Error links saved to: {filename}")
    return filename

def save_job_data(job_data, portal, keyword, retry_attempt=0, is_rescraping=False):
    if not job_data:
        print("No job data to save")
        return None
    data_frame = pd.DataFrame(job_data)
    if is_rescraping:
        filename = f"data/jobnet_{portal}_{keyword}_rescraped.csv"
    elif retry_attempt > 0:
        filename = f"data/jobnet_{portal}_{keyword}_retry_{retry_attempt}.csv"
    else:
        filename = f"data/jobnet_{portal}_{keyword}.csv"
    data_frame.to_csv(
        filename,
        index=False,
        quotechar='"',
        escapechar='\\',
        encoding='utf-8-sig'
    )
    print(f"Data saved to: {filename} ({len(job_data)} records)")
    return filename

async def login(page, portal):
    # Navigate to the login page
    await page.goto(f"https://www.jobnet.com.{portal}/login")
    # Login Values -- you should replace with your real credentials
    email = "dummydmy123@gmail.com"
    password = "dummypassword123"
    # Fill in the login form
    await page.fill("input[id='BodyPlaceHolder_txtEmail']", email)
    await page.fill("input[id='BodyPlaceHolder_txtLoginPassword']", password)
    # Submit the login form
    await page.press("input[id='BodyPlaceHolder_txtLoginPassword']", "Enter")
    # Wait for the login to complete
    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_selector("p[class='profile__main-name']", timeout=15000)

async def extract_job_links(page, portal, keyword, max_pages):
    job_links = []
    seen_links = set()
    current_page = 1
    await page.goto(f"https://www.jobnet.com.{portal}/jobs?kw={keyword}")
    print(f"Extracting links from https://www.jobnet.com.{portal}/jobs?kw={keyword}")
    while current_page <= max_pages:
        await page.wait_for_selector("a.search__job-title.ClickTrack-JobDetail")
        job_item_links = page.locator("a.search__job-title.ClickTrack-JobDetail")
        link_count = await job_item_links.count()
        if link_count == 0:
            print(f"No job links found on jobnet.com.{portal} for the keyword of \"{keyword}\"")
            break
        else:
            page_job_links = []
            for link in range(link_count):
                job_href = await job_item_links.nth(link).get_attribute('href')
                if job_href and job_href not in seen_links:
                    job_links.append(job_href)
                    seen_links.add(job_href)
                    page_job_links.append(job_href)
            print(f"Found {len(page_job_links)} job links on page {current_page} of {max_pages}")
            # Locate the next page button
            next_button = page.locator("//div[@class='search__action-wrapper-left']/a[last()]")
            if await next_button.count() > 0:
                is_button_disabled = True if await next_button.get_attribute("disabled") else False
                if is_button_disabled:
                    break
                else:
                    await next_button.hover()
                    await next_button.click(force=True)
                    current_page += 1
            else:
                break
    print(f"\nTotal Job Links collected: {len(job_links)} from {current_page} pages of {max_pages} pages")
    print(f"Unique job links: {len(set(job_links))} from {current_page} pages of {max_pages} pages")
    return job_links

async def scrape_single_job(page, link, portal, currency_values, currency_dictionary):
    try:
        job_id = link.rsplit("/", 1)[1]
        job_url = f"https://www.jobnet.com.{portal}{link}"
        print(f"Job URL: {job_url}")
        await page.goto(job_url, timeout=30000)
        title = await parse_text_content(
            page,
            "//div[@class='job-details__card-header']/div[1]/p[@class='job-details__card-title']"
        )
        location = await parse_text_content(
            page,
            "//div[@class='job-details__card-footer']/div/div[1]/i[@class='icon-font icon-cursor']/following-sibling::span"
        )
        date_posted = await parse_date_posted(page)
        job_type = await parse_text_content(
            page,
            "//div[@class='job-details__showing']/div[2]/p[contains(., 'Job Type')]/following-sibling::span"
        )
        salary_source, interval, min_amount, max_amount, currency = await parse_salary(
            page,
            "//a[@class='job-details__card-login salary-no-link']/span",
            currency_values,
            currency_dictionary
        )
        job_level = await parse_text_content(page,
            "//div[@class='job-details__showing']/div[2]/p[contains(., 'Experience level')]/following-sibling::span"
        )
        job_function = await parse_text_content(page,
            "//div[@class='job-details__showing']/div[2]/p[contains(., 'Job Function')]/following-sibling::span"
        )
        description = await parse_text_content(
            page,
            "//p[contains(., 'Job Description') and @class='job-details__description-title']/parent::div"
        )
        requirement = await parse_text_content(
            page,
            "//p[contains(., 'Job Requirements') and @class='job-details__description-title']/parent::div"
        )
        company, company_industry, company_url, company_logo, company_addresses, company_num_emp, company_description = await parse_company_info(
            page,
            portal
        )
        return {
            "id": job_id,
            "site": "jobnet",
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
            "listing_type": NA,
            "emails": NA,
            "description": description,
            "requirement": requirement,
            "company_industry": company_industry,
            "company_url": company_url,
            "company_logo": company_logo,
            "company_addresses": company_addresses,
            "company_num_emp": company_num_emp,
            "company_description": company_description
        }
    except Exception as e:
        print(f"Error scraping {link}: {e}")
        return None

async def process_job_links(job_links, portal, currency_values, currency_dictionary, retry_attempt=0):
    job_data = []
    error_links = []
    total_processed = 0
    async with async_playwright() as pw:
        async def start_browser():
            browser = await pw.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()
            await login(page, portal)
            return browser, context, page
        browser, context, page = await start_browser()
        while total_processed < len(job_links):
            batch_links = job_links[total_processed: total_processed + BATCH_LIMIT]
            print(f"\n--- Processing batch: {len(batch_links)} jobs ---")
            sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
            async def bound_scrape(link):
                async with sem:
                    job_page = await context.new_page()
                    data = await scrape_single_job(job_page, link, portal, currency_values, currency_dictionary)
                    await job_page.close()
                    if data:
                        job_data.append(data)
                    else:
                        error_links.append(link)
            await asyncio.gather(*(bound_scrape(link) for link in batch_links))
            total_processed += len(batch_links)
            await browser.close()
            if total_processed < len(job_links):
                print(f"\n--- Restarting browser after {total_processed} jobs ---")
                browser, context, page = await start_browser()
        await browser.close()
    return job_data, error_links

async def web_scraper(is_rescraping, link_file_name, portal="mm", keyword="data+analyst", max_pages=2, max_retries=2):
    print("Initiating Job Net Scraper with Batch/Retry Logic")
    print(f"Parameters: {portal} {keyword} {max_pages}")

    currency_country_dictionary = {
        "kh": ["KHR", "₭"],
        "mm": ["MMK", "Ks", "Ḵ"],
    }
    currency_values = currency_country_dictionary[portal]
    currency_dictionary = {
        "IDR": "IDR", "MYR": "MYR", "PHP": "PHP", "THB": "THB", 
        "USD": "USD", "SGD": "SGD", "VND": "VND", "KHR": "KHR", "MMK": "MMK",
        "Rp": "IDR", "RM": "MYR", "₱": "PHP", "฿": "THB", "$": "SGD", "S$": "SGD", 
        "₫": "VND", "Ks": "MMK", "Ḵ": "MMK", "₭": "KHR"
    }

    if is_rescraping:
        job_links_df = pd.read_csv(f"data/{link_file_name}", header=None)
        job_links = job_links_df[0].tolist()
        print(f"Loaded {len(job_links)} links from file for rescraping")
    else:
        # Login once and extract job links in a single context to stay authenticated
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()
            await login(page, portal)
            job_links = await extract_job_links(page, portal, keyword, max_pages)
            await browser.close()
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
            current_links, portal, currency_values, currency_dictionary, retry_attempt
        )
        all_job_data.extend(job_data)
        if job_data:
            save_job_data(job_data, portal, keyword, retry_attempt, is_rescraping)
        if error_links:
            print(f"\nFound {len(error_links)} failed links")
            error_file = save_error_links(error_links, portal, keyword, retry_attempt)
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
            final_filename = f"data/jobnet_{portal}_{keyword}_rescraped_final.csv"
        else:
            final_filename = f"data/jobnet_{portal}_{keyword}_final.csv"
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
    print("| = | = | = | Job Net Scraper with Batch/Retry | = | = | = |")
    print("List of available Portal from JobNet")
    print("mm = Myanmar")
    print("kh = Cambodia")
    rescrape_input = input("\nAre you here to rescrape? (Y/N): ").lower().strip()
    is_rescraping = True if rescrape_input == "y" else False
    if is_rescraping:
        print("Note: The filename format is jobnet_{portal}_{keyword}_errors.csv")
        link_file_name = input("File name the CSV file from the data folder: ").strip()
        parts = link_file_name.replace("jobnet_", "").replace("_errors.csv", "").replace("_retry_", "_").split("_")
        portal = parts[0]
        keyword = "_".join(parts[1:])
        max_pages = 0
        max_retries = int(input("Maximum retry attempts (default 2): ") or "2")
    else:
        portal = input("Choose a JobNet Portal: ").lower().strip()
        keyword = input("Keyword (e.g., data analyst): ").strip().replace(" ", "+")
        max_pages = int(input("Maximum pages to scrape (default 50): ") or "50")
        max_retries = int(input("Maximum retry attempts for failed links (default 2): ") or "2")
        link_file_name = ""
    log_file = setup_logging(is_rescraping, portal, keyword)
    asyncio.run(web_scraper(is_rescraping, link_file_name, portal, keyword, max_pages, max_retries))