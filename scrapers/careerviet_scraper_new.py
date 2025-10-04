# A Career Viet Scraper with Batch Processing and Auto-Retry Logic
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
        log_file_name = f"terminal_output_careerviet_vn_{keyword}_rescraped.txt"
    elif retry_attempt > 0:
        log_file_name = f"terminal_output_careerviet_vn_{keyword}_retry_{retry_attempt}.txt"
    else:
        log_file_name = f"terminal_output_careerviet_vn_{keyword}.txt"
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
    locator = page.locator(selector).first
    if await locator.count() > 0:
        return (await locator.text_content()).strip()
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

async def parse_salary(page, selector, currency_values):
    salary_text = (await parse_text_content(page, selector))
    print(salary_text)
    if not pd.isna(salary_text):
        salary_text_lower = salary_text.strip().lower()
        if salary_text_lower not in ["negotiable", "competitive"]:
            salary_source = "direct_data"
            numbers = re.findall(r"\d+(?:,\d+)*(?:\.\d+)?\s*(?:million|mil|m|k|thousand|thousands)?\b", salary_text_lower, re.IGNORECASE)
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
            # Assigning min and max salary:
            if len(parsed_numbers) > 1:
                min_amount = min(parsed_numbers)
                max_amount = max(parsed_numbers)
            elif "up to" in salary_text_lower:
                min_amount = 0
                max_amount = parsed_numbers[0]
            elif "starting from" in salary_text_lower:
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
                if c.lower() in salary_text_lower:
                    currency = currency_values[c]
                    break
            if "year" in salary_text_lower:
                interval = "yearly"
            elif "month" in salary_text_lower:
                interval = "monthly"
            elif "week" in salary_text_lower:
                interval = "weekly"
            elif "day" in salary_text_lower:
                interval = "daily"
            elif "hour" in salary_text_lower:
                interval = "hourly"
            else:
                interval = NA
            print(f"Salary: {salary_text_lower}")
            print(f"Min Amount: {min_amount}")
            print(f"Max Amount: {max_amount}")
            print(f"Currency: {currency}")
            print(f"Interval: {interval}")
            return salary_source, interval, min_amount, max_amount, currency
    return NA, NA, NA, NA, NA

async def parse_job_function(page, selector):
    job_function_raw_text = await parse_text_content(page, selector)
    if not pd.isna(job_function_raw_text):
        job_function_text = re.sub(r'\s+', ' ', job_function_raw_text)
        job_function = [item.strip() for item in job_function_text.split(",") if item.strip()]
        print(f"Job Function: {job_function}")
        return job_function
    else:
        return NA

async def parse_year_of_experience(page, selector):
    year_of_experience_raw = await parse_text_content(page, selector)
    if not pd.isna(year_of_experience_raw):
        year_of_experience = re.sub(r"\s+", " ", year_of_experience_raw).lstrip(", ")
        return year_of_experience
    else:
        return NA

async def parse_skill(page, selector):
    skill_li =  page.locator(selector)
    skill_count = await skill_li.count()
    if skill_count > 0:
        skills = []
        for i in range(skill_count):
            skill_raw = (await skill_li.nth(i).text_content()).strip()
            skill = re.sub(r"\s+", " ", skill_raw).lstrip(", ")
            skills.append(skill)
        return skills
    else:
        return NA

async def parse_company_info(page, job_template_type):
    print(f"Job Template Type: {job_template_type}")
    match job_template_type:
        case "A":
            company = await parse_text_content(page, "//div[@class='apply-now-content']/div[1]/a")
        case "B":
            company = await parse_text_content(page, "//div[@class='title']/following-sibling::a[@class='company']")
    if not pd.isna(company):
        match job_template_type:
            case "A":
                company_url = await page.locator("//div[@class='apply-now-content']/div[1]/a").get_attribute("href")
            case "B":
                company_url = await page.locator("//div[@class='title']/following-sibling::a[@class='company']").get_attribute("href")
        await page.goto(company_url, wait_until="domcontentloaded", timeout=40000)
        print(f"Company URL: {company_url}")
        if await page.locator("header.header-premium").count() > 0:
            company_template_type = "C"
        elif await page.locator("div.section-page.cp_basic_info").count() > 0:
            company_template_type = "B"
        else:
            company_template_type = "A"
        print(f"Company Template Type: {company_template_type}")
        match company_template_type:
            case "A":
                load_more_button_locator = page.locator(
                    "//h2[contains(., 'About Us')]/following-sibling::div[@class='box-text more-less']/div[@class='view-style']/a[@class='read-more']"
                )
                while await load_more_button_locator.is_visible():
                    await load_more_button_locator.click()
                    await page.wait_for_timeout(500)
                company_logo_src = page.locator(
                    "//div[@class='company-info']/div/div[@class='img']/img"
                )
                company_logo = await company_logo_src.get_attribute("src") if await company_logo_src.count() > 0 else NA
                company_url_direct_text = await parse_text_content(
                    page,
                    "//strong[contains(., 'Company Information')]/following-sibling::ul/li[span[@class='mdi mdi-link']]"
                )
                company_url_direct = company_url_direct_text.split("Website:")[1].strip() if not pd.isna(company_url_direct_text) and "Website:" in company_url_direct_text else NA
                company_addresses = await parse_text_content(
                    page,
                    "//div[@class='content']/strong[contains(., 'Location')]/following-sibling::p"
                )
                company_num_emp_text = await parse_text_content(
                    page,
                    "//strong[contains(., 'Company Information')]/following-sibling::ul/li[span[@class='mdi mdi-account-supervisor']]"
                )
                company_num_emp = company_num_emp_text.split("Company size:")[1].strip() if not pd.isna(company_num_emp_text) and "Company size:" in company_num_emp_text else NA
                company_description = await parse_text_content(
                    page,
                    "//h2[contains(., 'About us')]/following-sibling::div[contains(@class, 'box-text')]/div[@class='main-text']"
                )
            case "B":
                company_logo_src = page.locator(
                    "//span[@class='logoJobs']/table/tbody/tr/td/a/img"
                )
                company_logo = await company_logo_src.get_attribute("src") if await company_logo_src.count() > 0 else NA
                company_url_direct_text = await parse_text_content(
                    page,
                    "//h2[@id='cp_company_name']/following-sibling::ul/li[span[contains(text(),'Website:')]]/span[contains(text(),'Website:')]"
                )
                company_url_direct = company_url_direct_text.split("Website:")[1].strip() if not pd.isna(company_url_direct_text) and "Website:" in company_url_direct_text else NA
                company_addresses = await parse_text_content(
                    page,
                    "//h2[@id='cp_company_name']/following-sibling::ul/li[1]"
                )
                company_num_emp_text = await parse_text_content(
                    page,
                    "//h2[@id='cp_company_name']/following-sibling::ul/li[span[contains(text(),'Company size:')]]/span[contains(text(),'Company size:')]"
                )
                company_num_emp = company_num_emp_text.split("Company size:")[1].strip() if not pd.isna(company_num_emp_text) and "Company size:" in company_num_emp_text else NA
                company_description = await parse_text_content(
                    page,
                    "//h2[contains(@class,'section-title') and contains(., 'About us')]/parent::header/following-sibling::div[@class='container']"
                )
            case "C":
                company_logo_src = page.locator(
                    "//div[@class='profile-intro-wrap']/div[@class='img']/img"
                )
                company_logo = await company_logo_src.get_attribute("src") if await company_logo_src.count() > 0 else NA
                company_url_direct_text = await parse_text_content(
                    page,
                    "//strong[contains(., 'Information')]/following-sibling::ul/li[span[@class='mdi mdi-link']]"
                )
                company_url_direct = company_url_direct_text.split("Website:")[1].strip() if not pd.isna(company_url_direct_text) and "Website:" in company_url_direct_text else NA
                company_addresses_text = await parse_text_content(
                    page,
                    "//p[@class='company-location']"
                )
                company_addresses = company_addresses_text.split("Location")[1].strip() if not pd.isna(company_addresses_text) and "Location" in company_addresses_text else NA
                company_num_emp_text = await parse_text_content(
                    page,
                    "//strong[contains(., 'Information')]/following-sibling::ul/li[span[@class='mdi mdi-account']]"
                )
                company_num_emp = company_num_emp_text.split("Company size:")[1].strip() if not pd.isna(company_num_emp_text) and "Company size:" in company_num_emp_text else NA
                company_description = await parse_text_content(
                    page,
                    "//h2[contains(., 'About us')]/parent::div[@class='cb-title']/h2"
                )
        return company, company_url, company_logo, company_url_direct, company_addresses, company_num_emp, company_description
    else:
        return NA, NA, NA, NA, NA, NA, NA

# --- Batch/Retry/Save Error Links Functions ---

def save_error_links(error_links, keyword, retry_attempt=0):
    if not error_links:
        return None
    os.makedirs("data", exist_ok=True)
    if retry_attempt > 0:
        filename = f"data/careerviet_vn_{keyword}_retry_{retry_attempt}_errors.csv"
    else:
        filename = f"data/careerviet_vn_{keyword}_errors.csv"
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
        filename = f"data/careerviet_vn_{keyword}_rescraped.csv"
    elif retry_attempt > 0:
        filename = f"data/careerviet_vn_{keyword}_retry_{retry_attempt}.csv"
    else:
        filename = f"data/careerviet_vn_{keyword}.csv"
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
        job_id = link.split(".html")[0].rsplit(".", 1)[1]
        job_url = link
        await page.goto(job_url, wait_until="domcontentloaded")
        print(f"Scraping job: {job_url}")
        job_template_type = "A" if await page.locator("div.apply-now-content").count() > 0 else "B"
        match job_template_type:
            case "A":
                title = await parse_text_content(page, "//div[@class='apply-now-content']/div[1]/div[1]")
                location = await parse_text_content(page, "//strong[contains(., 'Location')]/following-sibling::p")
                date_posted = await parse_date_posted(page, "//strong[contains(., 'Updated')]/following-sibling::p")
                job_type = await parse_text_content(page, "//strong[contains(., 'Job type')]/following-sibling::p")
                salary_source, interval, min_amount, max_amount, currency  = await parse_salary(page, "//strong[contains(., 'Salary')]/following-sibling::p", currency_values)
                job_level = await parse_text_content(page, "//strong[contains(., 'Job level')]/following-sibling::p")
                job_function = await parse_job_function(page, "//strong[contains(., 'Industry')]/following-sibling::p")
                year_of_experience = await parse_year_of_experience(page,"//strong[contains(., 'Experience')]/following-sibling::p")
                skill = await parse_skill(page, "//h2[contains(., 'Job tags / skills')]/following-sibling::ul/li")
                description = await parse_text_content(page, "//h2[contains(., 'Job Description')]/parent::div[@class='detail-row reset-bullet']")
                requirement = await parse_text_content(page, "//h2[contains(., 'Job Requirement')]/parent::div[@class='detail-row reset-bullet']")
                company, company_url, company_logo, company_url_direct, company_addresses, company_num_emp, company_description = await parse_company_info(page, job_template_type)
            case "B":
                title = await parse_text_content(page, "//a[@class='company']/preceding-sibling::div[@class='title']")
                location = await parse_text_content(page, "//h3[contains(., 'Work location')]/following-sibling::div/span")
                date_posted = await parse_text_content(page, "//p[contains(., 'Updated')]/parent::td/following-sibling::td")
                job_type = await parse_text_content(page, "//p[contains(., 'Job type')]/parent::td/following-sibling::td")
                salary_source, interval, min_amount, max_amount, currency  = await parse_salary(page, "//p[contains(., 'Salary')]/parent::td/following-sibling::td", currency_values)
                job_level = await parse_text_content(page, "//p[contains(., 'Job level')]/parent::td/following-sibling::td")
                job_function = await parse_job_function(page, "//p[contains(., 'Industry')]/parent::td/following-sibling::td")
                year_of_experience = await parse_year_of_experience(page, "//p[contains(., 'Experience')]/parent::td/following-sibling::td")
                skill = await parse_skill(page, "//h3[contains(., 'JOB TAGS / SKILLS:') and @class='detail-title']/following-sibling::ul/li")
                description = await parse_text_content(page, "//h3[contains(., 'Job Description') and @class='detail-title']/following-sibling::div[@class='content']")
                requirement = await parse_text_content(page, "//h3[contains(., 'Job Requirement') and @class='detail-title']/following-sibling::div[@class='content']")
                company, company_url, company_logo, company_url_direct, company_addresses, company_num_emp, company_description = await parse_company_info(page, job_template_type)
        return {
            "id": job_id,
            "site": "careerviet",
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
            "skill": skill,
            "listing_type": NA,
            "emails": NA,
            "description": description,
            "requirement": requirement,
            "company_industry": NA,
            "company_url": company_url,
            "company_logo": company_logo,
            "company_url_direct": company_url_direct,
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

async def web_scraper(is_rescraping, link_file_name, keyword="data-mining", max_pages=50, max_retries=2):
    print("Initiating Career Viet Scraper with Batch/Retry Logic")
    print(f"{keyword} {max_pages}")
    os.makedirs("data", exist_ok=True)
    currency_values = {
        "đ": "VND",
        "₫": "VND",
        "vnd": "VND",
        "$": "USD",
        "usd": "USD"
    }
    if is_rescraping:
        job_links_df = pd.read_csv(f"data/{link_file_name}", header=None)
        job_links = job_links_df[0].tolist()
        print(f"Loaded {len(job_links)} links from file for re-scraping")
    else:
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
                url = f"https://careerviet.vn/jobs/{keyword}-k-page-{current_page}-en.html"
                print(f"Scraping page {current_page}: {url}")
                try:
                    await page.goto(url, timeout=30000)
                    await scroll_to_bottom(page, pause=2)
                    job_item_links = page.locator("//div[@class='figcaption']/div[contains(@class, 'title')]/h2/a[@class='job_link']")
                    link_count = await job_item_links.count()
                    if link_count == 0:
                        consecutive_empty_pages += 1
                        print(f"No job links found on page {current_page}. Empty pages count: {consecutive_empty_pages}")
                        no_result_indicator = page.locator("//div[@class='no-search']/div[@class='image']/figure/figcaption")
                        if await no_result_indicator.count() > 0:
                            consecutive_empty_pages += 1
                            print(f"End of results detected on page {current_page}")
                            print("Reached end of job listings")
                            break
                    else:
                        consecutive_empty_pages = 0
                        page_job_links = []
                        for link_index in range(link_count):
                            job_link = await job_item_links.nth(link_index).get_attribute("href")
                            
                            # Ensure all job links are in English
                            if "/vi/" in job_link and 'tim-viec-lam/' in job_link:
                                job_link = job_link.replace("/vi/", "/en/")
                                job_link = job_link.replace("tim-viec-lam/", "search-job/")
                            
                            if job_link and job_link not in seen_links:
                                job_links.append(job_link)
                                seen_links.add(job_link)
                                page_job_links.append(job_link)
                        print(f"Found {len(page_job_links)} new job links on page {current_page}")
                except Exception as e:
                    print(e)
                    consecutive_empty_pages += 1
                current_page += 1
            print(f"\nTotal Job Links collected: {len(job_links)} from {current_page - 1} pages")
            print(f"Unique job links: {len(set(job_links))}")
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
            final_filename = f"data/careerviet_vn_{keyword}_rescraped_final.csv"
        else:
            final_filename = f"data/careerviet_vn_{keyword}_final.csv"
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
    print("| = | = | = | Career Viet Web Scraper with Batch/Retry | = | = | = |")
    rescrape_input = input("\nAre you here to rescrape? (Y/N): ").lower().strip()
    is_rescraping = True if rescrape_input == "y" else False
    if is_rescraping:
        print("Note: The filename format is careerviet_vn_{keyword}_errors.csv")
        link_file_name = input("File name the CSV file from the data folder: ").strip()
        keyword = link_file_name.replace("careerviet_vn_", "").replace("_errors.csv", "").replace("_retry_", "_")
        max_pages = 0
        max_retries = int(input("Maximum retry attempts (default 2): ") or "2")
    else:
        keyword = input("Keyword (e.g., data analyst): ").strip().replace(" ", "-")
        max_pages = int(input("Maximum pages to scrape (default 50): ") or "50")
        max_retries = int(input("Maximum retry attempts for failed links (default 2): ") or "2")
        link_file_name = ""
    log_file = setup_logging(is_rescraping, keyword)
    asyncio.run(web_scraper(is_rescraping, link_file_name, keyword, max_pages, max_retries))