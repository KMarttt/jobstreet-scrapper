# A Job Street Scraper with Auto-Retry Logic
from playwright.async_api import async_playwright
import asyncio
import pandas as pd
from pandas import NA
from datetime import datetime, timedelta
import re
import sys
import os

# -----------------------------


async def clear_browser_data(context, page):
    """Comprehensive browser data clearing function"""
    try:
        # Clear cookies at context level
        await context.clear_cookies()

        # Clear all browser storage using JavaScript
        await page.evaluate("""
            async () => {
                // Clear localStorage
                if (typeof(Storage) !== "undefined" && localStorage) {
                    localStorage.clear();
                }
                
                // Clear sessionStorage
                if (typeof(Storage) !== "undefined" && sessionStorage) {
                    sessionStorage.clear();
                }
                
                // Clear indexedDB
                if (window.indexedDB) {
                    try {
                        const databases = await indexedDB.databases();
                        await Promise.all(
                            databases.map(db => {
                                return new Promise((resolve, reject) => {
                                    const deleteReq = indexedDB.deleteDatabase(db.name);
                                    deleteReq.onsuccess = () => resolve();
                                    deleteReq.onerror = () => reject();
                                });
                            })
                        );
                    } catch (e) {
                        console.log('IndexedDB clearing failed:', e);
                    }
                }
                
                // Clear Cache API if available
                if ('caches' in window) {
                    try {
                        const cacheNames = await caches.keys();
                        await Promise.all(
                            cacheNames.map(cacheName => caches.delete(cacheName))
                        );
                    } catch (e) {
                        console.log('Cache API clearing failed:', e);
                    }
                }
                
                // Clear any service worker data
                if ('serviceWorker' in navigator) {
                    try {
                        const registrations = await navigator.serviceWorker.getRegistrations();
                        await Promise.all(
                            registrations.map(registration => registration.unregister())
                        );
                    } catch (e) {
                        console.log('Service worker clearing failed:', e);
                    }
                }
            }
        """)

        print("Browser data cleared successfully")

    except Exception as e:
        print(f"Warning: Could not clear all browser data: {e}")

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


def setup_logging(is_scraping, site, portal, job_location, keyword, retry_attempt=0):
    if is_scraping:
        log_file_name = f"terminal_output_{site}_{portal}_{job_location}_{keyword}_rescraped.txt"
    elif retry_attempt > 0:
        log_file_name = f"terminal_output_{site}_{portal}_{job_location}_{keyword}_retry_{retry_attempt}.txt"
    else:
        log_file_name = f"terminal_output_{site}_{portal}_{job_location}_{keyword}.txt"

    log_file = open(f"data/{log_file_name}", "w", encoding="utf-8")
    sys.stdout = sys.stderr = Tee(sys.stdout, log_file)
    return log_file

# -----------------------------


async def extract_job_links(page, portal, site, job_location, keyword, max_pages):
    # Extract all job links with automatic page detection

    # Initializing variables
    job_links = []
    seen_links = set()  # To avoid duplicate job links
    current_page = 1
    consecutive_empty_pages = 0
    max_consecutive_empty = 3  # Stop if we encounter 3 consecutive empty pages

    # Extracting job links
    print("Extracting Job Links")
    while current_page <= max_pages and consecutive_empty_pages < max_consecutive_empty:
        loc_param = f"/in-{job_location}" if job_location else ""
        url = f"https://{portal}.{site}.com/{keyword}-jobs{loc_param}?page={current_page}"
        print(f"Scraping page {current_page}: {url}")

        try:
            await page.goto(url, timeout=30000)  # 30 second timeout

            # Wait for the job cards to load
            await page.wait_for_selector("article", timeout=10000)

            # Locator for job card links
            card_links = page.locator(
                "//article[contains(@data-testid, 'job-card')]/div/a[@data-automation='job-list-view-job-link']"
            )

            # Get the count of job links on current page
            link_count = await card_links.count()

            if link_count == 0:
                consecutive_empty_pages += 1
                print(
                    f"No job links found on page {current_page}. Empty pages count: {consecutive_empty_pages}")

                # Check if we've reached the end by looking for "no results" messages
                no_results_indicators = [
                    "text=No jobs found",
                    "text=We couldn't find any jobs",
                    "text=Sorry, no jobs found",
                    "[data-automation='no-search-results']",
                    ".no-results"
                ]

                page_reached_end = False
                for indicator in no_results_indicators:
                    if await page.locator(indicator).count() > 0:
                        print(
                            f"End of results detected on page {current_page}")
                        page_reached_end = True
                        break

                if page_reached_end:
                    print("Reached end of job listings")
                    break

            else:
                consecutive_empty_pages = 0  # Reset counter if we found jobs
                page_job_links = []

                for link_index in range(link_count):
                    link_href = await card_links.nth(link_index).get_attribute('href')
                    if link_href and link_href not in seen_links:
                        job_links.append(link_href)
                        seen_links.add(link_href)
                        page_job_links.append(link_href)

                print(
                    f"Found {len(page_job_links)} new job links on page {current_page}")

        except Exception as e:
            print(f"Error scraping links on page {current_page}: {str(e)}")
            consecutive_empty_pages += 1

        current_page += 1

    print(
        f"\nTotal Job Links collected: {len(job_links)} from {current_page - 1} pages")
    print(f"Unique job links: {len(set(job_links))}")

    return job_links


async def parse_text_content(page, selector):
    # Check if the element exists
    locator = page.locator(selector).first
    if await locator.count() > 0:
        return (await locator.text_content()).strip()
    else:
        return NA


async def parse_date_posted(page, selector):
    date_text = await parse_text_content(page, selector)

    current_date = datetime.now()
    relative_date_text = date_text.replace(
        "Posted", "").replace("ago", "").strip()
    print(f"Relative date text: {relative_date_text!r}")

    if "day" in relative_date_text:  # if the date is in days
        days = int(re.sub(r"[^\d]", "", relative_date_text))
        return (current_date - timedelta(days=days)).strftime(r"%Y-%m-%d")
    elif "hour" in relative_date_text:  # If the date is in hour(s) ago
        return current_date.strftime(r"%Y-%m-%d")
    else:  # If the date is in minute(s) or second(s)
        return current_date.strftime(r"%Y-%m-%d")


async def parse_location(page, selector):
    location_section = await parse_text_content(page, selector)

    if pd.isna(location_section):
        return NA

    if re.search("(hybrid)", location_section.lower()):
        location = location_section.split("(hybrid)")[0].strip()
        work_setup = "hybrid"
        is_remote = False
    elif re.search("(remote)", location_section.lower()):
        location = location_section.split("(remote)")[0].strip()
        work_setup = "remote"
        is_remote = True
    else:
        location = location_section
        work_setup = "onsite"
        is_remote = False

    return location, is_remote, work_setup


async def parse_salary(page, selector, currency_values, currency_dictionary, portal):
    # salary_locator = page.locator(selector)
    salary_text = (await parse_text_content(page, selector))

    # Checks if the salary is present on the page
    if not pd.isna(salary_text):
        salary_text = salary_text.lower()
        salary_source = "direct_data"

        # Select the salary value
        if portal == "id":
            # Designed for Indoensian Portal where the salary separator is a period or a comma -- does not consider decimal
            numbers = re.findall(
                r"(?:\d{1,3}(?:[.,]\d{3})*|\d+)\s*(?:million|mil|m|k|thousand|thousands)?\b",
                salary_text,
                re.IGNORECASE
            )

            # Parse the numbers (conversion & formatting)
            parsed_numbers = []
            for n in numbers:
                n = n.replace(",", "").replace(".", "").strip()
                print("n:", n)

                if re.search(r"(million|mil|m)\b", n):
                    num_part = re.sub(r"(million|mil|m)\b", "", n).strip()
                    parsed_numbers.append(int(float(num_part) * 1_000_000))
                elif re.search(r"(k|thousand|thousands)\b", n):
                    num_part = re.sub(
                        r"(k|thousand|thousands)\b", "", n).strip()
                    parsed_numbers.append(int(float(num_part) * 1_000))
                else:
                    print("parsed n:", n)
                    parsed_numbers.append(int(float(n)))

        else:
            # Designed for Others Portals where the salary separator is a comma -- considers decimal (if present)
            numbers = re.findall(
                r"\d+(?:,\d+)*(?:\.\d+)?\s*(?:million|mil|m|k|thousand|thousands)?\b",
                salary_text,
                re.IGNORECASE
            )

            # Parse the numbers (conversion & formatting)
            parsed_numbers = []
            for n in numbers:
                n = n.replace(",", "").strip()

                if re.search(r"(million|mil|m)\b", n):
                    num_part = re.sub(r"(million|mil|m)\b", "", n).strip()
                    parsed_numbers.append(int(float(num_part) * 1_000_000))
                elif re.search(r"(k|thousand|thousands)\b", n):
                    num_part = re.sub(
                        r"(k|thousand|thousands)\b", "", n).strip()
                    parsed_numbers.append(int(float(num_part) * 1_000))
                else:
                    parsed_numbers.append(int(float(n)))

        # Assigning min and max sallary:
        if len(parsed_numbers) > 1:
            min_amount = parsed_numbers[0] if parsed_numbers[0] < parsed_numbers[1] else parsed_numbers[1]
            max_amount = parsed_numbers[0] if parsed_numbers[0] > parsed_numbers[1] else parsed_numbers[1]
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

        # Setting the currency
        for c in currency_values:
            if re.search(c.lower(), salary_text):
                currency = currency_values[0]
                break

        # Fallback if the currency of the country portal is not found
        if "currency" not in locals():
            for c in currency_dictionary:
                if c.lower() in salary_text:
                    currency = currency_dictionary[c]
                    break

        # Last Falback for currency
        if "currency" not in locals():
            currency = NA

        # Determining salary interval
        if re.search("year", salary_text):
            interval = "yearly"
        elif re.search("month", salary_text):
            interval = "monthly"
        elif re.search("week", salary_text):
            interval = "weekly"
        elif re.search("hour", salary_text):
            interval = "hourly"
        else:
            interval = NA
    else:
        return NA, NA, NA, NA, NA

    return salary_source, interval, min_amount, max_amount, currency


async def parse_company_logo(page, selector):
    logo = page.locator(selector)
    if await logo.count() > 0:
        logo_src = await logo.get_attribute("src")
        print(logo_src)
        return await logo.get_attribute("src")
    else:
        return NA


async def parse_company_info(portal, site, page):
    link_locator = page.locator(
        "a[data-automation='company-profile-profile-link']"
    )

    if await link_locator.count() > 0:
        company_url_href = await link_locator.get_attribute("href")
    else:
        company_url_href = NA

    if not pd.isna(company_url_href):
        company_url = f"https://{portal}.{site}.com{company_url_href}"
        print(company_url)
        await page.goto(company_url)

        company_url_direct_locator = page.locator("a[id='website-value']")
        if await company_url_direct_locator.count() > 0:
            company_url_direct = await company_url_direct_locator.get_attribute("href")
        else:
            company_url_direct = NA

        company_industry = await parse_text_content(
            page,
            "//h3[contains(text(), 'Industry')]/parent::div/following-sibling::div//span"
        )

        company_addresses = await parse_text_content(
            page,
            "//h3[contains(text(), 'Primary location')]/parent::div/following-sibling::div//span"
        )

        company_num_emp = await parse_text_content(
            page,
            "//h3[contains(text(), 'Company size')]/parent::div/following-sibling::div//span"
        )

        company_description = await parse_text_content(
            page,
            "//h2[contains(text(), 'Company overview')]/ancestor::div[3]/following-sibling::div[1]/div/div[last()]"
        )

    else:
        return NA, NA, NA, NA, NA, NA

    return company_industry, company_url, company_url_direct, company_addresses, company_num_emp, company_description


async def process_job_links(job_links, portal, site, currency_values, currency_dictionary, retry_attempt=0):
    """Process a list of job links and return job data and error links"""
    job_data = []
    error_links = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)

        # Create a clean context with no storage, cookies, or cache
        context = await browser.new_context(
            storage_state=None,  # No storage state
            accept_downloads=False,
            bypass_csp=False,
            color_scheme='light',
            extra_http_headers=None,
            offline=False,
            timezone_id=None,
            locale='en-US',
            # Clear any previous data
            java_script_enabled=True,
            permissions=[],
            # Use incognito-like settings
            ignore_https_errors=True
        )

        # Clear any existing cookies and storage at context level
        await clear_browser_data(context, page)

        print(
            f"\nProcessing {len(job_links)} job links (Attempt {retry_attempt + 1})")

        # Clear browser cache/data every 50 jobs to prevent accumulation
        for i, link in enumerate(job_links, 1):
            print(
                f"Processing job {i}/{len(job_links)} (Total errors so far: {len(error_links)})")

            # Clear browser data every 50 jobs or if memory might be building up
            if i % 50 == 0:
                print("Clearing browser data to prevent memory buildup...")
                await clear_browser_data(context, page)

            try:
                job_id = link.split("/job/")[1].split("?")[0]

                job_url = f"https://{portal}.{site}.com{link}"
                print(f"Job URL: {job_url}")
                await page.goto(job_url, timeout=30000)

                title = await parse_text_content(
                    page,
                    "//h1[@data-automation='job-detail-title']"
                )

                company = await parse_text_content(
                    page,
                    "//span[@data-automation='advertiser-name']"
                )

                location, is_remote, work_setup = await parse_location(
                    page,
                    "span[data-automation='job-detail-location']"
                )

                date_posted = await parse_date_posted(
                    page,
                    "xpath=(//span[contains(text(),'Posted')])[1]"
                )

                job_type = await parse_text_content(
                    page,
                    "//span[@data-automation='job-detail-work-type']"
                )

                salary_source, interval, min_amount, max_amount, currency = await parse_salary(
                    page,
                    "span[data-automation='job-detail-salary']",
                    currency_values,
                    currency_dictionary,
                    portal
                )

                job_function = await parse_text_content(
                    page,
                    "//span[@data-automation='job-detail-classifications']"
                )

                listing_type = link.split("type=")[1].split(
                    "&")[0] if "type=" in link else NA

                description = await parse_text_content(
                    page,
                    "//div[@data-automation='jobAdDetails']/div"
                )

                company_logo = await parse_company_logo(
                    page,
                    "div[data-testid='bx-logo-image'] img"
                )

                company_industry, company_url, company_url_direct, company_addresses, company_num_emp, company_description = await parse_company_info(
                    portal,
                    site,
                    page
                )

                job_data.append({
                    "id": job_id,
                    "site": site,
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
                    "is_remote": is_remote,
                    "work_setup": work_setup,
                    "job_level": NA,
                    "job_function": job_function,
                    "listing_type": listing_type,
                    "emails": NA,
                    "description": description,
                    "company_industry": company_industry,
                    "company_url": company_url,
                    "company_logo": company_logo,
                    "company_url_direct": company_url_direct,
                    "company_addresses": company_addresses,
                    "company_num_emp": company_num_emp,
                    "company_revenue": NA,
                    "company_description": company_description,
                })

            except Exception as e:
                print(f"Error processing job {i} ({link}): {str(e)}")
                error_links.append(link)
                continue

        await browser.close()

    return job_data, error_links


def save_error_links(error_links, site, portal, job_location, keyword, retry_attempt=0):
    """Save error links to a CSV file"""
    if not error_links:
        return None

    if retry_attempt > 0:
        filename = f"data/{site}_{portal}_{job_location}_{keyword}_retry_{retry_attempt}_errors.csv"
    else:
        filename = f"data/{site}_{portal}_{job_location}_{keyword}_errors.csv"

    error_df = pd.DataFrame(error_links, columns=['job_link'])
    error_df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"Error links saved to: {filename}")
    return filename


def load_error_links(filename):
    """Load error links from a CSV file"""
    try:
        if os.path.exists(f"data/{filename}"):
            error_df = pd.read_csv(f"data/{filename}")
            return error_df['job_link'].tolist()
        else:
            print(f"Error file not found: {filename}")
            return []
    except Exception as e:
        print(f"Error loading error links: {e}")
        return []


def save_job_data(job_data, site, portal, job_location, keyword, retry_attempt=0, is_rescraping=False):
    """Save job data to CSV file"""
    if not job_data:
        print("No job data to save")
        return None

    data_frame = pd.DataFrame(job_data)

    if is_rescraping:
        filename = f"data/{site}_{portal}_{job_location}_{keyword}_rescraped.csv"
    elif retry_attempt > 0:
        filename = f"data/{site}_{portal}_{job_location}_{keyword}_retry_{retry_attempt}.csv"
    else:
        filename = f"data/{site}_{portal}_{job_location}_{keyword}.csv"

    data_frame.to_csv(
        filename,
        index=False,
        quotechar='"',
        escapechar='\\',
        encoding='utf-8-sig'
    )

    print(f"Data saved to: {filename} ({len(job_data)} records)")
    return filename


async def web_scraper(is_rescraping, link_file_name, portal="my", site="jobstreet", job_location="", keyword="Data-Analyst", max_pages=2, max_retries=2):
    # Phase 1: Initialize
    print("Initiating JobStreet Scraper with Auto-Retry Logic")
    print(f"{portal} {site} {job_location} {keyword} {max_pages}")

    # Initialize currency dictionary
    currency_country_dictionary = {
        "ph": ["PHP", "â‚±"],
        "th": ["THB", "à¸¿"],
        "my": ["MYR", "RM"],
        "id": ["IDR", "Rp"],
        "sg": ["SGD", "$", "S$"],
        "vn": ["VND", "â‚«"],
    }
    currency_values = currency_country_dictionary[portal]

    currency_dictionary = {
        "IDR": "IDR", "MYR": "MYR", "PHP": "PHP", "THB": "THB", "USD": "USD", "SGD": "SGD", "VND": "VND",
        "Rp": "IDR", "RM": "MYR", "â‚±": "PHP", "à¸¿": "THB", "$": "SGD", "S$": "SGD", "â‚«": "VND",
    }

    # Phase 2: Get job links
    if is_rescraping:
        # Extract job links from the given file
        job_links_df = pd.read_csv(f"data/{link_file_name}", header=None)
        job_links = job_links_df[0].tolist()
        print(f"Loaded {len(job_links)} links from file for re-scraping")
    else:
        # Extract job links from the website
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=False)

            # Create clean context for link extraction
            context = await browser.new_context(
                storage_state=None,
                java_script_enabled=True,
                accept_downloads=False
            )
            await context.clear_cookies()

            page = await context.new_page()

            # Clear any browser storage
            await clear_browser_data(context, page)

            job_links = await extract_job_links(page, portal, site, job_location, keyword, max_pages)

            await browser.close()

        if not job_links:
            print("No job links found. Exiting.")
            return

    # Phase 3: Process job links with retry logic
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

        # Process current batch of links
        job_data, error_links = await process_job_links(
            current_links, portal, site, currency_values, currency_dictionary, retry_attempt
        )

        # Add successful jobs to our collection
        all_job_data.extend(job_data)

        # Save current batch of job data
        if job_data:
            save_job_data(job_data, site, portal, job_location,
                          keyword, retry_attempt, is_rescraping)

        # Handle error links
        if error_links:
            print(f"\nFound {len(error_links)} failed links")
            error_file = save_error_links(
                error_links, site, portal, job_location, keyword, retry_attempt)

            if retry_attempt < max_retries:
                print(
                    f"Will retry {len(error_links)} failed links in next attempt...")
                current_links = error_links
                retry_attempt += 1
            else:
                print(
                    f"Max retries ({max_retries}) reached. {len(error_links)} links still failed.")
                break
        else:
            print("No error links found. All jobs processed successfully!")
            break

    # Phase 4: Save final consolidated data
    if all_job_data:
        print(f"\n{'='*60}")
        print("FINAL CONSOLIDATION")
        print(f"{'='*60}")

        if is_rescraping:
            final_filename = f"data/{site}_{portal}_{job_location}_{keyword}_rescraped_final.csv"
        else:
            final_filename = f"data/{site}_{portal}_{job_location}_{keyword}_final.csv"

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

        # Summary statistics
        initial_links = len(job_links)
        successful_jobs = len(all_job_data)
        failed_jobs = initial_links - successful_jobs
        success_rate = (successful_jobs / initial_links) * \
            100 if initial_links > 0 else 0

        print(f"\nSCRAPING SUMMARY:")
        print(f"Initial job links: {initial_links}")
        print(f"Successfully scraped: {successful_jobs}")
        print(f"Failed to scrape: {failed_jobs}")
        print(f"Success rate: {success_rate:.1f}%")
        print(f"Total retry attempts: {retry_attempt}")


# Run the Function
if __name__ == "__main__":
    # User Input
    print("| = | = | = | Job Web Scraper with Auto-Retry | = | = | = |")

    rescrape_input = input(
        "\nAre you here to rescrape? (Y/N): ").lower().strip()
    is_rescraping = True if rescrape_input == "y" else False

    if is_rescraping:
        print(
            "Note: The filename format is {site}_{portal}_{location}_{keyword}_error.csv")
        link_file_name = input(
            "File name the CSV file from the data folder: ").strip()
        file_parts = link_file_name.split("_")

        site = file_parts[0]
        portal = file_parts[1]
        job_location = file_parts[2]
        keyword = file_parts[3]
        max_pages = 0
        max_retries = int(input("Maximum retry attempts (default 2): ") or "2")

    else:
        print("\nList of available Portal from JobStreet & JobsDB")
        print("id = Indonesia (JobStreet)")
        print("my = Malaysia (JobStreet)")
        print("sg = Singapore (JobStreet)")
        print("ph = Philippines (JobStreet)")
        print("th = Thailand (JobsDB)")
        portal = input("Choose a JobStreet Portal: ").lower().strip()
        job_location = input("Location (optional): ").strip()
        keyword = input("Job Position: ").strip().replace(" ", "-")
        max_pages = int(
            input("Maximum pages to scrape (default 50): ") or "50")
        max_retries = int(
            input("Maximum retry attempts for failed links (default 2): ") or "2")

        if portal == "th":
            site = "jobsdb"
        else:
            site = "jobstreet"

        link_file_name = ""

    # Terminal Logging
    log_file = setup_logging(
        is_rescraping, site, portal, job_location, keyword)

    # Proper Run
    asyncio.run(web_scraper(is_rescraping, link_file_name, portal,
                site, job_location, keyword, max_pages, max_retries))
