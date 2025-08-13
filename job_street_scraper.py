# A Job Street Scraper
from playwright.async_api import async_playwright
import asyncio
import pandas as pd
from pandas import NA
from datetime import datetime, timedelta
import re


async def parse_date_posted(page):
    date_text = (await page.locator("xpath=(//span[contains(text(),'Posted')])[1]").first.text_content()).strip()

    current_date = datetime.now()
    relative_date_text = date_text.replace(
        "Posted", "").replace("ago", "").strip()
    print(f"Relative date text: {relative_date_text!r}")

    if "d" in relative_date_text:  # if the date is in days
        days = int(re.sub(r"[^\d]", "", relative_date_text))
        return (current_date - timedelta(days=days)).strftime(r"%Y-%m-%d")
    elif "h" in relative_date_text:  # If the date is in hour(s) ago
        return current_date.strftime(r"%Y-%m-%d")
    else:  # If the date is in minute(s) or second(s)
        return current_date.strftime(r"%Y-%m-%d")


async def parse_location(page):
    location_section = (await page.locator("span[data-automation='job-detail-location']").text_content()).strip()

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


async def parse_salary(page, currency_values, currency_dictionary):
    salary_locator = page.locator("span[data-automation='job-detail-salary']")

    # Checks if the salary is present on the page
    if await salary_locator.count() > 0:
        salary_source = "direct_data"
        salary_text = (await salary_locator.text_content()).strip().lower()
        numbers = re.findall(r"\d[\d,]*", salary_text)
        print(numbers)
        numbers = [int(n.replace(",", "")) for n in numbers]
        print(numbers)

        # Assigning min and max sallary
        if len(numbers) > 1:  # If the salary is a range
            min_amount = numbers[0] if numbers[0] < numbers[1] else numbers[1]
            print("min ammount:", min_amount)
            max_amount = numbers[0] if numbers[0] > numbers[1] else numbers[1]
            print("max ammount:", max_amount)
        elif len(numbers) == 1:  # If the salary is fixed (1 value)
            min_amount = numbers[0]
            max_amount = numbers[0]
        else:
            min_amount = NA
            max_amount = NA

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
        salary_source = NA
        interval = NA
        min_amount = NA
        max_amount = NA
        currency = NA

    return salary_source, interval, min_amount, max_amount, currency


async def parse_company_logo(page):
    logo = page.locator("div[data-testid='bx-logo-image'] img")
    if await logo.count() > 0:
        logo_src = await logo.get_attribute("src")
        print(logo_src)
        return await logo.get_attribute("src")
    else:
        return NA


async def parse_company_info(portal, site, page):
    link_locator = page.locator(
        "a[data-automation='company-profile-profile-link']")

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

        company_industry_locator = page.locator(
            "xpath=//h3[contains(text(), 'Industry')]/parent::div/following-sibling::div//span")
        if await company_industry_locator.count() > 0:
            company_industry = (await company_industry_locator.text_content()).strip()
            print(company_industry)
        else:
            company_industry = NA

        company_addresses_locator = page.locator(
            "xpath=//h3[contains(text(), 'Primary location')]/parent::div/following-sibling::div//span")
        if await company_addresses_locator.count() > 0:
            company_addresses = (await company_addresses_locator.text_content()).strip()
            print(company_addresses)
        else:
            company_addresses = NA

        company_num_emp_locator = page.locator(
            "xpath=//h3[contains(text(), 'Company size')]/parent::div/following-sibling::div//span")
        if await company_num_emp_locator.count() > 0:
            company_num_emp = (await company_num_emp_locator.text_content()).strip()
            print(company_num_emp)
        else:
            company_num_emp = NA

        company_description_locator = page.locator(
            "//h2[contains(text(), 'Company overview')]/ancestor::div[3]/following-sibling::div[1]/div/div[last()]")
        if await company_description_locator.count() > 0:
            company_description = (await company_description_locator.text_content()).strip()
        else:
            company_description = NA
    else:
        company_url = NA
        company_industry = NA
        company_url_direct = NA
        company_addresses = NA
        company_num_emp = NA
        company_description = NA

    return company_url, company_industry, company_url_direct, company_addresses, company_num_emp, company_description


async def web_scraper(portal="my", site="jobstreet", location="", keyword="Data-Analyst", max_pages=50):
    # Phase 1: Initiate
    print("Initiating JobStreet Scraper")
    print(f"{portal} {site} {location} {keyword}")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=False,
        )

        # Configuring the browser to be Incognito (to have clean cookies, cache, etc.)
        context = await browser.new_context()
        # Open new page
        page = await context.new_page()
        job_links = []
        job_data = []
        seen_links = set()  # To avoid duplicate job links

        # Phase 2: Extract all job links with automatic page detection
        print("Extracting Job Links")
        current_page = 1
        consecutive_empty_pages = 0
        max_consecutive_empty = 3  # Stop if we encounter 3 consecutive empty pages

        while current_page <= max_pages and consecutive_empty_pages < max_consecutive_empty:
            loc_param = f"/in-{location}" if location else ""
            url = f"https://{portal}.{site}.com/{keyword}-jobs{loc_param}?page={current_page}"
            print(f"Scraping page {current_page}: {url}")

            try:
                await page.goto(url, timeout=30000)  # 30 second timeout

                # Wait for the job cards to load
                await page.wait_for_selector("article", timeout=10000)

                # Locator for job card links
                card_links = page.locator(
                    "article div._1lns5ab0._6c7qzn50._6c7qzn4y a")

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
                print(f"Error scraping page {current_page}: {str(e)}")
                consecutive_empty_pages += 1

            current_page += 1

        print(
            f"\nTotal Job Links collected: {len(job_links)} from {current_page - 1} pages")
        print(f"Unique job links: {len(set(job_links))}")

        if not job_links:
            print("No job links found. Exiting.")
            await browser.close()
            return

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

        # Phase 3: Extract Job Details
        print("\nExtracting Job Details")
        for i, link in enumerate(job_links, 1):
            print(f"Processing job {i}/{len(job_links)}")

            try:
                job_id = link.split("/job/")[1].split("?")[0]
                job_url = f"https://{portal}.{site}.com{link}"
                print(f"Job URL: {job_url}")
                await page.goto(job_url, timeout=30000)

                title = (await page.locator("h1[data-automation='job-detail-title']").text_content()).strip()
                company = (await page.locator("span[data-automation='advertiser-name']").text_content()).strip()
                location, is_remote, work_setup = await parse_location(page)
                date_posted = await parse_date_posted(page)
                job_type = (await page.locator("span[data-automation='job-detail-work-type']").text_content()).strip()
                salary_source, interval, min_amount, max_amount, currency = await parse_salary(page, currency_values, currency_dictionary)
                job_function = (await page.locator("span[data-automation='job-detail-classifications']").text_content()).strip()
                listing_type = link.split("type=")[1].split(
                    "&")[0] if "type=" in link else NA
                description = (await page.locator("div._1lns5ab0.sye2ly0").text_content()).strip()
                company_logo = await parse_company_logo(page)
                company_url, company_industry, company_url_direct, company_addresses, company_num_emp, company_description = await parse_company_info(portal, site, page)

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
                continue

        await browser.close()

        print("Extraction Completed")
        print("Saving data to CSV")
        data_frame = pd.DataFrame(job_data)

        for col in data_frame.columns:
            if data_frame[col].dtype == "object":
                data_frame[col] = data_frame[col].apply(
                    lambda x: x.replace("\n", " ").replace(
                        "\r", " ") if isinstance(x, str) else x
                )

        data_frame.to_csv("data/jobstreet_jobs.csv", index=False,
                          quotechar='"', escapechar='\\', encoding='utf-8-sig')
        print(f"Data saved to CSV with {len(job_data)} job records")

# Run the Function
if __name__ == "__main__":

    # User Input
    print("| = | = | = | Job Web Scraper | = | = | = |")
    print("List of available Portal from JobStreet & JobsDB")
    print("id = Indonesia (JobStreet)")
    print("my = Malaysia (JobStreet)")
    print("sg = Singapore (JobStreet)")
    print("ph = Philippines (JobStreet)")
    print("th = Thailand (JobsDB)")
    portal = input("Choose a JobStreet Portal: ").lower().strip()
    location = input("Location (optional): ").strip()
    keyword = input("Job Position: ").strip().replace(" ", "-")
    max_pages = int(input("Maximum pages to scrape (default 50): ") or "50")

    if portal == "th":
        site = "jobsdb"
    else:
        site = "jobstreet"

    # Proper Run
    asyncio.run(web_scraper(portal, site, location, keyword, max_pages))
