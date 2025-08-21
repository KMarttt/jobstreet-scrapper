# A Job Net Scraper
from playwright.async_api import async_playwright
import asyncio
import pandas as pd
from pandas import NA
from datetime import datetime, timedelta
import re

async def parse_text_content(page, selector):
    # Check if the element exists and return its text content
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

        # Handle absolute date
        try:
            date_posted_object = datetime.strptime(date_posted_text, "%d %b %Y")
            return date_posted_object.strftime("%Y-%m-%d")
        except ValueError:
            pass 
        

        # Handle relative times (e.g. "3 weeks ago", "2 days ago", ...)
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
                    delta = timedelta(days = 30 * num) #approx
                case "year":
                    delta = timedelta(days = 365 * num) #approx

            date_posted_object = datetime.today() - delta
            return date_posted_object.strftime("%Y-%m-%d")
        
        # Handle special words
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

    if salary_text != "negotiable" and salary_text != "competitive":
        salary_source = "direct_data"
        # Select the salary value (with units)
        numbers = re.findall(r"\d+(?:,\d+)*(?:\.\d+)?\s*(?:million|mil|m|k|thousand|thousands)?\b", salary_text, re.IGNORECASE)

        # Parse the numbers (conversion & formatting)
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


async def web_scraper(portal="mm", keyword="data+analyst", max_pages=2):
    
    # === Phase 1: Initiate
    print("Initiating Job Net Scraper")
    print(f"Parameters: {portal} {keyword} {max_pages}")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=False,
        )
    
        # Configure the browser to be Incognito (to have clean cookies, cache, etc.)
        context = await browser.new_context()
        # Open new page
        page = await context.new_page()


        # === Phase 2: Login
        # Navigate to the login page
        await page.goto(f"https://www.jobnet.com.{portal}/login")

        # Login Values
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


        # ===  Phase 3: Extract all job links
        
        # Initialize variables 
        job_links = []
        seen_links = set()
        current_page = 1

        # Navigate to the job search page
        await page.goto(f"https://www.jobnet.com.{portal}/jobs?kw={keyword}")
        print(f"Extracting links from https://www.jobnet.com.{portal}/jobs?kw={keyword}")

        while current_page <= max_pages:
            # Wait and select the job cards
            await page.wait_for_selector("a.search__job-title.ClickTrack-JobDetail")
            job_item_links = page.locator("a.search__job-title.ClickTrack-JobDetail")

            link_count = await job_item_links.count()

            if link_count == 0:
                # Stop extracting links if no links were found
                print(f"No job links found on jobnet.com.{portal} for the keyword of \"{keyword}\"")
                break
            else:
                page_job_links = []

                # Extract job links
                for link in range(link_count):
                    job_href = await job_item_links.nth(link).get_attribute('href')
                    
                    if job_href and job_href not in seen_links:
                        job_links.append(job_href)
                        seen_links.add(job_href)
                        page_job_links.append(job_href)
                    
                print(f"Found {len(page_job_links)} job links on page {current_page} of {max_pages}")
                

                # Locate the next page button
                next_button = page.locator("//div[@class='search__action-wrapper-left']/a[last()]")
                
                # Checks if the next page button exists
                if await next_button.count() > 0:
                    # Checks if the button is disabled
                    is_button_disabled = True if await next_button.get_attribute("disabled") else False

                    
                    if is_button_disabled:
                        # Stop extracting links if the next page button is disabled
                        break
                    else:
                        # Click the pagination control
                        await next_button.hover()
                        await next_button.click(force=True)
                        current_page += 1
                else:
                    # Stop extracting links if the next page button is not found
                    break
        
        print(f"\nTotal Job Links collected: {len(job_links)} from {current_page} pages of {max_pages} pages")
        print(f"Unique job links: {len(set(job_links))} from {current_page} pages of {max_pages} pages")
        # ===  Phase 4: Extract Job Details

        # Initialize currency variables
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

        # Initialize data list
        job_data = []

        # Extract job details
        print("\nExtracting Job Details")
        for i, link in enumerate(job_links, 1):
            print(f"Processing job {i}/{len(job_links)}")

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

            except Exception as e:
                print(f"Error processing job {i} ({link}): {str(e)}")
                continue

            job_data.append({
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
            })

        print("Extraction Completed")
        print("Saving data to CSV")
        data_frame = pd.DataFrame(job_data)

        for col in data_frame.columns:
            if data_frame[col].dtype == "object":
                data_frame[col] = data_frame[col].apply(
                    lambda x: re.sub(r"\s+", " ", x).strip() if isinstance(x, str) else x
                )

        data_frame.to_csv(f"data/jobnet_{portal}_{keyword}.csv", 
            index=False, 
            quotechar='"', 
            escapechar='\\', 
            encoding='utf-8-sig'
        )
        print("Data saved to CSV")


if __name__ == "__main__":
    # User Input
    print("| = | = | = | Job Net Scraper | = | = | = |")
    print("List of available Portal from JobNet")
    print("mm = Myanmar")
    print("kh = Cambodia")
    portal = input("Choose a JobNet Portal: ").lower().strip()
    keyword = input("Keyword (e.g., data analyst): ").strip().replace(" ", "+")
    max_pages = int(input("Maximum pages to scrape (default 50): ") or "50")

    # Proper Run
    asyncio.run(web_scraper(portal, keyword, max_pages))

    # # Test Run
    # asyncio.run(web_scraper())
