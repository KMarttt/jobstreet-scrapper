# A Career Viet Scraper
from playwright.async_api import async_playwright
import asyncio
import pandas as pd
from pandas import NA
from datetime import datetime, timedelta
import re


# -----------------------------
# Logging setup for cmd terminal
log_file = open("data/output_jobstreet_sg_data-analyst.txt", "w", encoding="utf-8")

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

sys.stdout = sys.stderr = Tee(sys.stdout, log_file)
# -----------------------------

async def parse_text_content(page, selector):
    # Check if the element exists
    locator = page.locator(selector).first
    if await locator.count() > 0:
        return (await locator.text_content()).strip()
    else:
        return NA

async def parse_date_posted(page, selector):
    date_posted_text = await parse_text_content(page, selector)

    if not pd.isna(date_posted_text):

        date_posted_text = date_posted_text.lower()

        # Handle absolute date
        try:
            date_posted_object = datetime.strptime(date_posted_text, "%d %b %Y")
            return date_posted_object.strftime("%Y-%m-%d")
        except ValueError:
            pass 
        

        # Handle relative times (e.g. "3 weeks ago", "2 days ago", ...)
        match = re.search(
            r"(\d+)\s*(second|minutes|day|week|month|year)",
            date_posted_text
        )

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

async def parse_salary(page, selector, currency_values):
    # Select and process salary information

    # salary_text = (await page.locator(selector).text_content()).strip().lower()
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
            if c.lower() in salary_text:
                currency = currency_values[c]
                break
        
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

async def parse_job_function(page, selector):
    job_function_raw_text = await parse_text_content(page, selector)
    job_function_text = re.sub(r'\s+', ' ', job_function_raw_text)
    job_function = [item.strip() for item in job_function_text.split(",") if item.strip()]
    print(f"Job Function: {job_function}")
    return job_function

async def parse_year_of_experience(page, selector):
    year_of_experience_raw = await parse_text_content(page, selector)

    # Remove extra spaces
    if not pd.isna(year_of_experience_raw):
        year_of_experience = re.sub(
            r"\s+",
            " ",
            year_of_experience_raw
        ).lstrip(", ")
    else:
        return NA
    
    return year_of_experience

async def parse_skill(page, selector):
    # Get the skill(s)
    skill_li =  page.locator(selector)

    # Count the number of skill_li
    skill_count = await skill_li.count()

    if skill_count > 0:
        # Insert skill(s) into a list
        skills = []
        for i in range(skill_count):
            # Select the skill
            skill_raw = (await skill_li.nth(i).text_content()).strip()
            
            # Remove extra spaces
            skill = re.sub(
                r"\s+",
                " ",
                skill_raw
            ).lstrip(", ")

            # Add the skill to the list
            skills.append(skill)
    else:
        return NA
    
    return skills


async def parse_company_info(page, job_template_type):
    print(f"Job Template Type: {job_template_type}")
    
    
    match job_template_type:

        case "A":
            company = await parse_text_content(
                page,
                "//div[@class='apply-now-content']/div[1]/a"
            )
            
        case "B":
            company = await parse_text_content(
                page,
                "//div[@class='title']/following-sibling::a[@class='company']"
            )
            

    if not pd.isna(company):
        
        match job_template_type:

            case "A":
                company_url = await page.locator(
                    "//div[@class='apply-now-content']/div[1]/a"
                ).get_attribute("href")

            case "B":
                company_url = await page.locator(
                    "//div[@class='title']/following-sibling::a[@class='company']"
                ).get_attribute("href")

        await page.goto(company_url, wait_until="domcontentloaded", timeout=40000)
        print(f"Company URL: {company_url}")

        # Determine the company template
        if await page.locator("header.header-premium").count() > 0:
            # Premium Template (Usually used by CareerViet)
            company_template_type = "C"
        elif await page.locator("div.section-page.cp_basic_info").count() > 0:
            # Template with better UI 
            company_template_type = "B"
        else:
            # Basic Template 
            company_template_type = "A"

        print(f"Company Template Type: {company_template_type}")
        
        match company_template_type:
            
            case "A":
                print("Company Template Type A")

                # Find the "load more" button
                load_more_button_locator = page.locator(
                    "//h2[contains(., 'About Us')]/following-sibling::div[@class='box-text more-less']/div[@class='view-style']/a[@class='read-more']"
                )

                # Click the "load more" button
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
                company_url_direct = company_url_direct_text.split("Website:")[1].strip() if not pd.isna(company_url_direct_text) else NA

                company_addresses = await parse_text_content(
                    page,
                    "//div[@class='content']/strong[contains(., 'Location')]/following-sibling::p"
                )

                company_num_emp_text = await parse_text_content(
                    page,
                    "//strong[contains(., 'Company Information')]/following-sibling::ul/li[span[@class='mdi mdi-account-supervisor']]"
                )
                company_num_emp = company_num_emp_text.split("Company size:")[1].strip() if not pd.isna(company_num_emp_text) else NA

                company_description = await parse_text_content(
                    page,
                    "//h2[contains(., 'About us')]/following-sibling::div[contains(@class, 'box-text')]/div[@class='main-text']"
                )

            case "B":
                print("Company Template Type B")

                company_logo_src = page.locator(
                    "//span[@class='logoJobs']/table/tbody/tr/td/a/img"
                )
                company_logo = await company_logo_src.get_attribute("src") if await company_logo_src.count() > 0 else NA

                company_url_direct_text = await parse_text_content(
                    page,
                    "//h2[@id='cp_company_name']/following-sibling::ul/li[span[contains(text(),'Website:')]]/span[contains(text(),'Website:')]"
                )
                company_url_direct = company_url_direct_text.split("Website:")[1].strip() if not pd.isna(company_url_direct_text) else NA

                company_addresses = await parse_text_content(
                    page,
                    "//h2[@id='cp_company_name']/following-sibling::ul/li[1]"
                )

                company_num_emp_text = await parse_text_content(
                    page,
                    "//h2[@id='cp_company_name']/following-sibling::ul/li[span[contains(text(),'Company size:')]]/span[contains(text(),'Company size:')]"
                )
                company_num_emp = company_num_emp_text.split("Company size:")[1].strip() if not pd.isna(company_num_emp_text) else NA

                company_description = await parse_text_content(
                    page,
                    "//h2[contains(@class,'section-title') and contains(., 'About us')]/parent::header/following-sibling::div[@class='container']"
                )

            case "C":
                print("Company Template Type C")

                company_logo_src = page.locator(
                    "//div[@class='profile-intro-wrap']/div[@class='img']/img"
                )
                company_logo = await company_logo_src.get_attribute("src") if await company_logo_src.count() > 0 else NA
                
                company_url_direct_text = await parse_text_content(
                    page, 
                    "//strong[contains(., 'Information')]/following-sibling::ul/li[span[@class='mdi mdi-link']]"
                )
                company_url_direct = company_url_direct_text.split("Website:")[1].strip() if not pd.isna(company_url_direct_text) else NA

                company_addresses_text = await parse_text_content(
                    page, 
                    "//p[@class='company-location']"
                )
                company_addresses = company_addresses_text.split("Location")[1].strip() if not pd.isna(company_addresses_text) else NA

                company_num_emp_text = await parse_text_content(
                    page, 
                    "//strong[contains(., 'Information')]/following-sibling::ul/li[span[@class='mdi mdi-account']]"
                )
                company_num_emp = company_num_emp_text.split("Company size:")[1].strip() if not pd.isna(company_num_emp_text) else NA

                company_description = await parse_text_content(
                    page, 
                    "//h2[contains(., 'About us')]/parent::div[@class='cb-title']/h2"
                )

    else:
        return NA, NA, NA, NA, NA, NA, NA

    return company, company_url, company_logo, company_url_direct, company_addresses, company_num_emp, company_description


async def web_scraper(keyword="data-mining", max_pages=50):
    # Phase 1: Initiate
    print("Initiating Career Viet Scraper")
    print(f"{keyword} {max_pages}")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)

        # Configure the browser to be Incognito (to have clean cookies, cache, etc.)
        context = await browser.new_context()
        # Open new page
        page = await context.new_page()

        # Phase 2: Extract all job links

        # Initialize variables
        job_links = []
        seen_links = set() # To avoid duplicate job links
        current_page = 1
        consecutive_empty_pages = 0
        max_consecutive_empty = 3 # Stop if we encounter 3 consecutive empty pages

        # Extract job links
        print("Extracting Job Links")
        while current_page <= max_pages and consecutive_empty_pages < max_consecutive_empty:
            url = f"https://careerviet.vn/jobs/{keyword}-k-page-{current_page}-en.html"
            print(f"Scraping page {current_page}: {url}")

            try:
                await page.goto(url, timeout=30000) # 30 second timeouut

                job_item_links = page.locator(
                    "//div[contains(@class, 'title')]/h2/a[@class='job_link']"
                )

                link_count = await job_item_links.count()

                if link_count == 0:
                    consecutive_empty_pages += 1
                    print(
                        f"No job links found on page {current_page}. Empty pages count: {consecutive_empty_pages}"
                    )

                    # Check if we've reached the end
                    no_result_indicator = page.locator(
                        "//div[@class='no-search']/div[@class='image']/figure/figcaption"
                    )

                    if await no_result_indicator.count() > 0:
                        consecutive_empty_pages += 1
                        print(
                            f"End of results detected on page {current_page}"
                        )
                        print("Reached end of job listings")
                        break

                else:
                    consecutive_empty_pages = 0 # Reset counter if we found jobs
                    page_job_links = []

                    for link_index in range(link_count):
                        link_href = await job_item_links.nth(link_index).get_attribute("href")
                        if link_href and link_href not in seen_links:
                            job_links.append(link_href)
                            seen_links.add(link_href)
                            page_job_links.append(link_href)
                    
                    print(
                        f"Found {len(page_job_links)} new job links on page {current_page}"
                    )

            except Exception as e:
                print(e)
                consecutive_empty_pages += 1

            current_page += 1

        print(
            f"\nTotal Job Links collected: {len(job_links)} from {current_page - 1} pages"
        )
        print(f"Unique job links: {len(set(job_links))}")
        
        # Phase 3: Extract Job Details

        # Initialize variables for currency conversion
        currency_values = {
            "đ": "VND",
            "₫": "VND",
            "VND": "VND",
            "$": "USD",
            "USD": "USD"
        }

        # Initialize data list
        job_data = []
        
        print("\nExtracting Job Details")
        for link in job_links:
            job_id = link.split(".html")[0].rsplit(".", 1)[1]
            job_url = link

            await page.goto(job_url, wait_until="domcontentloaded")
            print(f"Scraping job: {job_url}")

            job_template_type = "A" if await page.locator("div.apply-now-content").count() > 0 else "B"

            match job_template_type:

                case "A":
                    title = await parse_text_content(
                        page, 
                        "//div[@class='apply-now-content']/div[1]/div[1]"
                    )

                    location = await parse_text_content(
                        page,
                        "//strong[contains(., 'Location')]/following-sibling::p"
                    )

                    date_posted = await parse_date_posted(
                        page,
                        "//strong[contains(., 'Updated')]/following-sibling::p", 
                    )

                    job_type = await parse_text_content(
                        page, 
                        "//strong[contains(., 'Job type')]/following-sibling::p"
                    )

                    salary_source, interval, min_amount, max_amount, currency  = await parse_salary(
                        page, 
                        "//strong[contains(., 'Salary')]/following-sibling::p", 
                        currency_values
                    )

                    job_level = await parse_text_content(
                        page, 
                        "//strong[contains(., 'Job level')]/following-sibling::p"
                    )

                    job_function = await parse_job_function(
                        page, 
                        "//strong[contains(., 'Industry')]/following-sibling::p"
                    )

                    year_of_experience = await parse_year_of_experience(page,"//strong[contains(., 'Experience')]/following-sibling::p")

                    skill = await parse_skill(
                        page,
                        "//h2[contains(., 'Job tags / skills')]/following-sibling::ul/li"
                    )

                    description = await parse_text_content(
                        page, 
                        "//h2[contains(., 'Job Description')]/parent::div[@class='detail-row reset-bullet']"
                    )

                    requirement = await parse_text_content(
                        page, 
                        "//h2[contains(., 'Job Requirement')]/parent::div[@class='detail-row reset-bullet']"
                    )

                    company, company_url, company_logo, company_url_direct, company_addresses, company_num_emp, company_description = await parse_company_info(
                        page, 
                        job_template_type
                    )

                case "B":
                    
                    title = await parse_text_content(
                        page, 
                        "//a[@class='company']/preceding-sibling::div[@class='title']"
                    )

                    location = await parse_text_content(
                        page, 
                        "//h3[contains(., 'Work location')]/following-sibling::div/span"
                    )

                    date_posted = await parse_text_content(
                        page, 
                        "//p[contains(., 'Updated')]/parent::td/following-sibling::td"
                    )

                    job_type = await parse_text_content(
                        page,
                        "//p[contains(., 'Job type')]/parent::td/following-sibling::td"
                    )

                    salary_source, interval, min_amount, max_amount, currency  = await parse_salary(
                        page, 
                        "//p[contains(., 'Salary')]/parent::td/following-sibling::td", currency_values
                    )

                    job_level = await parse_text_content(
                        page, 
                        "//p[contains(., 'Job level')]/parent::td/following-sibling::td"
                    )

                    job_function = await parse_job_function(
                        page, 
                        "//p[contains(., 'Industry')]/parent::td/following-sibling::td"
                    )

                    year_of_experience = await parse_year_of_experience(
                        page,
                        "//p[contains(., 'Experience')]/parent::td/following-sibling::td"
                    )

                    skill = await parse_skill(
                        page,
                        "//h3[contains(., 'JOB TAGS / SKILLS:') and @class='detail-title']/following-sibling::ul/li"
                    )


                    description = await parse_text_content(
                        page, 
                        "//h3[contains(., 'Job Description') and @class='detail-title']/following-sibling::div[@class='content']"
                    )

                    requirement = await parse_text_content(
                        page, 
                        "//h3[contains(., 'Job Requirement') and @class='detail-title']/following-sibling::div[@class='content']"
                    )

                    company, company_url, company_logo, company_url_direct, company_addresses, company_num_emp, company_description = await parse_company_info(
                        page, 
                        job_template_type
                    )

            job_data.append({
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
            })
        
        print("Extraction Completed")
        print("Saving data to CSV")

        # Phase 4: Save to CSV
        data_frame = pd.DataFrame(job_data)

        # Last cleanup
        for col in data_frame.columns:
            if data_frame[col].dtype == "object":
                data_frame[col] = data_frame[col].apply(
                    lambda x: re.sub(r"\s+", " ", x).strip() if isinstance(x, str) else x
                )

        # Save to CSV
        data_frame.to_csv(f"data/careerviet_vn_{keyword}.csv", 
            index=False, 
            quotechar='"', 
            escapechar='\\', 
            encoding='utf-8-sig'
        )
        print("Data saved to CSV")
            

if __name__ == "__main__":
    # User Input
    print("| = | = | = | Career Viet Web Scraper | = | = | = |")
    keyword = input("Keyword (e.g., data analyst): ").strip().replace(" ", "-")
    max_pages = int(input("Maximum pages to scrape (default 50): ") or "50")

    # Proper Run
    asyncio.run(web_scraper(keyword, max_pages))

    # # Test Run
    # asyncio.run(web_scraper())