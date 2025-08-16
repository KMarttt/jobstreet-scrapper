# A Career Viet Scraper
from playwright.async_api import async_playwright
import asyncio
import pandas as pd
from pandas import NA
from datetime import datetime
import re


async def web_locator(page, selector):
    # Check if the element exists
    locator = page.locator(selector)
    if await locator.count() > 0:
        return (await locator.text_content()).strip()
    else:
        return NA

async def parse_location(page):
    # Select the location information 
    location = await web_locator(page, "//h3[contains(., 'Work location')]/following-sibling::div/span")

    if not pd.isna(location):
        print(f"Location: {location}")
        return location 

    location_alternative = await web_locator(page, "//strong[contains(., 'Location')]/following-sibling::p")

    if not pd.isna(location_alternative):
        print(f"Alternative Location: {location_alternative}")
        return location_alternative
    
    return NA


async def parse_date_posted(page):
    # Select the date posted information
    
    # date_string = (await page.locator("//strong[contains(., 'Updated')]/following-sibling::p").text_content()).strip()
    date_text = await web_locator(page, "//strong[contains(., 'Updated')]/following-sibling::p")

    # Parse the date text to date object
    date_object = datetime.strptime(date_text, "%d/%m/%Y")

    # Return the date as "YYYY-MM-DD" format
    return date_object.strftime("%Y-%m-%d")

async def parse_salary(page, selector, currency_values):
    # Select and process salary information

    salary_text = (await page.locator(selector).text_content()).strip().lower()
    print(salary_text)

    # TODO: do the same for the currency of the other platforms

    if salary_text != "competitive":
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
    else:
        return NA, NA, NA, NA, NA

    
    return salary_source, interval, min_amount, max_amount, currency 

async def parse_job_function(page, selector):
    job_function_raw_text = await web_locator(page, selector)
    job_function_text = re.sub(r'\s+', ' ', job_function_raw_text)
    job_function = [item.strip() for item in job_function_text.split(",") if item.strip()]
    print(f"Job Function: {job_function}")
    return job_function

async def parse_company_info(page, job_template_type):
    print(f"Job Template Type: {job_template_type}")
    
    match job_template_type:
        case "A":
            company = await web_locator(page, "//div[@class='apply-now-content']/div[1]/a")
            
        case "B":
            company = await web_locator(page, "//div[@class='title']/following-sibling::a[@class='company']")
            

    if not pd.isna(company):
        
        match job_template_type:
            case "A":
                company_url = await page.locator("//div[@class='apply-now-content']/div[1]/a").get_attribute("href")
            case "B":
                company_url = await page.locator("//div[@class='title']/following-sibling::a[@class='company']").get_attribute("href")

        await page.goto(company_url, wait_until="domcontentloaded", timeout=40000)
        print(f"Company URL: {company_url}")


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
                # Click "load more" button
                load_more_button_locator = page.locator("//div[@class='box-text more-less']/div[@class='view-style']/a[@class='read-more']")
                while await load_more_button_locator.is_visible():
                    await load_more_button_locator.click()
                    await page.wait_for_timeout(500)
                
                company_logo_src = page.locator("//div[@class='company-info']/div/div[@class='img']/img")
                company_logo = await company_logo_src.get_attribute("src") if await company_logo_src.count() > 0 else NA

                company_url_direct_text = await web_locator(page, "//strong[contains(., 'Company Information')]/following-sibling::ul/li[span[@class='mdi mdi-link']]")
                company_url_direct = company_url_direct_text.split("Website:")[1].strip() if not pd.isna(company_url_direct_text) else NA

                company_addresses = await web_locator(page, "//div[@class='content']/strong[contains(., 'Location')]/following-sibling::p")

                company_num_emp_text = await web_locator(page, "//strong[contains(., 'Company Information')]/following-sibling::ul/li[span[@class='mdi mdi-account-supervisor']]")
                company_num_emp = company_num_emp_text.split("Company size:")[1].strip() if not pd.isna(company_num_emp_text) else NA

                company_description = await web_locator(page, "//h2[contains(., 'About us')]/following-sibling::div[contains(@class, 'box-text')]/div[@class='main-text']")

            case "B":
                print("Company Template Type B")
                company_logo_src = page.locator("//span[@class='logoJobs']/table/tbody/tr/td/a/img")
                company_logo = await company_logo_src.get_attribute("src") if await company_logo_src.count() > 0 else NA

                company_url_direct_text = await web_locator(page, "//h2[@id='cp_company_name']/following-sibling::ul/li[span[contains(text(),'Website:')]]/span[contains(text(),'Website:')]")
                company_url_direct = company_url_direct_text.split("Website:")[1].strip() if not pd.isna(company_url_direct_text) else NA

                company_addresses = await web_locator(page, "//h2[@id='cp_company_name']/following-sibling::ul/li[1]")

                company_num_emp_text = await web_locator(page, "//h2[@id='cp_company_name']/following-sibling::ul/li[span[contains(text(),'Company size:')]]/span[contains(text(),'Company size:')]")
                company_num_emp = company_num_emp_text.split("Company size:")[1].strip() if not pd.isna(company_num_emp_text) else NA

                company_description = await web_locator(page, "//h2[contains(@class,'section-title') and contains(., 'About us')]/parent::header/following-sibling::div[@class='container']")

            case "C":
                print("Company Template Type C")
                company_logo_src = page.locator("//div[@class='profile-intro-wrap']/div[@class='img']/img")
                company_logo = await company_logo_src.get_attribute("src") if await company_logo_src.count() > 0 else NA
                
                company_url_direct_text = await web_locator(page, "//strong[contains(., 'Information')]/following-sibling::ul/li[span[@class='mdi mdi-link']]")
                company_url_direct = company_url_direct_text.split("Website:")[1].strip() if not pd.isna(company_url_direct_text) else NA

                company_addresses_text = await web_locator(page, "//p[@class='company-location']")
                company_addresses = company_addresses_text.split("Location")[1].strip() if not pd.isna(company_addresses_text) else NA

                company_num_emp_text = await web_locator(page, "//strong[contains(., 'Information')]/following-sibling::ul/li[span[@class='mdi mdi-account']]")
                company_num_emp = company_num_emp_text.split("Company size:")[1].strip() if not pd.isna(company_num_emp_text) else NA

                company_description = await web_locator(page, "//h2[contains(., 'About us')]/parent::div[@class='cb-title']/h2")

    else:
        return NA, NA, NA, NA, NA, NA, NA

    return company, company_url, company_logo, company_url_direct, company_addresses, company_num_emp, company_description

async def web_scraper(keyword, page_number):
    # Phase 1: Initiate
    print("Initiating Career Viet Scraper")
    print(f"{keyword} {page_number}")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)

        # Configure the browser to be Incognito (to have clean cookies, cache, etc.)
        context = await browser.new_context()
        # Open new page
        page = await context.new_page()
        job_links = []
        job_data = []


        # Phase 2: Extract all job links
        print("Extracting Job Links")
        for i in range(1, page_number + 1):
            await page.goto(f"https://careerviet.vn/jobs/{keyword}-k-page-{page_number}-en.html")

            job_item_links = page.locator("//div[contains(@class, 'title')]/h2/a[@class='job_link']")

            link_count = await job_item_links.count()

            for link in range(link_count):
                job_links.append(await job_item_links.nth(link).get_attribute('href'))

            print(job_links)
            print(f"# of job links: {len(job_links)}")
        
        currency_values = {
            "đ": "VND",
            "₫": "VND",
            "VND": "VND",
            "$": "USD",
            "USD": "USD"
        }

        # Phase 3: Extract Job Details
        print("\nExtracting Job Details")
        for link in job_links:
            job_id = link.split(".html")[0].rsplit(".", 1)[1]
            job_url = link

            await page.goto(job_url, wait_until="domcontentloaded")

            job_template_type = "A" if await page.locator("div.apply-now-content").count() > 0 else "B"

            match job_template_type:
                case "A":
                    title = await web_locator(page, "//div[@class='apply-now-content']/div[1]/div[1]")
                    location = await parse_location(page)
                    date_posted = await parse_date_posted(page)
                    job_type = await web_locator(page, "//strong[contains(., 'Job type')]/following-sibling::p" )
                    salary_source, interval, min_amount, max_amount, currency  = await parse_salary(page, "//strong[contains(., 'Salary')]/following-sibling::p", currency_values)
                    job_level = await web_locator(page, "//strong[contains(., 'Job level')]/following-sibling::p")
                    job_function = await parse_job_function(page, "//strong[contains(., 'Industry')]/following-sibling::p")
                    description = await web_locator(page, "//h2[contains(., 'Job Description')]/parent::div[@class='detail-row reset-bullet']")
                    requirement = await web_locator(page, "//h2[contains(., 'Job Requirement')]/parent::div[@class='detail-row reset-bullet']")
                    company, company_url, company_logo, company_url_direct, company_addresses, company_num_emp, company_description = await parse_company_info(page, job_template_type)
                case "B":
                    title = await web_locator(page, "//a[@class='company']/preceding-sibling::div[@class='title']")
                    location = await web_locator(page, "//h3[contains(., 'Work location')]/following-sibling::div/span")
                    date_posted = NA
                    job_type = await web_locator(page,"//p[contains(., 'Job type')]/parent::td/following-sibling::td")
                    salary_source, interval, min_amount, max_amount, currency  = await parse_salary(page, "//p[contains(., 'Salary')]/parent::td/following-sibling::td", currency_values)
                    job_level = await web_locator(page, "//p[contains(., 'Job level')]/parent::td/following-sibling::td")
                    job_function = await parse_job_function(page, "//p[contains(., 'Industry')]/parent::td/following-sibling::td")
                    description = await web_locator(page, "//h3[contains(., 'Job Description')]/parent::div[@class='title-icon']/following-sibling::div[1]/div[@class='content']")
                    requirement = await web_locator(page, "//h3[contains(., 'Job Requirement') and @class='detail-title']/following-sibling::div[@class='content']")
                    company, company_url, company_logo, company_url_direct, company_addresses, company_num_emp, company_description = await parse_company_info(page, job_template_type)


            # TODO: Add skills
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
        data_frame = pd.DataFrame(job_data)

        for col in data_frame.columns:
            if data_frame[col].dtype == "object":
                data_frame[col] = data_frame[col].apply(
                    lambda x: x.replace("\n", " ").replace("\r", " ").replace("\t", " ").strip() if isinstance(x, str) else x
                )

        data_frame.to_csv(f"data/careerviet_vn_{keyword}.csv", index=False, quotechar='"', escapechar='\\', encoding='utf-8-sig')
        print("Data saved to CSV")
            


if __name__ == "__main__":
    # User Input
    print("| = | = | = | Career Viet Web Scraper | = | = | = |")
    keyword = input("Keyword (e.g., data analyst): ").strip().replace(" ", "-")
    page_number = int(input("Number of pages you want to scrape: "))

    # Proper Run
    asyncio.run(web_scraper(keyword, page_number))