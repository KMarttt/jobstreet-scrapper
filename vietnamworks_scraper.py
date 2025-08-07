# A Vietnam Works Scraper
from playwright.async_api import async_playwright
import asyncio
import pandas as pd
from pandas import NA
from datetime import datetime
import re

async def parse_title(page):
    title_text = (await page.locator("h1[name='title']").text_content()).strip()
    title_text_lower = title_text.lower()

    if "work from home" in title_text_lower or "wfh" in title_text_lower or "remote" in title_text_lower:
        work_setup = "remote"
        is_remote = True
    elif "hybrid" in title_text_lower:
        work_setup = "hybrid"
        is_remote = False
    else:
        is_remote = False
        work_setup = "onsite"

    return title_text, work_setup, is_remote

async def parse_date_posted(page):
    date_string = (await page.locator("//label[contains(., 'POSTED DATE')]/following-sibling::p[1]").text_content()).strip()

    # Parse the date string
    date_object = datetime.strptime(date_string, "%d/%m/%Y")

    # return the date as "YYYY-MM-DD"
    return date_object.strftime("%Y-%m-%d")

async def parse_salary(page, currency_values):
    salary_text = (await page.locator("//h1[@name='title']/parent::div/parent::div/following-sibling::div[1]/div/span").text_content()).strip()

    if salary_text != "Negotiable":
        salary_source = "direct_data"

        # numbers = re.findall(r"\d[\d,]*", salary_text)
        numbers = re.findall(r"\d[\d,.]*\s*[mM]?", salary_text)

        # Parse the numbers (conversion & formatting)
        parsed_numbers = []
        for n in numbers:
            n = n.strip().lower().replace(",","")
            if n.endswith("m"):
                parsed_numbers.append(int(float(n[:-1]) * 1_000_000))
            else:
                parsed_numbers.append(int(n))
        
        # Assigning min and max sallary
        if len(parsed_numbers) > 1: #If the salary is a range
            min_amount = parsed_numbers[0] if parsed_numbers[0] < parsed_numbers[1] else parsed_numbers[1]
            print("min ammount:", min_amount)
            max_amount = parsed_numbers[0] if parsed_numbers[0] > parsed_numbers[1] else parsed_numbers[1]
            print("max ammount:",max_amount)
        elif len(parsed_numbers) == 1: #If the salary is fixed (1 value)
            min_amount = parsed_numbers[0]
            max_amount = parsed_numbers[0]
        else:
            min_amount = NA
            max_amount = NA
        
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
        elif "hour" in salary_text:
            interval = "hourly" 
        else:
            interval = NA

    else:
        salary_source, interval, min_amount, max_amount, currency = NA, NA, NA, NA, NA
    
    print(f"Salary Text: {salary_text} ")
    
    return salary_source, interval, min_amount, max_amount, currency

async def parse_other_job_data(page):
    year_of_experience_text = (await page.locator("//label[contains(., 'YEAR OF EXPERIENCE')]/following-sibling::p[1]").text_content()).strip()
    year_of_experience = year_of_experience_text if year_of_experience_text != "Not shown" else NA
    
    education_level_text = (await page.locator("//label[contains(., 'EDUCATION LEVEL')]/following-sibling::p[1]").text_content()).strip()
    education_level = education_level_text if education_level_text != "Not shown" else NA
    
    age_preference_text = (await page.locator("//label[contains(., 'AGE PREFERENCE')]/following-sibling::p[1]").text_content()).strip()
    age_preference = age_preference_text if age_preference_text != "Not shown" else NA
    
    skill_text = (await page.locator("//label[contains(., 'SKILL')]/following-sibling::p[1]").text_content()).strip()
    skill = skill_text if skill_text != "Not shown" else NA
    
    preferred_language_text = (await page.locator("//label[contains(., 'PREFERRED LANGUAGE')]/following-sibling::p[1]").text_content()).strip()
    preferred_language = preferred_language_text if preferred_language_text != "Not shown" else NA
    
    nationality_text = (await page.locator("//label[contains(., 'NATIONALITY')]/following-sibling::p[1]").text_content()).strip()
    nationality = nationality_text if nationality_text != "Not shown" else NA

    return year_of_experience, education_level, age_preference, skill, preferred_language, nationality


async def web_scraper(keyword="data-analyst", page_number=1):
    # Phase 1: Initiate
    print("Initiating Vietnam Works Scraper")
    print(f"{keyword} {page_number}")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)

        # Configure the browser to be Incognito (to have clean cookies, cache, etc.)
        context = await browser.new_context()
        # Open new page
        page = await context.new_page()
        job_links = []
        job_data = []

        # Phase 2: Extracct all job links
        print("Extracting Job Links")
        for i in range(1, page_number + 1):
            await page.goto(f"https://www.vietnamworks.com/jobs?q={keyword}&page={i}&sorting=relevant")

            await page.wait_for_selector("a.img_job_card", timeout=10000)
            job_item_links = page.locator("a.img_job_card")

            link_count = await job_item_links.count()

            for link in range(link_count):
                print(await job_item_links.nth(link).get_attribute('href'))
                job_links.append(await job_item_links.nth(link).get_attribute('href'))
        
        print(job_links)
        print(f"# of job links: {len(job_links)}")


        currency_values = {
            "đ": "VND",
            "VND": "VND",
            "$": "USD",
            "USD": "USD"
        }

        # Phase 3: Extract Job Details
        print("\nExtracting Job Details")
        for link in job_links:
            
            job_id = re.search(r"-(\d+)-jd", link).group(1)
            job_url = f"https://www.vietnamworks.com/{link}"
            print(job_url)
            await page.goto(job_url, wait_until="domcontentloaded")
            
            # Click "Xem thêm" to see more and expand the section
            see_more_button = page.locator("//h2[contains(., 'Job Information')]/following-sibling::div[last()]/div[1]//button[contains(., 'View more')]")
            await see_more_button.scroll_into_view_if_needed()

            # Try clicking until the button is clicked (idk why sometimes it's not being clicked)
            while await see_more_button.count() > 0:
                await see_more_button.hover()
                
                # Click (force to ensure it's triggered)
                await see_more_button.click(force=True)
                
                # Small wait for the section to expand
                await page.wait_for_timeout(500)

            title, work_setup, is_remote = await parse_title(page)
            # company
            location = (await page.locator("//h2[contains(., 'Job Locations')]/following-sibling::div[1]/div/div/p").text_content()).strip()
            date_posted = await parse_date_posted(page)
            job_type = (await page.locator("//label[contains(., 'WORKING TYPE')]/following-sibling::p[1]").text_content()).strip()
            salary_source, interval, min_amount, max_amount, currency = await parse_salary(page, currency_values)
            job_level = (await page.locator("//label[contains(., 'JOB LEVEL')]/following-sibling::p[1]").text_content()).strip()
            job_function = (await page.locator("//label[contains(., 'JOB FUNCTION')]/following-sibling::p[1]").text_content()).strip()
            year_of_experience, education_level, age_preference, skill, preferred_language, nationality = await parse_other_job_data(page)

            # # print(f"Job ID: {job_id}")
            # print(f"Title: {title}")
            # print(f"Location: {location}")
            # print(f"Date Posted: {date_posted}")
            # print(f"Job Type: {job_type}")
            # print(f"Salary Source: {salary_source}; Interval: {interval}; Min Amount: {min_amount}; Max Amount: {max_amount}; Currency: {currency}")


            job_data.append({
                "id": job_id,
                "site": "vietnamworks",
                "job_url": job_url,
                "job_url_direct": NA,
                "title": title,
                # company
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
                # job description
                # more company info
            })
        
        print("Extraction Completed")
        print("Saving data to CSV")
        data_frame = pd.DataFrame(job_data)

        for col in data_frame.columns:
            if data_frame[col].dtype == "object":
                data_frame[col] = data_frame[col].apply(
                    lambda x: x.replace("\n", " ").replace("\r", " ") if isinstance(x, str) else x
                )

        data_frame.to_csv(f"data/vietnamworks_vn_{keyword}.csv", index=False, quotechar='"', escapechar='\\', encoding='utf-8-sig')
        print("Data saved to CSV")


# TODO: Learn how to translate (for multiple request a day)
# Run the Function
if __name__ == "__main__":
    # User Input
    print("| = | = | = | Vietnam Works Web Scraper | = | = | = |")
    keyword = input("Keyword (e.g., data analyst): ").strip().replace(" ", "-")
    page_number = int(input("Number of pages you want to scrape: "))

    # Proper Run
    asyncio.run(web_scraper(keyword, page_number))