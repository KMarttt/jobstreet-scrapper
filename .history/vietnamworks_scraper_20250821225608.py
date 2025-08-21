# A Vietnam Works Scraper
from playwright.async_api import async_playwright
import asyncio
import pandas as pd
from pandas import NA
from datetime import datetime, timedelta
import re

async def parse_text_content(page, selector):
    # Check if the element exists
    locator = page.locator(selector)
    if await locator.count() > 0:
        return (await locator.text_content()).strip()
    else:
        return NA


async def parse_location(page, selector):
    # Get the job location(s)
    location_div = page.locator(selector)

    # Count the number of location_div
    location_count = await location_div.count()

    if location_count > 0:
        # Insert location(s) into a list
        locations = []
        for i in range(location_count):
            location_text = (await location_div.nth(i).locator("p").text_content()).strip()
            locations.append(location_text)
    else:
        return NA
    
    return locations

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

async def parse_salary(page, currency_values):
    salary_text = (await page.locator(
        "//h1[@name='title']/parent::div/parent::div/following-sibling::div[1]/div/span"
    ).text_content()).strip().lower()

    if salary_text != "negotiable" and salary_text != "competitive":
        salary_source = "direct_data"
        # Select the salary value (with units)
        numbers = re.findall(
            r"\d+(?:,\d+)*(?:\.\d+)?\s*(?:million|mil|m|k|thousand|thousands)?\b",
            salary_text,
            re.IGNORECASE
        )

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
        print(f"Salary: {salary_text}")
        return NA, NA, NA, NA, NA

    return salary_source, interval, min_amount, max_amount, currency 

async def parse_other_job_data(page):
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
        
        # Go to the company page
        await page.goto(company_url)

        # Locate the "Read more" button
        read_more_button = page.locator(
            "//h2[contains(., 'About Us')]/following-sibling::div[1]/div/span[contains(., 'Read more')]")
        
        # Click until the button is clicked 
        while await read_more_button.count() > 0:
            
            await read_more_button.click(force=True)
            
            # Small wait for the section to expand
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

        print(company_url)  

    else:
        return NA, NA, NA, NA, NA, NA, NA
    
    return company, company_industry, company_url, company_logo, company_addresses, company_num_emp, company_description

async def web_scraper(keyword="data-analyst", max_pages=2):
    # === Phase 1: Initiate
    print("Initiating Vietnam Works Scraper")
    print(f"{keyword} {max_pages}")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)

        # Configure the browser to be Incognito (to have clean cookies, cache, etc.)
        context = await browser.new_context()
        # Open new page
        page = await context.new_page()

        # === Phase 2: Extract all job links

        # Initialize variables
        job_links = []
        seen_links = set() # To avoid duplicate job links

        # Extract job links
        print("Extracting Job Links")
        current_page = 1
        consecutive_empty_pages = 0
        max_consecutive_empty = 3 # Stop if we encounter 3 consecutive empty pages

        while current_page <= max_pages and consecutive_empty_pages < max_consecutive_empty:
            url = f"https://www.vietnamworks.com/jobs?q={keyword}&page={current_page}&sorting=relevant"
            print(f"Scraping page {current_page}: {url}")

            try:
                await page.goto(url, timeout = 30000) # 30 second timeouut

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
                
                card_links = page.locator(
                    "a.img_job_card"
                )

                # Get the count of job links on current page
                link_count = await card_links.count()

                if link_count == 0:
                    consecutive_empty_pages += 1
                    print(
                        f"No job links found on page {current_page}. Empty pages count: {consecutive_empty_pages}"
                    )
                
                    # Check if we've reached the end 
                    no_result_indicator = page.locator(
                        "//div[@class='noResultWrapper animated fadeIn']/div/h2[contains(., 'We have not found jobs for this search at the moment')]"
                    )

                    page_reached_end = False
                    if await no_result_indicator.count() > 0:
                        consecutive_empty_pages += 1
                        print(
                            f"End of results detected on page {current_page}"
                        )
                        page_reached_end = True
                        # break -- i think we can delete this

                    if page_reached_end:
                        print("Reached end of job listings")
                        break
                else:
                    consecutive_empty_pages = 0 # Reset counter if we found jobs
                    page_job_links = []

                    for link_index in range(link_count):
                        link_href = await card_links.nth(link_index).get_attribute("href")
                        if link_href and link_href not in seen_links:
                            job_links.append(link_href)
                            seen_links.add(link_href)
                            page_job_links.append(link_href)

                    print(
                        f"Found {len(page_job_links)} new job links on page {current_page}"
                    )

            except Exception as e:
                print(f"Error scraping page {current_page}: {str(e)}")
                consecutive_empty_pages += 1
            
            current_page += 1
                
        print(
            f"\nTotal Job Links collected: {len(job_links)} from {current_page - 1} pages"
        )
        print(f"Unique Job Links collected: {len(seen_links)}")

        if not job_links:
            print("No job links found")
            await browser.close()
            return

        # === Phase 3: Extract Job Details
        
        # Initialize currency values
        currency_values = {
            "đ": "VND",
            "₫": "VND",
            "VND": "VND",
            "$": "USD",
            "USD": "USD"
        }

        # Initialize job list
        job_data = []

        # Extract job details
        print("\nExtracting Job Details")
        for link in job_links:   
            job_id = re.search(r"-(\d+)-jd", link).group(1)
            job_url = f"https://www.vietnamworks.com/{link}"
            print(job_url)
            await page.goto(job_url, wait_until="domcontentloaded")
            
            # Initiatialize list of clickable buttons essential for extracting job details
            site_buttons = []
            
            # Add buttons to the list
            site_buttons.append(
                page.locator(
                    "//h2[contains(., 'Job Information')]/following-sibling::div[last()]/div[1]//button[contains(., 'View more')]"
                    )
                )
            
            site_buttons.append(page.locator(
                "//button[contains(., 'View full job description')]"))

            # Click until the button is clicked 
            for button in site_buttons:
                while await button.is_visible():
                    await button.hover()
                    
                    # Click (force to ensure it's triggered)
                    await button.click(force=True)
                    
                    # Small wait for the section to expand
                    await page.wait_for_timeout(500)

            title = await parse_text_content(
                page, 
                "h1[name='title']")
            
            location = await parse_location(
                page,
                "//h2[contains(., 'Job Locations')]/following-sibling::div[1]/div"
            )
            
            date_posted = await parse_date_posted(
                page,
                "//label[contains(., 'POSTED DATE')]/following-sibling::p[1]"
            )
            
            job_type = await parse_text_content(
                page, 
                "//label[contains(., 'WORKING TYPE')]/following-sibling::p[1]"
            )

            salary_source, interval, min_amount, max_amount, currency = await parse_salary(page, currency_values)
            
            job_level = await parse_text_content(
                page, 
                "//label[contains(., 'JOB LEVEL')]/following-sibling::p[1]"
            )

            job_function = await parse_text_content(
                page, 
                "//label[contains(., 'JOB FUNCTION')]/following-sibling::p[1]"
            )
            
            year_of_experience, education_level, age_preference, skill, preferred_language, nationality = await parse_other_job_data(page)
            
            description = await parse_text_content(
                page, 
                "//h2[contains(., 'Job description')]/parent::div"
            )

            requirement = await parse_text_content(
                page, 
                "//h2[contains(., 'Job requirements')]/parent::div"
            )

            company, company_industry, company_url, company_logo, company_addresses, company_num_emp, company_description = await parse_company_info(page)
            
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
            })
        
        print("Extraction Completed")
        print("Saving data to CSV")
        data_frame = pd.DataFrame(job_data)

        for col in data_frame.columns:
            if data_frame[col].dtype == "object":
                data_frame[col] = data_frame[col].apply(
                    lambda x: x.replace("\n", " ").replace("\r", " ") if isinstance(x, str) else x
                )

        data_frame.to_csv(
            f"data/vietnamworks_vn_{keyword}.csv",
            index=False,
            quotechar='"',
            escapechar='\\',
            encoding='utf-8-sig'
        )

        print("Data saved to CSV")

# Run the Function
if __name__ == "__main__":
    # User Input
    print("| = | = | = | Vietnam Works Web Scraper | = | = | = |")
    keyword = input("Keyword (e.g., data analyst): ").strip().replace(" ", "-")
    # page_number = int(input("Number of pages you want to scrape: "))
    max_pages = int(input("Maximum pages to scrape (default 50): ") or "50")

    # # Proper Run
    # asyncio.run(web_scraper(keyword, max_pages))

    # Test Run
    asyncio.run(web_scraper())