# A Job Street Scraper
from playwright.async_api import async_playwright
import asyncio
import pandas as pd
from numpy import nan
from datetime import datetime, timedelta
import re

async def parse_date_posted(page, selector):
    date_text = (await page.locator(selector).first.text_content()).strip()

    current_date = datetime.now()
    relative_date_text = date_text.replace("Posted", "").replace("ago", "").strip()

    if "d" in relative_date_text: # if the date is in days
        days = int(relative_date_text.replace("d", "").strip())
        return (current_date - timedelta(days=days)).strftime(r"%Y-%m-%d")
    elif "h" in relative_date_text: # If the date is in hour(s) ago
        return current_date.strftime(r"%Y-%m-%d")
    else: # If the date is in minute(s) or second(s)
        return current_date.strftime(r"%Y-%m-%d")


async def web_scraper(portal="ph", location="Thailand", keyword="Data-Analyst", page_number=1):
    # Phase 1: Initiate
    print("Initiating JobStreet Scraper")
    print(f"{portal} {location} {keyword} {page_number}")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=False,
        )

        # Configuring the browser to be Incognito (to have clean cookies, cache, etc.)
        context = await browser.new_context()


        page = await context.new_page()
        job_links = []
        job_data = []

        # # Block unnecessary resources
        # async def route_handler(route, request):
        #     if request.resource_type in ["image", "stylesheet", "font"]:
        #         await route.abort()
        #     else:
        #         await route.continue_()
        
        # await context.route("**/*", route_handler)

        # , wait_until="networkidle"

        # Phase 2: Extract all job links
        print("Extracting Job Links")
        for i in range(1, page_number + 1):
            print(f"https://{portal}.jobstreet.com/{keyword}-jobs/in-{location}?page={i}")
            await page.goto(f"https://{portal}.jobstreet.com/{keyword}-jobs/in-{location}?page={i}")

            # Locator is not awaitable, you can only await locator with methods (e.g., .text_content)
            card_links = page.locator("article div._1lns5ab0._6c7qzn50._6c7qzn4y a")
            
            # Locator has .count() instead of len()
            link_count = await card_links.count()

            for link in range(link_count):

                # print(await card_links.nth(link).get_attribute('href'))
                # Locator has .nth() instead of the []
                job_links.append( await card_links.nth(link).get_attribute('href'))

        print(f"\nJob Links: {job_links}")
        print(f"Number of Job links collected: {len(job_links)}")

        # Phase 3: Extract Job Details
        print("\nExtracting Job Details")
        for link in job_links:

            job_id = link.split("/job/")[1].split("?")[0]
            site = "jobstreet"
            job_url = f"https://{portal}.jobstreet.com{link}" 
            job_url_direct = nan

            await page.goto(job_url)

            title = (await page.locator("h1[data-automation='job-detail-title']").text_content()).strip()

            company = (await page.locator("span[data-automation='advertiser-name']").text_content()).strip()
            location = (await page.locator("span[data-automation='job-detail-location']").text_content()).strip()
            date_posted = await parse_date_posted(page, "xpath=(//span[contains(text(),'Posted')])[1]")
            


            
            
            job_function = (await page.locator("span[data-automation='job-detail-classifications']").text_content()).strip()
            employment_type = (await page.locator("span[data-automation='job-detail-work-type']").text_content()).strip()
            salary_locator = page.locator("span[data-automation='job-detail-salary']")

            if await salary_locator.count() > 0:
                salary_range = (await salary_locator.text_content()).strip()
            else:
                salary_range = nan

            job_description = (await page.locator("div._1lns5ab0.sye2ly0").text_content()).strip()

            job_data.append({
                "title": title,
                "company": company,
                "location": location,
                "date_posted": date_posted,
                "job_function": job_function,
                "employment_type": employment_type,
                "salary_range": salary_range,
                "job_description_raw": job_description,
                "job_link": job_url
            })

            # print(job_position)
            # print(company_name)
            # print(location)
            # print(date_posted)
            # print(department)
            # print(employment_type)
            # print(salary_range)
            # print(job_description)
        
        print("Extraction Completed")
        print("Saving data to CSV")
        data_frame = pd.DataFrame(job_data)

        for col in data_frame.columns:
            if data_frame[col].dtype == "object":
                data_frame[col] = data_frame[col].str.replace("\n", " ", regex=False).str.replace("\r", " ", regex=False)


        data_frame.to_csv("data/jobstreet_jobs.csv", index=False, quotechar='"', escapechar='\\', encoding='utf-8-sig')
        print("Data saved to CSV")

# Run the Function

if __name__ == "__main__":

    # User Input
    print("| = | = | = | JobStreet Web Scraper | = | = | = |")
    print("List of available Portal")
    print("my = Malaysia")
    print("sg = Singapore")
    print("ph = Philippines")
    print("id = Indonesia")
    portal = input("Choose a JobStreet Portal: ").lower().strip()
    location = input("Location: ").strip()
    keyword = input("Job Position: ").strip().replace(" ", "-")
    page_number = int(input("Number of pages you want to scrape: "))

    # Test Run
    asyncio.run(web_scraper())

    # Proper Run
    # asyncio.run(web_scraper(portal, location, keyword, page_number))
