# A Job Street Scraper
from playwright.async_api import async_playwright
import asyncio
import pandas as pd


async def web_scraper(portal, location, keyword, page_number):
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

            job_link = f"https://{portal}.jobstreet.com{link}" 

            await page.goto(job_link)

            job_position = (await page.locator("h1[data-automation='job-detail-title']").text_content()).strip()

            company_name = (await page.locator("span[data-automation='advertiser-name']").text_content()).strip()


            location = (await page.locator("span[data-automation='job-detail-location']").text_content()).strip()
            department = (await page.locator("span[data-automation='job-detail-classifications']").text_content()).strip()
            employment_type = (await page.locator("span[data-automation='job-detail-work-type']").text_content()).strip()
            salary_locator = page.locator("span[data-automation='job-detail-salary']")

            if await salary_locator.count() > 0:
                salary_range = (await salary_locator.text_content()).strip()
            else:
                salary_range = "Not specified"

            job_description = (await page.locator("div._1lns5ab0.sye2ly0").text_content()).strip()

            job_data.append({
                "position": job_position,
                "company": company_name,
                "location": location,
                "department": department,
                "employment_type": employment_type,
                "salary_range": salary_range,
                "job_description_raw": job_description,
                "job_link": job_link
            })

            # print(job_position)
            # print(company_name)
            # print(location)
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


        data_frame.to_csv("jobstreet_jobs.csv", index=False, quotechar='"', escapechar='\\', encoding='utf-8-sig')
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
    portal = input("Please choose a JobStreet Portal: ").lower().strip()
    location = input("Please input a Location: ").strip()
    keyword = input("Please input a Job Position: ").strip().replace(" ", "-")
    page_number = int(input("Kindly input the number of pages you want to scrape: "))

    asyncio.run(web_scraper(portal, location, keyword, page_number))
