# A Vietnam Works Scraper
from playwright.async_api import async_playwright
import asyncio
import pandas as pd
from pandas import NA
from datetime import datetime
import re


async def parse_date_posted(page):
    date_string = (await page.locator("//label[contains(., 'NGÀY ĐĂNG')]/following-sibling::p[1]").text_content()).strip()

    # Parse the date string
    date_object = datetime.strptime(date_string, "%d/%m/%Y")

    # return the date as "YYYY-MM-DD"
    return date_object.strftime("%Y-%m-%d")

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
            await page.goto(f"https://www.vietnamworks.com/viec-lam?q={keyword}&page={i}&sorting=relevant")

            job_item_links = page.locator("a.img_job_card")

            link_count = await job_item_links.count()

            for link in range(link_count):
                print(await job_item_links.nth(link).get_attribute('href'))
                job_links.append(await job_item_links.nth(link).get_attribute('href'))
        
        print(job_links)
        print(f"# of job links: {len(job_links)}")


        # Phase 3: Extract Job Details
        print("\nExtracting Job Details")
        for link in job_links:
            
            job_id = link.split("-jv?")[0].split("-")[-1]
            job_url = f"https://www.vietnamworks.com/{link}"
            print(job_url)
            await page.goto(job_url, wait_until="domcontentloaded")
            
            # Click "Xem thêm" to see more and expand the section
            see_more_button = page.locator("//h2[contains(., 'Thông tin việc làm')]/following-sibling::div[last()]/div[1]//button[contains(., 'Xem thêm')]")
            await see_more_button.scroll_into_view_if_needed()

            # Try clicking until the button is clicked (idk why sometimes it's not being clicked)
            while await see_more_button.count() > 0:
                await see_more_button.hover()
                
                # Click (force to ensure it's triggered)
                await see_more_button.click(force=True)
                
                # Small wait for the section to expand
                await page.wait_for_timeout(500)

            title = (await page.locator("h1[name='title']").text_content()).strip()
            # company
            location = (await page.locator("//h2[contains(., 'Địa điểm làm việc')]/following-sibling::div[1]/div/div/p").text_content()).strip()
            date_posted = await parse_date_posted(page)
            job_type = (await page.locator("//label[contains(., 'LOẠI HÌNH LÀM VIỆC')]/following-sibling::p[1]").text_content()).strip()


            # print(f"Job ID: {job_id}")
            print(f"Title: {title}")
            print(f"Location: {location}")
            print(f"Date Posted: {date_posted}")
            print(f"Job Type: f{job_type}")

        print("Extraction complete")

# TODO: Learn how to translate (for multiple request a day)
# Run the Function
if __name__ == "__main__":
    # User Input
    print("| = | = | = | Vietnam Works Web Scraper | = | = | = |")
    keyword = input("Keyword (e.g., data analyst): ").strip().replace(" ", "-")
    page_number = int(input("Number of pages you want to scrape: "))

    # Proper Run
    asyncio.run(web_scraper(keyword, page_number))