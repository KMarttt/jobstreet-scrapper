from playwright.async_api import async_playwright
import asyncio


async def test_xpath():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        await page.goto("https://www.jobnet.com.mm/job/lead-senior-business-analyst-dai-ichi-life-insurance-myanmar-ltd/116107", wait_until="domcontentloaded", timeout=40000)
        # await page.goto("https://careerviet.vn/en/search-job/security-engineer-red-team-khoi-cong-nghe-thong-tin-ho25-294.35C4F0F1.html")


        # button_locator = page.locator("//div[@class='box-text more-less']/div[@class='view-style']/a[@class='read-more']")

        # while await button_locator.is_visible():
        #     await button_locator.click()
        #     # Small wait for the section to expand
        #     await page.wait_for_timeout(500)

        # Replace with your XPath
        # locator = page.locator("//h2[@id='cp_company_name']/following-sibling::ul/li[1]")
        # locator = page.locator("//h2[@id='cp_company_name']/following-sibling::ul/li[span[contains(text(),'Company size:')]]/span[contains(text(),'Company size:')]")
        locator = page.locator("//div[@class='job-details__card-footer']/div/div[2]/i[@class='fa fa-calendar-check-o']/following-sibling::span").first
        count = await locator.count()
        print(f"Number of matching elements: {count}")
        print(f"Locator Value: {await (locator).text_content()}")
        # print(f"Locator value: {locator}")
        print(f"Value: {await locator.text_content()}")

        if count > 0:
            print("✅ XPath works!")
        else:
            print("❌ XPath did NOT match anything.")

        # button_locator = page.locator("//div[@class='box-text more-less']/div[@class='view-style']/a[@class='read-more']")

        # while await button_locator.is_visible():
        #     await button_locator.click()
        #     # Small wait for the section to expand
        #     await page.wait_for_timeout(500)

        await asyncio.sleep(2)

        await browser.close()

asyncio.run(test_xpath())
