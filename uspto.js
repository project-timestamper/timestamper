import puppeteer from 'puppeteer-extra'
import StealthPlugin from 'puppeteer-extra-plugin-stealth'
puppeteer.use(StealthPlugin())

const sleep = async (ms) => {
  return new Promise(resolve => setTimeout(resolve, ms))
}

const setupBrowser = async () => {
  const browser = await puppeteer.launch({
    headless: false,
    executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
  })
  const page = await browser.newPage()
  await page.setViewport({ width: 1920, height: 1080 })
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
  return { browser, page }
}

var page1 = null;

const clickLinkWithText = async (page, text) => {
  await page.evaluate((text) => {
    const links = Array.from(document.querySelectorAll('a'))
    const link = links.find(a => a.innerText.trim() === text)
    if (link) {
      link.scrollIntoView({ behavior: 'smooth', block: 'center' })
      window.setTimeout(() => link.click(), 3000)
      return true
    }
    return false
  }, text)
}

export const run = async () => {
  const { page } = await setupBrowser()
  page1 = page
  await page.goto('https://data.uspto.gov/bulkdata/datasets/', {
    waitUntil: 'networkidle2',
    timeout: 60000
  })
  await sleep(3000)
  await clickLinkWithText(page, 'Patent Grant Multi-page PDF Images')
  await sleep(2000) // Wait for navigation after click
}

export const gotoPage = async (page, url) => {
  await page.goto(url, {
    waitUntil: 'networkidle2',
    timeout: 60000
  })
  // Wait a bit more for any JavaScript to finish executing
  await sleep(2000)
}run()