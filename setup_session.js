const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
puppeteer.use(StealthPlugin());
const path = require("path");
const fs = require("fs");


const KASPR_EXT = path.join(
  process.env.HOME,
  "Library/Application Support/net.imput.helium/Default/Extensions/kkfgenjfpmoegefcckjklfjieepogfhg/2.0.19_0"
);
const PROFILE_DIR = path.join(process.cwd(), "runtime", "chrome_profile");

async function main() {
  fs.mkdirSync(PROFILE_DIR, { recursive: true });
  console.log("Profile dir:", PROFILE_DIR);
  console.log("Extension:", KASPR_EXT);
  console.log("\nLaunching browser... Log into LinkedIn and Kaspr, then press Ctrl+C to save.\n");

  const browser = await puppeteer.launch({
    headless: false,
    args: [
      `--disable-extensions-except=${KASPR_EXT}`,
      `--load-extension=${KASPR_EXT}`,
      "--no-sandbox",
      "--disable-setuid-sandbox",
    ],
    userDataDir: PROFILE_DIR,
    defaultViewport: null,
  });

  const page = (await browser.pages())[0] || await browser.newPage();
  await page.goto("https://www.linkedin.com/in/williamhgates/", { waitUntil: "domcontentloaded" });
  console.log("The Kaspr widget should appear on the right side of the page.");
  console.log("Click 'Log in' in the Kaspr widget, complete the login, then press Ctrl+C.\n");

  // Keep alive until user presses Ctrl+C
  process.on("SIGINT", async () => {
    console.log("\nSaving session and closing browser...");
    await browser.close();
    console.log("Done! Session saved to", PROFILE_DIR);
    console.log("Future scrape runs will reuse this session automatically.");
    process.exit(0);
  });

  // Keep the process alive
  await new Promise(() => {});
}

main().catch(e => { console.error(e.message); process.exit(1); });
