'use strict';
require('dotenv').config()
const fs = require('node:fs');
const readline = require('node:readline');
const puppeteer = require('puppeteer');
const scrollToBottom = require('scroll-to-bottomjs');
const PDFDocument = require('pdfkit');

function coverPage() {
    // Create a document
    const doc = new PDFDocument({size: 'A4'});

    doc.pipe(fs.createWriteStream('0000.pdf'));
    
    doc.image(process.env.COVER_IMAGE, {
            fit: [200, 200],
            align: 'center',
            valign: 'center'
          });

    doc
        .fontSize(25)
        .fillColor("black")
          // position text over 70 and down 150
        .text(process.env.COVER_TITLE, 70, 300)
        .fontSize(11)
        .text(process.env.COPYRIGHT, 70, 650);
    
    // Finalize PDF file
    doc.end();

}

// cover page is 0.pdf, so start `i` at 1
var i = 1;

async function requestPage(url) {
  const browser = await puppeteer.launch({
    headless: 'shell',
    userDataDir: './tmp',
    args: [
        '--disable-features=IsolateOrigins',
        '--disable-site-isolation-trials',
        '--autoplay-policy=user-gesture-required',
        '--disable-background-networking',
        '--disable-background-timer-throttling',
        '--disable-backgrounding-occluded-windows',
        '--disable-breakpad',
        '--disable-client-side-phishing-detection',
        '--disable-component-update',
        '--disable-default-apps',
        '--disable-dev-shm-usage',
        '--disable-domain-reliability',
        '--disable-extensions',
        '--disable-features=AudioServiceOutOfProcess',
        '--disable-hang-monitor',
        '--disable-ipc-flooding-protection',
        '--disable-notifications',
        '--disable-offer-store-unmasked-wallet-cards',
        '--disable-popup-blocking',
        '--disable-print-preview',
        '--disable-prompt-on-repost',
        '--disable-renderer-backgrounding',
        '--disable-setuid-sandbox',
        '--disable-speech-api',
        '--disable-sync',
        '--hide-scrollbars',
        '--ignore-gpu-blacklist',
        '--metrics-recording-only',
        '--mute-audio',
        '--no-default-browser-check',
        '--no-first-run',
        '--no-pings',
        '--no-sandbox',
        '--no-zygote',
        '--password-store=basic',
        '--use-gl=swiftshader',
        '--use-mock-keychain']
  });
  const fileName = (String(i).padStart(4, '0')).concat('.', 'pdf');
  const page = await browser.newPage();

  await page.goto(url, { waitUntil: 'domcontentloaded', });
  await page.evaluate(scrollToBottom);

  await page.pdf({
    outline: true,
    path: fileName,
    format: 'A4',
    margin: {
      top: '0.5in',
      bottom: '0.5in',
      left: '0.5in',
      right: '0.5in',
    },
  });

  // Get the details to write the YAML file
  // We need title and filename
    const pageTitle = await page.title();
    const cleanedTitle = pageTitle.replaceAll('\[', '').replaceAll('\]', '').replaceAll(':', '').replaceAll(' | StarRocks', '').replaceAll(' | CelerData', '')
  const pageDetails = `    - file: ${fileName}\n      title: ${cleanedTitle}\n`;

  console.log(`Title is ${cleanedTitle}`);
  console.log(`Filename is ` + fileName );
  i++;

  await browser.close();
}

async function processLineByLine() {
  const fileStream = fs.createReadStream('URLs.txt');

  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    // Each line in input.txt will be successively available here as `line`.
    console.log(`URL: ${line}`);
    await requestPage(line).then(resp => {
    console.log(`done.\n`);
  }).catch(err => {
    console.log(err);
  });
  }
}

coverPage();
processLineByLine();
