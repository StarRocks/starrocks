const fs = require('node:fs');
const readline = require('node:readline');
const axios = require('axios');
const cheerio = require('cheerio');

async function getH1(url) {
  try {
    const response = await axios.get(url);
    const html = response.data;

    const $ = cheerio.load(html);
    return $('h1').text();

  } catch (error) {
    console.error('Error:', error);
  }
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

async function requestPage(url) {
  const fileName = (String(i).padStart(4, '0')).concat('.', 'pdf');

  // Get the details to write the YAML file
  // We need title and filename
    const pageTitle = await getH1(url);
    const cleanedTitle = pageTitle.replaceAll('\[', '').replaceAll('\]', '').replaceAll(':', '').replaceAll(' | StarRocks', '')
  const pageDetails = `    - file: ${fileName}\n      title: ${cleanedTitle}\n`;

  fs.appendFile('./combine.yaml', pageDetails, err => {
    if (err) {
      console.error(err);
    } else {
      console.log(`Title is ${pageTitle}`);
      console.log(`Filename is ` + fileName );
      // file written successfully
    }
  });

  i++;

}

// Example usage:
var i = 0;
processLineByLine();

