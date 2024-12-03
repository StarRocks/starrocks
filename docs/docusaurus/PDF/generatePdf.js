const fs = require('node:fs');
const readline = require('node:readline');
const axios = require('axios');
const cheerio = require('cheerio');
const process = require('process');
const util = require('node:util');

async function getPageTitle(url) {
  try {
    const response = await axios.get(url);
    const html = response.data;

    const $ = cheerio.load(html);
    const h1Text = $('h1').text();
    if (h1Text !== "") { return h1Text; }
      else { return "blank"; }

  } catch (error) {
    console.error('Error:', error);
  }
}

function getUrls(url) {
    var execSync = require('child_process').execSync;

    // the URL that the user has is to `0.0.0.0` or `localhost`, 
    // which needs to be modified to the `docusaurus` service 
    // in the Docker compose environment
    let docusaurusUrl = 
        url.replace("localhost", "docusaurus").replace("0.0.0.0", "docusaurus");

    var command = `npx docusaurus-prince-pdf --list-only -u ${docusaurusUrl} --file URLs.txt`

    try {
        const {stdout, stderr} = execSync(command);
    } catch (error) {
        console.log(error);
    }
};

async function callGotenberg(docusaurusUrl, fileName) {
    //var util = require('util');
    var execSync = require('child_process').execSync;

    var command = `curl --request POST http://gotenberg:3000/forms/chromium/convert/url --form url=${docusaurusUrl} -o ${fileName}`

    child = execSync(command, function(error, stdout, stderr){

    //console.log('stdout: ' + stdout);
    console.log('stderr: ' + stderr);

    if(error !== null)
    {
        console.log('exec error: ' + error);
    }

    });
}

async function processLineByLine() {
  const fileStream = fs.createReadStream('URLs.txt');

  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    // Each line in input.txt will be successively available here as `line`.
    //console.log(`URL: ${line}`);
    await requestPage(line).then(resp => {
    //console.log(`done.\n`);
  }).catch(err => {
    console.log(err);
  });
  }
}

async function requestPage(url) {
  const fileName = '../../PDFoutput/'.concat(String(i).padStart(4, '0')).concat('.', 'pdf');

  // Get the details to write the YAML file
  // We need title and filename
    const pageTitle = await getPageTitle(url);
    const cleanedTitle = pageTitle.replaceAll('\[', '').replaceAll('\]', '').replaceAll(':', '').replaceAll(' | StarRocks', '')
  const pageDetails = `    - file: ${fileName}\n      title: ${cleanedTitle}\n`;

  fs.appendFile('./combine.yaml', pageDetails, err => {
    if (err) {
      console.error(err);
    } else {
      //console.log(`Title is ${pageTitle}`);
      //console.log(`Filename is ` + fileName );
      // file written successfully
    }
  });

  await callGotenberg(url, fileName);
  i++;

}




function main(ms) {
    // startingUrl is the URL for the first page of the docs
    // Get all of the URLs and write to URLs.txt
    getUrls(startingUrl);

    console.log(startingUrl);

    const yamlHeader = 'files:\n';

    fs.writeFile('./combine.yaml', yamlHeader, err => {
        if (err) {
            console.error(err);
        } else {
            // file written successfully
        }
    });

    processLineByLine();
};

var i = 0;
const startingUrl = process.argv[2];
main();
