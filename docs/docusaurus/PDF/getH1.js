const fs = require('node:fs');
const readline = require('node:readline');
const axios = require('axios');
const cheerio = require('cheerio');

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function getH1(url) {
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

async function callGotenberg(url, fileName) {
    //var util = require('util');
    var exec = require('child_process').exec;

    var command = `curl --request POST http://gotenberg:3000/forms/chromium/convert/url --form url=${url} -o ${fileName}`

    // curl --request POST \
    // http://gotenberg:3000/forms/chromium/convert/url \
    // --form url=$line -o ${padded}.pdf

    child = exec(command, function(error, stdout, stderr){

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
  const fileName = 'PDFoutput/'.concat(String(i).padStart(4, '0')).concat('.', 'pdf');

  // Get the details to write the YAML file
  // We need title and filename
    const pageTitle = await getH1(url);
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

  // shelling out to run curl with no delay causes Gotenberg
  // to fail
  await sleep(2000);
  i++;

}

const yamlHeader = 'files:\n';

fs.writeFile('./combine.yaml', yamlHeader, err => {
  if (err) {
    console.error(err);
  } else {
    // file written successfully
  }
});

var i = 0;

processLineByLine();

