#! /usr/bin/bash
cd /app/temp/PDF_DIR
cp -r /app/temp/config/PDF/* .
npx --yes docusaurus-prince-pdf --list-only \
  --file URLs.txt \
  -u ${START_URL}
yarn install
node docusaurus-puppeteer-pdf.js
pdftk 0*pdf output ${FILENAME}.pdf


