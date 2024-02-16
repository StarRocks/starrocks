FROM node:18.18.2

WORKDIR /app/docusaurus
ENV NODE_OPTIONS="--max-old-space-size=4096"

RUN apt update && apt install -y neovim

EXPOSE 3000

COPY . .
RUN mv docusaurus.config.js original
RUN awk '/^  ],/{flag=""} /plugins/{flag=1;print;next} flag{next} 1' original > step1.config.js
RUN awk '/\/\/ end only for production/{flag=""} /\/\/ only for production/{flag=1;print;next} flag{next} 1' step1.config.js > docusaurus.config.js

RUN yarn install

