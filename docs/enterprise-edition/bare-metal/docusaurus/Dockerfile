FROM node:21-bookworm

WORKDIR /app/docusaurus
ENV NODE_OPTIONS="--max-old-space-size=4096"
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true

RUN apt update && apt install -y neovim rsync
RUN apt-get update && apt-get install curl gnupg -y \
  && curl --location --silent https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
  && sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list' \
  && apt-get update \
  && apt-get install google-chrome-stable -y --no-install-recommends \
  && rm -rf /var/lib/apt/lists/*

EXPOSE 3000

COPY . .

RUN yarn install
RUN npx update-browserslist-db@latest
