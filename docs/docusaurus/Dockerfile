FROM node:18.18.2

WORKDIR /app/docusaurus
ENV NODE_OPTIONS="--max-old-space-size=4096"

RUN apt update && apt install -y neovim

EXPOSE 3000

COPY . .

RUN yarn install

