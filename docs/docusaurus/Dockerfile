FROM node:21

WORKDIR /app/docusaurus
ENV NODE_OPTIONS="--max-old-space-size=8192"

RUN apt update && apt install -y neovim python3.11-venv ghostscript

EXPOSE 3000

COPY . .

RUN yarn install

