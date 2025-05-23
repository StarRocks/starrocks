FROM node:22.14-bookworm

WORKDIR /app
ENV NODE_OPTIONS="--max-old-space-size=8192"

RUN apt update && apt install -y neovim rsync
RUN apt-get update && apt-get install curl gnupg -y

EXPOSE 3000

