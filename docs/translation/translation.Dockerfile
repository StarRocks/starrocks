FROM python:3.11.9-slim-bookworm

WORKDIR /app

RUN apt update && apt install -y neovim

RUN python -m pip install --upgrade pip
RUN python -m pip install gpt_translate

