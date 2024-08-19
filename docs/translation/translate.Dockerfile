FROM python:3.11.9-slim-bookworm

WORKDIR /app

RUN apt update && apt install -y neovim

RUN python -m pip install --upgrade pip
RUN python -m pip install gpt_translate

# cd into translation/ folder
# cd translation

# create the Chinese list from the English list:
# sed "s#^../en/#../zh/#g" ./files.txt > ./new_files.txt

# cp -r docs/translation/configs . # ugh, only works if configs is in cwd

# gpt_translate.files \
#   --input_file ./files.txt \
#   --config_folder ./configs

# Fix the sidebar metadata:
# cat ./.github/outputs/new_files.txt
# while IFS="" read -r english || [ -n "$english" ]
#   do
#       sed -i'' '/displayed_sidebar:/s/English/Chinese/' "$english"
#   done < ./new_files.txt

