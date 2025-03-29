cd /docs/translation/
rm -rf configs
cp -r zh-to-en-configs configs
gpt_translate.files --input_file ./files.txt --config_folder ./configs --language en
