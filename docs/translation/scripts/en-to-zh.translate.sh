cd /docs/translation/
rm -rf configs
cp -r en-to-zh-configs configs
gpt_translate.files --input_file ./files.txt --config_folder ./configs --language zh
