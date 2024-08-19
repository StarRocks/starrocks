cd /docs/translation/
gpt_translate.files --input_file ./files.txt --config_folder ./configs
sed "s#docs/en#docs/zh#g" ./files.txt > ./new_files.txt
while IFS="" read -r english || [ -n "$english" ];
  do sed -i'' '/displayed_sidebar:/s/English/Chinese/' "$english";
done < ./new_files.txt
