cd /docs/translation/
gpt_translate.files --input_file ./files.txt --config_folder ./configs
sed "s#^../en#../zh#g" ./files.txt > ./new_files.txt

echo "Fixing the frontmatter for the displayed sidebar"
while IFS="" read -r english || [ -n "$english" ];
  do sed -i'' '/displayed_sidebar:/s/English/Chinese/' "$english";
done < ./new_files.txt

echo "Removing new_files.txt"
rm ./new_files.txt
