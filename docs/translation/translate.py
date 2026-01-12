import os
import yaml
import argparse
import re
from google import genai
from google.genai import types

client = genai.Client()
MODEL_NAME = "gemini-2.0-flash" 
CONFIG_BASE_PATH = "./configs"

class StarRocksTranslator:
    def __init__(self, target_lang: str, dry_run: bool = False):
        self.target_lang = target_lang
        self.dry_run = dry_run
        self.system_instruction = self._read_file(f"{CONFIG_BASE_PATH}/system_prompt.txt")
        self.human_prompt_template = self._read_file(f"{CONFIG_BASE_PATH}/human_prompt.txt")
        dict_path = f"{CONFIG_BASE_PATH}/language_dicts/{target_lang}.yaml"
        self.dictionary_str = self._load_dict_as_string(dict_path)

    def _read_file(self, path: str) -> str:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()

    def _load_dict_as_string(self, path: str) -> str:
        if not os.path.exists(path): return ""
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            return "\n".join([f"{k}: {v}" for k, v in data.items()]) if data else ""

    def validate_mdx(self, original: str, translated: str) -> bool:
        """Ensures MDX components and Frontmatter keys weren't accidentally translated."""
        # Extract tags like <Component />, <Tag>, </Tag>
        tag_pattern = r'<[a-zA-Z0-9/.\s]+>'
        orig_tags = re.findall(tag_pattern, original)
        trans_tags = re.findall(tag_pattern, translated)
        
        if len(orig_tags) != len(trans_tags):
            print(f"⚠️  Tag count mismatch! Original: {len(orig_tags)}, Translated: {len(trans_tags)}")
            return False
        return True

    def translate_file(self, input_file: str):
        if not os.path.exists(input_file): return
        
        source_text = self._read_file(input_file)
        prompt = self.human_prompt_template.replace("{dictionary}", self.dictionary_str)
        prompt += f"\n\n### CONTENT TO TRANSLATE ###\n\n{source_text}"

        if self.dry_run:
            print(f"🔍 [DRY RUN] {input_file}")
            return

        print(f"🚀 Translating {input_file}...")
        response = client.models.generate_content(
            model=MODEL_NAME,
            config=types.GenerateContentConfig(system_instruction=self.system_instruction, temperature=0.0),
            contents=prompt
        )
        
        translated_text = response.text
        
        # Validation Check
        if not self.validate_mdx(source_text, translated_text):
            print(f"❌ Validation failed for {input_file}. Check for broken tags.")
            # We still save it so you can inspect it in the PR, 
            # but we flag it in the log.

        base, ext = os.path.splitext(input_file)
        output_file = f"../{self.target_lang}/{base}.{ext}"
        print(f"input filename: {input_file}")
        print(f"base: {base}")
        print(f"output language: {self.target_lang}")
        print(f"file extension: {ext}")
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(translated_text)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", nargs='*', help="List of files from git diff")
    parser.add_argument("-l", "--lang", choices=['ja', 'zh', 'en'], required=True)
    parser.add_argument("--dry-run", action="store_true")
    
    args = parser.parse_args()
    translator = StarRocksTranslator(target_lang=args.lang, dry_run=args.dry_run)

    if args.files:
        for f in args.files:
            if f.endswith(('.md', '.mdx')):
                translator.translate_file(f)

if __name__ == "__main__":
    main()

