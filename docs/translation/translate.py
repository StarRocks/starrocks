import os
import yaml
import argparse
import re
from google import genai
from google.genai import types

# Initialize Gemini client with explicit error handling for missing API key.
if "GEMINI_API_KEY" not in os.environ:
    raise RuntimeError(
        "GEMINI_API_KEY environment variable is not set. "
        "Please set GEMINI_API_KEY to your Gemini API key before running this script."
    )

try:
    # Automatically picks up GEMINI_API_KEY from environment
    client = genai.Client()
except Exception as e:
    raise RuntimeError(
        "Failed to initialize Gemini client. "
        "Please verify that GEMINI_API_KEY is correctly set and that your environment "
        "is configured for the Google Gemini SDK."
    ) from e
MODEL_NAME = "gemini-2.0-flash" 
CONFIG_BASE_PATH = "./docs/translation/configs"

LANG_MAP = {
    "en": "English",
    "ja": "Japanese",
    "zh": "Simplified Chinese"
}

class StarRocksTranslator:
    def __init__(self, target_lang: str, dry_run: bool = False):
        self.target_lang = target_lang
        self.target_lang_full = LANG_MAP.get(target_lang, target_lang)
        self.dry_run = dry_run
        
        self.system_template = self._read_file(f"{CONFIG_BASE_PATH}/system_prompt.txt")
        self.human_template = self._read_file(f"{CONFIG_BASE_PATH}/human_prompt.txt")
        
        dict_path = f"{CONFIG_BASE_PATH}/language_dicts/{target_lang}.yaml"
        self.dictionary_str = self._load_dict_as_string(dict_path)

    def _read_file(self, path: str) -> str:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Config file not found: {path}") from e
        except OSError as e:
            raise RuntimeError(f"Error reading config file '{path}': {e}") from e

    def _load_dict_as_string(self, path: str) -> str:
        if not os.path.exists(path):
            return ""
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            return "\n".join([f"{k}: {v}" for k, v in data.items()]) if data else ""

    def validate_mdx(self, original: str, translated: str) -> bool:
        # Match MDX/JSX-style tags, including attributes, hyphen/underscore in names,
        # and optional self-closing slash with or without preceding whitespace.
        tag_pattern = r'<\s*/?\s*[A-Za-z_][A-Za-z0-9_.-]*\b[^<>]*?/?>'
        return len(re.findall(tag_pattern, original)) == len(re.findall(tag_pattern, translated))

    def translate_file(self, input_file: str):
        if not os.path.exists(input_file):
            print(f"Warning: Input file not found, skipping: {input_file}")
            return
        
        # 1. Detect Source Language
        source_lang = "en"
        if "docs/zh/" in input_file: source_lang = "zh"
        elif "docs/ja/" in input_file: source_lang = "ja"
        source_lang_full = LANG_MAP.get(source_lang, source_lang)

        # 2. Path Mapping
        abs_input = os.path.abspath(input_file)
        output_file = abs_input.replace(f"/docs/{source_lang}/", f"/docs/{self.target_lang}/")
        
        if os.path.exists(output_file) and os.path.getmtime(output_file) >= os.path.getmtime(abs_input):
            print(f"⏩ Skipping {output_file}: Target is up to date.")
            return

        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # 3. FIXED: Dynamic Prompt Injection (Using local variables)
        system_instruction = (self.system_template
                              .replace("${source_lang}", source_lang_full)
                              .replace("${target_lang}", self.target_lang_full)
                              .replace("${dictionary}", self.dictionary_str))
        
        # We don't modify self.human_template; we create a new string for THIS file
        current_human_prompt = (self.human_template 
                                + f"\n\n### CONTENT TO TRANSLATE ###\n\n{self._read_file(input_file)}")

        if self.dry_run:
            print(f"🔍 [DRY RUN] {source_lang_full} -> {self.target_lang_full}")
            return

        print(f"🚀 Translating {input_file} to {output_file}...")
        try:
            response = client.models.generate_content(
                model=MODEL_NAME,
                config=types.GenerateContentConfig(system_instruction=system_instruction, temperature=0.0),
                contents=current_human_prompt
            )
            
            # Access text inside the try block and handle empty/blocked responses
            if not response.text:
                print(f"⚠️ Warning: Gemini returned an empty response for {input_file}. This usually happens if content is blocked by safety filters.")
                return
                
            translated_text = response.text.strip()

        except Exception as e:
            error_message = str(e)
            print(f"❌ Gemini API request failed for {input_file}:")
            print(f"   Details: {error_message}")
            lower_msg = error_message.lower()
            if "429" in error_message or "rate" in lower_msg:
                print("   Possible cause: rate limit exceeded. Try reducing request frequency or batching files.")
            elif "quota" in lower_msg or "exceeded" in lower_msg:
                print("   Possible cause: quota exhausted. Check your Gemini project billing and quota settings.")
            elif "safety" in lower_msg or "blocked" in lower_msg:
                print("   Possible cause: content blocked by safety filters. Review the source content and Gemini safety settings.")
            elif "network" in lower_msg or "timeout" in lower_msg or "connection" in lower_msg:
                print("   Possible cause: network connectivity issue. Verify your internet connection and retry.")
            else:
                print("   The error could be due to configuration, authentication, or an unexpected server issue.")
            print("   If the problem persists, verify GEMINI_API_KEY is set correctly and that your environment can reach the Gemini API.")
            return
        translated_text = response.text.strip()
        
        # CLEANUP: Remove markdown code fences if Gemini added them
        if translated_text.startswith("```"):
            lines = translated_text.splitlines()
            if lines[0].startswith("```"): lines = lines[1:]
            if lines and lines[-1].startswith("```"): lines = lines[:-1]
            translated_text = "\n".join(lines).strip()

        if not self.validate_mdx(self._read_file(input_file), translated_text):
            print(f"❌ Validation warning for {input_file}: Tag mismatch detected.")

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(translated_text)
        print(f"✅ Saved: {output_file}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", nargs='*', help="Files to process")
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
