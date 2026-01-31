import os
import sys
import yaml
import argparse
import re
import time
import collections 
from google import genai
from google.genai import types

# Initialize Gemini client with explicit error handling
if "GEMINI_API_KEY" not in os.environ:
    raise RuntimeError(
        "GEMINI_API_KEY environment variable is not set. "
        "Please set GEMINI_API_KEY to your Gemini API key before running this script."
    )

try:
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
except Exception as e:
    raise RuntimeError("Failed to initialize Gemini client.") from e

# Use gemini-2.0-flash for stability and speed
MODEL_NAME = "gemini-2.0-flash" 
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_BASE_PATH = os.path.join(SCRIPT_DIR, "configs")

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
        
        # Tracking for the final report
        self.has_errors = False
        self.successes = [] 
        self.failures = []  
        
        # 1. Load Templates
        self.system_template = self._read_file(f"{CONFIG_BASE_PATH}/system_prompt.txt")
        self.human_template = self._read_file(f"{CONFIG_BASE_PATH}/human_prompt.txt")
        
        # 2. Load Dictionary
        dict_path = f"{CONFIG_BASE_PATH}/language_dicts/{target_lang}.yaml"
        self.dictionary_str = self._load_dict_as_string(dict_path)

        # 3. Load Synonyms (for normalization)
        synonyms_path = f"{CONFIG_BASE_PATH}/synonyms.yaml"
        self.synonyms = self._load_yaml_as_dict(synonyms_path)
        
        # 4. Load "Never Translate" List
        raw_terms = self._load_yaml_as_list(f"{CONFIG_BASE_PATH}/never_translate.yaml")
        
        # A. Create the string for the System Prompt (Double Safety)
        self.never_translate_str = self._expand_terms(raw_terms)
        
        # B. Inject into Dictionary as "Identity Rules" (Leader: Leader)
        identity_rules = []
        for term in raw_terms:
            identity_rules.append(f"{term}: {term}")          
            identity_rules.append(f"{term.lower()}: {term.lower()}")
            
        if self.dictionary_str:
            self.dictionary_str += "\n" + "\n".join(identity_rules)
        else:
            self.dictionary_str = "\n".join(identity_rules)

    def _expand_terms(self, terms: list) -> str:
        expanded = set()
        for term in terms:
            expanded.add(term)
            expanded.add(term.lower())
        return ", ".join(sorted(expanded))

    def _load_yaml_as_list(self, path: str) -> list:
        if not os.path.exists(path):
            return []
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            return data if isinstance(data, list) else []

    def _read_file(self, path: str) -> str:
        if not os.path.exists(path):
            print(f"::warning::Template file not found: {path}")
            return ""
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()

    def _load_yaml_as_dict(self, path: str) -> dict:
        if not os.path.exists(path):
            print(f"::warning::Synonyms file not found at: {path}. Skipping normalization.")
            return {}
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
                if isinstance(data, dict):
                    print(f"✅ Loaded {len(data)} synonym rules from {path}")
                    return data
                return {}
        except Exception as e:
            print(f"::error::Failed to parse synonyms YAML: {e}")
            return {}
    
    def normalize_content(self, text: str) -> str:
        if not self.synonyms:
            return text
            
        # Regex to capture code blocks:
        # 1. Fenced code blocks (```...```) - matches across newlines
        # 2. Inline code (`...`) - typically single line, no internal newlines usually
        code_pattern = r'(```[\s\S]*?```|`[^`\n]+`)'
        
        # Split text by code blocks. 
        # Because we use capturing groups (), re.split includes the separators (the code blocks) in the result list.
        parts = re.split(code_pattern, text)
        
        processed_parts = []
        for part in parts:
            # Check if this part is a code block (starts with backtick)
            # Note: We rely on the regex structure ensuring code blocks start with `
            if part and part.startswith("`"):
                processed_parts.append(part) # Preserve code exactly as is
            else:
                # This is plain text, apply synonyms
                temp_text = part
                for bad, good in self.synonyms.items():
                    # Strict word boundary (\b) ensures we don't match inside other words
                    pattern = re.compile(r'\b' + re.escape(bad) + r'\b', re.IGNORECASE)
                    temp_text = pattern.sub(good, temp_text)
                processed_parts.append(temp_text)
                
        return "".join(processed_parts)

    def _load_dict_as_string(self, path: str) -> str:
        if not os.path.exists(path):
            print(f"::warning::Dictionary NOT found at: {path}")
            return ""
        print(f"✅ Loaded dictionary from: {path}")
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            return "\n".join([f"{k}: {v}" for k, v in data.items()]) if data else ""

    def validate_mdx(self, original: str, translated: str) -> tuple[bool, str]:
        tag_pattern = r'<\s*/?\s*[A-Za-z_][A-Za-z0-9_.-]*\b[^<>]*?/?>'
        
        orig_tags = re.findall(tag_pattern, original)
        trans_tags = re.findall(tag_pattern, translated)
        
        if len(orig_tags) == len(trans_tags):
            return True, ""
            
        # Build the detailed error report
        error_msg = [f"❌ TAG MISMATCH DETAILS:"]
        error_msg.append(f"   - Original Tag Count: {len(orig_tags)}")
        error_msg.append(f"   - Translated Tag Count: {len(trans_tags)}")
        
        orig_counts = collections.Counter(orig_tags)
        trans_counts = collections.Counter(trans_tags)
        
        all_tags = set(orig_counts.keys()) | set(trans_counts.keys())
        
        for tag in all_tags:
            diff = trans_counts[tag] - orig_counts[tag]
            if diff != 0:
                status = "EXTRA" if diff > 0 else "MISSING"
                error_msg.append(f"   - {status} {abs(diff)}x: {tag}")
                
        return False, "\n".join(error_msg)

    def translate_file(self, input_file: str):
        if not os.path.exists(input_file):
            msg = f"File not found: {input_file}"
            print(f"::error::{msg}")
            # Even if input missing, we log it. Since output path is hypothetical, use input.
            self.failures.append({"file": input_file, "error": msg})
            self.has_errors = True
            return
        
        source_lang = "en"
        if "docs/zh/" in input_file: source_lang = "zh"
        elif "docs/ja/" in input_file: source_lang = "ja"
        source_lang_full = LANG_MAP.get(source_lang, source_lang)

        abs_input = os.path.abspath(input_file)
        # Determine the Output Path
        output_file = abs_input.replace(f"/docs/{source_lang}/", f"/docs/{self.target_lang}/")
        # Relative path for cleaner reporting (e.g. pr_code/docs/ja/...)
        rel_output_file = output_file
        if os.getcwd() in output_file:
             rel_output_file = os.path.relpath(output_file, os.getcwd())

        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        system_instruction = (self.system_template
                              .replace("${source_lang}", source_lang_full)
                              .replace("${target_lang}", self.target_lang_full)
                              .replace("${dictionary}", self.dictionary_str)
                              .replace("${never_translate}", self.never_translate_str))
        
        if self.dry_run:
            print(f"🔍 [DRY RUN] {source_lang_full} -> {self.target_lang_full} | Input: {input_file}")
            return
        
        original_content = self._read_file(input_file)
        content_to_process = self.normalize_content(original_content)
        
        current_human_prompt = (self.human_template
                                .replace("${target_language}", self.target_lang_full) 
                                + f"\n\n### CONTENT TO TRANSLATE ###\n\n{content_to_process}")

        print(f"🚀 Translating {input_file} to {output_file}...")
        
        max_retries = 5
        base_delay = 5 
        translated_text = ""
        
        try:
            for attempt in range(max_retries):
                try:
                    response = client.models.generate_content(
                        model=MODEL_NAME,
                        config=types.GenerateContentConfig(system_instruction=system_instruction, temperature=0.0),
                        contents=current_human_prompt
                    )
                    
                    if not response.text:
                        print(f"⚠️ Warning: Gemini returned empty response for {input_file} (Blocked?).")
                        continue
                        
                    translated_text = response.text.strip()
                    break # Success
                    
                except Exception as e:
                    if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                        if attempt < max_retries - 1:
                            wait_time = base_delay * (2 ** attempt)
                            print(f"⏳ Hit rate limit (429). Retrying in {wait_time}s...")
                            time.sleep(wait_time)
                            continue
                    raise e 
            
            else:
                 raise RuntimeError("Max retries exceeded.")

        except Exception as e:
            msg = f"Gemini API failed: {str(e)}"
            print(f"❌ {msg}")
            # CHANGED: Report output_file (target) instead of input_file
            self.failures.append({"file": rel_output_file, "error": msg})
            self.has_errors = True
            return

        # Clean Markdown fences
        if translated_text.startswith("```"):
            lines = translated_text.splitlines()
            if lines[0].startswith("```"): lines = lines[1:]
            if lines and lines[-1].startswith("```"): lines = lines[:-1]
            translated_text = "\n".join(lines).strip()

        # Validation
        is_valid, validation_msg = self.validate_mdx(original_content, translated_text)
        
        if not is_valid:
            print(f"❌ Validation FAILED for {input_file}")
            print(validation_msg)
            # CHANGED: Report output_file (target) instead of input_file
            self.failures.append({"file": rel_output_file, "error": validation_msg})
            self.has_errors = True

            # Save invalid output with a distinct suffix so it is not mistaken for a valid file
            invalid_output_file = output_file + ".invalid"
            with open(invalid_output_file, 'w', encoding='utf-8') as f:
                f.write(translated_text)
            print(f"⚠️ Saved invalid translation for inspection: {invalid_output_file}")
        else:
            # CHANGED: Report output_file (target) on success too
            self.successes.append(rel_output_file)

            # Only save to the final output path when validation passes
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

    # --- GENERATE SUMMARY REPORT ---
    report_path = "translation_summary.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("### 📝 Translation Report\n\n")
        
        if translator.successes:
            f.write("#### ✅ Successfully Translated\n")
            for success in translator.successes:
                f.write(f"- `{success}`\n")
            f.write("\n")
            
        if translator.failures:
            f.write("#### ❌ Failures (Action Required)\n")
            f.write("The following files failed validation or API checks. **These files were NOT committed.**\n\n")
            for failure in translator.failures:
                f.write(f"**File:** `{failure['file']}`\n")
                f.write("```text\n")
                f.write(failure['error'])
                f.write("\n```\n\n")

    if translator.has_errors:
        print("\n🚨 Translation finished with errors. Marking workflow as FAILED.")
        sys.exit(1)

if __name__ == "__main__":
    main()

