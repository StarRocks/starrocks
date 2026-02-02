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
        
        self.has_errors = False
        self.successes = [] 
        self.failures = []  
        
        self.system_template = self._read_file(f"{CONFIG_BASE_PATH}/system_prompt.txt")
        self.human_template = self._read_file(f"{CONFIG_BASE_PATH}/human_prompt.txt")
        
        dict_path = f"{CONFIG_BASE_PATH}/language_dicts/{target_lang}.yaml"
        self.dictionary_str = self._load_dict_as_string(dict_path)

        synonyms_path = f"{CONFIG_BASE_PATH}/synonyms.yaml"
        self.synonyms = self._load_yaml_as_dict(synonyms_path)
        
        raw_terms = self._load_yaml_as_list(f"{CONFIG_BASE_PATH}/never_translate.yaml")
        self.never_translate_str = self._expand_terms(raw_terms)
        
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
            return {}
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
                return data if isinstance(data, dict) else {}
        except Exception as e:
            print(f"::error::Failed to parse synonyms YAML: {e}")
            return {}
    
    def normalize_content(self, text: str) -> str:
        if not self.synonyms:
            return text
        code_pattern = r'(```[\s\S]*?```|`[^`\n]+`)'
        parts = re.split(code_pattern, text)
        processed_parts = []
        for part in parts:
            if part and part.startswith("`"):
                processed_parts.append(part)
            else:
                temp_text = part
                for bad, good in self.synonyms.items():
                    pattern = re.compile(r'\b' + re.escape(bad) + r'\b', re.IGNORECASE)
                    temp_text = pattern.sub(good, temp_text)
                processed_parts.append(temp_text)
        return "".join(processed_parts)
    
    def _load_dict_as_string(self, path: str) -> str:
        if not os.path.exists(path):
            return ""
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            return "\n".join([f"{k}: {v}" for k, v in data.items()]) if data else ""

    def _strip_code_blocks(self, text: str) -> str:
        code_pattern = r'(```[\s\S]*?```|`[^`\n]+`)'
        return re.sub(code_pattern, '', text)

    def _chunk_content(self, text: str) -> list[str]:
        # Primary strategy: split by Level 2 through Level 5 headers for high granularity.
        chunks = re.split(r'(?m)^(?=#{2,5}\s)', text)
        chunks = [c for c in chunks if c.strip()]

        # Fallback Strategy:
        # If the document has no matching headers (e.g. only H1 or no headers), 
        # header splitting will produce a single large chunk.
        # If that single chunk is too large, we fall back to splitting by paragraphs.
        LARGE_DOC_THRESHOLD = 20000
        MAX_FALLBACK_CHUNK_SIZE = 4000

        if len(chunks) == 1 and len(text) > LARGE_DOC_THRESHOLD:
            paragraphs = re.split(r'\n\s*\n', text)
            fallback_chunks = []
            current_buffer = ""
            
            for p in paragraphs:
                if not p.strip():
                    continue
                
                # Estimate size if we add this paragraph (plus double newline separator)
                candidate_len = len(current_buffer) + len(p) + 2
                
                if candidate_len > MAX_FALLBACK_CHUNK_SIZE and current_buffer:
                    # Current buffer is full, flush it and start a new one
                    fallback_chunks.append(current_buffer)
                    current_buffer = p
                else:
                    # Append to current buffer
                    if current_buffer:
                        current_buffer += "\n\n" + p
                    else:
                        current_buffer = p
            
            if current_buffer.strip():
                fallback_chunks.append(current_buffer)
            
            return [c for c in fallback_chunks if c.strip()]

        return chunks

    def validate_mdx(self, original: str, translated: str) -> tuple[bool, str]:
        clean_orig = self._strip_code_blocks(original)
        clean_trans = self._strip_code_blocks(translated)
        
        # Pattern to find tags but ignore HTML comments
        tag_pattern = r'<(?!\!--)\s*/?\s*([A-Za-z_][A-Za-z0-9_.-]*)(?=[\s/>])[^>]*?>'
        
        def get_tag_fingerprints(text):
            fingerprints = []
            for match in re.finditer(tag_pattern, text):
                full_tag = match.group(0)
                tag_name = match.group(1)
                if full_tag.startswith("</"):
                    type_prefix = "CLOSE"
                elif full_tag.endswith("/>"):
                    type_prefix = "SELF"
                else:
                    type_prefix = "OPEN"
                fingerprints.append(f"{type_prefix}:{tag_name}")
            return fingerprints

        orig_fingerprints = get_tag_fingerprints(clean_orig)
        trans_fingerprints = get_tag_fingerprints(clean_trans)
        
        if collections.Counter(orig_fingerprints) == collections.Counter(trans_fingerprints):
            return True, ""
            
        error_msg = [f"❌ TAG MISMATCH DETAILS:"]
        orig_counts = collections.Counter(orig_fingerprints)
        trans_counts = collections.Counter(trans_fingerprints)
        all_tags = set(orig_counts.keys()) | set(trans_counts.keys())
        
        for tag in all_tags:
            diff = trans_counts[tag] - orig_counts[tag]
            if diff != 0:
                status = "EXTRA" if diff > 0 else "MISSING"
                readable_tag = tag.replace("OPEN:", "<").replace("CLOSE:", "</").replace("SELF:", "<.../>")
                if "OPEN" in tag or "CLOSE" in tag: readable_tag += ">"
                error_msg.append(f"   - {status} {abs(diff)}x: {readable_tag}")
        return False, "\n".join(error_msg)

    def translate_file(self, input_file: str):
        if not os.path.exists(input_file):
            return
        
        source_lang = "en"
        if "docs/zh/" in input_file: source_lang = "zh"
        elif "docs/ja/" in input_file: source_lang = "ja"
        source_lang_full = LANG_MAP.get(source_lang, source_lang)

        abs_input = os.path.abspath(input_file)
        base_output_path = abs_input.replace(f"/docs/{source_lang}/", f"/docs/{self.target_lang}/")
        
        os.makedirs(os.path.dirname(base_output_path), exist_ok=True)

        system_instruction = (self.system_template
                              .replace("${source_lang}", source_lang_full)
                              .replace("${target_lang}", self.target_lang_full)
                              .replace("${dictionary}", self.dictionary_str)
                              .replace("${never_translate}", self.never_translate_str))
        
        original_content = self._read_file(input_file)
        content_to_process = self.normalize_content(original_content)
        chunks = self._chunk_content(content_to_process)
        translated_chunks = []
        
        print(f"🚀 Translating {input_file} ({len(chunks)} chunks)...")
        
        max_retries = 10  # Increased from 5 to 10 for better resilience
        
        for i, chunk in enumerate(chunks):
            current_human_prompt = (self.human_template.replace("${target_language}", self.target_lang_full) 
                                    + f"\n\n### CONTENT TO TRANSLATE ###\n\n{chunk}")
            
            chunk_translated = ""
            for attempt in range(max_retries):
                try:
                    response = client.models.generate_content(
                        model=MODEL_NAME,
                        config=types.GenerateContentConfig(system_instruction=system_instruction, temperature=0.0),
                        contents=current_human_prompt
                    )
                    
                    if not response.text:
                        raise RuntimeError("Gemini returned empty response.")
                        
                    chunk_translated = response.text.strip()
                    break
                except Exception as e:
                    error_str = str(e)
                    # Updated retry logic to include 503 and UNAVAILABLE
                    is_retryable = (
                        "429" in error_str or 
                        "RESOURCE_EXHAUSTED" in error_str or 
                        "503" in error_str or 
                        "UNAVAILABLE" in error_str or 
                        "empty response" in error_str.lower()
                    )
                    
                    if is_retryable and attempt < max_retries - 1:
                        # Exponential backoff with a cap at 60 seconds
                        wait_time = min(5 * (2 ** attempt), 60)
                        print(f"⚠️ API Error ({error_str}) on chunk {i+1}. Retrying in {wait_time}s... (Attempt {attempt+1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    
                    # If fatal or out of retries, fail the whole file
                    msg = f"Gemini API failed on chunk {i+1}: {str(e)}"
                    print(f"❌ {msg}")
                    self.failures.append({"file": base_output_path, "error": msg})
                    self.has_errors = True
                    return

            if chunk_translated.startswith("```"):
                lines = chunk_translated.splitlines()
                if lines[0].startswith("```"): lines = lines[1:]
                if lines and lines[-1].startswith("```"): lines = lines[:-1]
                chunk_translated = "\n".join(lines).strip()
            translated_chunks.append(chunk_translated)

        full_text = "\n".join(chunk.strip() for chunk in translated_chunks)
        is_valid, val_msg = self.validate_mdx(original_content, full_text)
        
        final_output_path = base_output_path if is_valid else f"{base_output_path}.invalid"
        rel_path = os.path.relpath(final_output_path, os.getcwd())

        if not is_valid:
            print(f"❌ Validation FAILED for {input_file}")
            self.failures.append({"file": rel_path, "error": val_msg})
            self.has_errors = True
        else:
            self.successes.append(rel_path)

        with open(final_output_path, 'w', encoding='utf-8') as f:
            f.write(full_text)
        print(f"✅ Saved: {rel_path}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", nargs='*', help="Files to process")
    parser.add_argument("-l", "--lang", choices=['ja', 'zh', 'en'], required=True)
    args = parser.parse_args()
    
    translator = StarRocksTranslator(target_lang=args.lang)
    if args.files:
        for f in args.files:
            translator.translate_file(f)

    report_path = "translation_summary.md"
    report_exists = os.path.exists(report_path)
    
    with open(report_path, "a", encoding="utf-8") as f:
        if not report_exists:
            f.write("### 📝 Translation Report\n\n")
        else:
            f.write("\n")
        
        f.write(f"#### 🌐 Language: {args.lang.upper()}\n")
        if translator.successes:
            f.write("✅ **Successfully Translated:**\n")
            for s in translator.successes: f.write(f"- `{s}`\n")
        
        if translator.failures:
            f.write("\n❌ **Failures (Action Required):**\n")
            for fail in translator.failures:
                f.write(f"**File:** `{fail['file']}`\n```text\n{fail['error']}\n```\n\n")
        
        f.write("\n---\n")

    if translator.has_errors: sys.exit(1)

if __name__ == "__main__":
    main()
