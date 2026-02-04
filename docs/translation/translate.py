import os
import sys
import yaml
import argparse
import re
import time
import collections 
from google import genai
from google.genai import types

# Initialize Gemini client
if "GEMINI_API_KEY" not in os.environ:
    raise RuntimeError("GEMINI_API_KEY not set.")

try:
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
except Exception as e:
    raise RuntimeError("Failed to initialize Gemini client.") from e

MODEL_NAME = "gemini-2.0-flash" 
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_BASE_PATH = os.path.join(SCRIPT_DIR, "configs")

LANG_MAP = {
    "en": "English",
    "ja": "Japanese",
    "zh": "Simplified Chinese"
}

class StarRocksTranslator:
    def __init__(self, target_lang: str):
        self.target_lang = target_lang
        self.target_lang_full = LANG_MAP.get(target_lang, target_lang)
        
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
        if not os.path.exists(path): return []
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            return data if isinstance(data, list) else []

    def _read_file(self, path: str) -> str:
        if not os.path.exists(path):
            print(f"::warning::Template file not found: {path}")
            return ""
        with open(path, 'r', encoding='utf-8') as f: return f.read()

    def _load_yaml_as_dict(self, path: str) -> dict:
        if not os.path.exists(path): return {}
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
                return data if isinstance(data, dict) else {}
        except Exception as e:
            print(f"::error::Failed to parse synonyms YAML: {e}")
            return {}
    
    def _load_dict_as_string(self, path: str) -> str:
        if not os.path.exists(path): return ""
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            return "\n".join([f"{k}: {v}" for k, v in data.items()]) if data else ""

    def normalize_content(self, text: str) -> str:
        if not self.synonyms: return text
        
        # Protect code blocks from synonym replacement
        code_pattern = r'(`[^`\n]+`)'
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

    def _parse_document_structure(self, text: str) -> list[dict]:
        """
        Parses document into chunks. 
        Code blocks (```...```) and HTML comments () are 'SKIP' chunks.
        Headers trigger new 'TRANS' chunks.
        """
        chunks = []
        # 1. Code Block (greedy match)
        # 2. HTML Comment (constructed safely to avoid chat UI bugs)
        # 3. Header start (matches ^ followed by ##.. and rest of line)
        pattern = r'(```[\s\S]*?```|' + r'<' + r'!--[\s\S]*?--' + r'>|^[ \t]*#{2,5}\s.*)'
        
        # Pass re.MULTILINE so '^' matches start of lines
        parts = re.split(pattern, text, flags=re.MULTILINE)
        
        current_trans_buffer = ""
        
        def flush_buffer():
            nonlocal current_trans_buffer
            if current_trans_buffer:
                chunks.append({'type': 'TRANS', 'content': current_trans_buffer})
                current_trans_buffer = ""

        for part in parts:
            if not part: continue
            
            # Identify the type of this part
            is_code = part.startswith("```")
            is_comment = part.startswith("<" + "!--")
            # Use re.match with MULTILINE to correctly check for headers
            is_header = re.match(r'^[ \t]*#{2,5}\s', part)
            
            if is_code or is_comment:
                flush_buffer()
                chunks.append({'type': 'SKIP', 'content': part})
            elif is_header:
                flush_buffer()
                current_trans_buffer = part
            else:
                current_trans_buffer += part
        
        flush_buffer()
        return chunks

    def _clean_model_output(self, chunk_translated: str) -> str:
        match = re.search(r'<chunk_to_translate>(.*?)</chunk_to_translate>', chunk_translated, re.DOTALL)
        if match: chunk_translated = match.group(1).strip()
        chunk_translated = re.sub(r'</?chunk_to_translate>', '', chunk_translated).strip()

        lines = chunk_translated.splitlines()
        if not lines: return ""
        
        # Remove markdown fences if the LLM wrapped the whole response
        if lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
            
        return "\n".join(lines).strip()

    def validate_mdx(self, original: str, translated: str) -> tuple[bool, str]:
        def strip_inline_code(text): return re.sub(r'(`[^`\n]+`)', '', text)
        
        clean_orig = strip_inline_code(original)
        clean_trans = strip_inline_code(translated)
        
        # Updated Regex for better tag matching
        tag_pattern = r'<(?!!--)\s*/?\s*([A-Za-z_][A-Za-z0-9_.-]*)(?=[\s/>])[^>]*?>'
        IGNORED_TAGS = {"none", "unset", "nil", "generated", "br"}

        def get_fingerprints(txt):
            fps = []
            for match in re.finditer(tag_pattern, txt):
                full = match.group(0)
                name = match.group(1)
                if name.lower() in IGNORED_TAGS: continue
                type_p = "CLOSE" if full.startswith("</") else "SELF" if full.endswith("/>") else "OPEN"
                fps.append(f"{type_p}:{name}")
            return fps

        orig_fps = get_fingerprints(clean_orig)
        trans_fps = get_fingerprints(clean_trans)
        
        orig_counts = collections.Counter(orig_fps)
        trans_counts = collections.Counter(trans_fps)
        
        valid_tags = set(orig_counts.keys())
        
        error_msg = [f"❌ TAG MISMATCH DETAILS:"]
        has_error = False

        for tag in valid_tags:
            diff = trans_counts[tag] - orig_counts[tag]
            if diff < 0:
                has_error = True
                error_msg.append(f"   - MISSING {abs(diff)}x: {tag}")

        for tag in trans_counts.keys():
            if tag in valid_tags and (trans_counts[tag] - orig_counts[tag]) > 0:
                has_error = True
                error_msg.append(f"   - EXTRA {trans_counts[tag] - orig_counts[tag]}x: {tag}")
            
        if not has_error: return True, ""
        return False, "\n".join(error_msg)

    def translate_file(self, input_file: str):
        if not os.path.exists(input_file): return
        abs_input = os.path.abspath(input_file)
        
        source_lang = "en"
        if "/docs/zh/" in abs_input: source_lang = "zh"
        elif "/docs/ja/" in abs_input: source_lang = "ja"
        elif "/docs/en/" in abs_input: source_lang = "en"
        
        source_lang_full = LANG_MAP.get(source_lang, source_lang)
        base_output_path = abs_input.replace(f"/docs/{source_lang}/", f"/docs/{self.target_lang}/")
        
        if base_output_path == abs_input:
            print(f"❌ Error: Output path identical to input. Aborting. (Input: {abs_input})")
            return

        os.makedirs(os.path.dirname(base_output_path), exist_ok=True)

        original_content = self._read_file(input_file)
        chunks = self._parse_document_structure(original_content)
        
        trans_chunks_count = sum(1 for c in chunks if c['type'] == 'TRANS')
        print(f"🚀 Processing {input_file}: {len(chunks)} segments ({trans_chunks_count} to translate)...")
        
        final_segments = []
        translated_count = 0
        
        system_instruction = (self.system_template
                              .replace("${source_lang}", source_lang_full)
                              .replace("${target_lang}", self.target_lang_full)
                              .replace("${dictionary}", self.dictionary_str)
                              .replace("${never_translate}", self.never_translate_str)
                              + "\nIMPORTANT: Return ONLY the translated content. Do not wrap in markdown code blocks.")

        for i, chunk in enumerate(chunks):
            if chunk['type'] == 'SKIP':
                final_segments.append(chunk['content'])
                continue
            
            translated_count += 1
            if trans_chunks_count > 0:
                percent = int((translated_count / trans_chunks_count) * 100)
            else:
                percent = 100
                
            sys.stdout.write(f"\r[{translated_count}/{trans_chunks_count}] {percent}% ...")
            sys.stdout.flush()

            content_to_translate = self.normalize_content(chunk['content'])
            
            if not content_to_translate.strip():
                # SKIPPING whitespace-only chunks to prevent excessive spacing
                continue

            anchored_chunk = f"<chunk_to_translate>\n{content_to_translate}\n</chunk_to_translate>"
            human_prompt = (self.human_template.replace("${target_language}", self.target_lang_full) 
                                    + f"\n\n### CONTENT TO TRANSLATE ###\n\n{anchored_chunk}")
            
            chunk_result = ""
            max_retries = 10
            for attempt in range(max_retries):
                try:
                    response = client.models.generate_content(
                        model=MODEL_NAME,
                        config=types.GenerateContentConfig(system_instruction=system_instruction, temperature=0.0),
                        contents=human_prompt
                    )
                    if not response.text: raise RuntimeError("Empty response")
                    chunk_result = response.text.strip()
                    break
                except Exception as e:
                    error_str = str(e)
                    is_retryable = ("429" in error_str or "503" in error_str or "500" in error_str or "INTERNAL" in error_str or "RESOURCE_EXHAUSTED" in error_str)
                    if is_retryable and attempt < max_retries - 1:
                        wait = min(5 * (2 ** attempt), 60)
                        sys.stdout.write("\033[K") 
                        print(f"\n⚠️ API Error on chunk {i+1}: {error_str}. Retrying in {wait}s...")
                        time.sleep(wait)
                        continue
                    print(f"\n❌ Failed chunk {i+1}: {str(e)}")
                    self.failures.append({"file": base_output_path, "error": str(e)})
                    self.has_errors = True
                    return

            cleaned_chunk = self._clean_model_output(chunk_result)
            final_segments.append(cleaned_chunk)

        print(f"\n✨ Translation complete!")
        
        # JOINING with double newlines for standard markdown spacing
        full_text = "\n\n".join(final_segments)
        
        is_valid, val_msg = self.validate_mdx(original_content, full_text)
        final_output_path = base_output_path if is_valid else f"{base_output_path}.invalid"
        rel_path = os.path.relpath(final_output_path, os.getcwd())

        if not is_valid:
            print(f"❌ Validation FAILED for {input_file}")
            print(val_msg)
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
            if not f.lower().endswith((".md", ".mdx")):
                print(f"⏭️ Skipping non-markdown file: {f}", file=sys.stderr)
                continue
            translator.translate_file(f)

    report_path = "translation_summary.md"
    report_exists = os.path.exists(report_path)
    report_is_empty = (not report_exists) or os.stat(report_path).st_size == 0
    mode = "w" if report_is_empty else "a"
    with open(report_path, mode, encoding="utf-8") as f:
        if report_is_empty:
            f.write("### 📝 Translation Report\n\n")
        f.write(f"#### 🌐 Language: {args.lang.upper()}\n")
        if translator.successes:
            f.write("✅ **Successfully Translated:**\n")
            for s in translator.successes: f.write(f"- `{s}`\n")
        if translator.failures:
            f.write("\n❌ **Failures:**\n")
            for fail in translator.failures:
                f.write(f"**File:** `{fail['file']}`\n```text\n{fail['error']}\n```\n\n")
        f.write("\n---\n")

    if translator.has_errors: sys.exit(1)

if __name__ == "__main__":
    main()
