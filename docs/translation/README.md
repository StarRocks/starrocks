# StarRocks Documentation Translation System

This directory contains the tools and configuration for the automated translation of StarRocks documentation using Google Gemini.

The system is designed to handle **GitHub Forks securely**, allowing contributors to trigger translations on their Pull Requests without accessing repository secrets.

## Important Notes

- **Output Paths:** The script automatically maps paths. `docs/en/item.md` translated to `zh` will automatically be saved to `docs/zh/item.md`.
- **Safety Skip:** If the target file already exists and has been modified more recently than the source file, the script will skip it to protect manual edits.
- **Terminology:** Translations are guided by dictionaries in `docs/translation/configs/language_dicts/`. Update these YAML files to fix recurring translation errors.
- **MDX Tags:** The script validates that the number of `<`Components `/>` matches the original. Check the console output for "Validation warnings."

## 📂 Directory Structure

```text
docs/translation/
├── translate.py           # Main CLI script (The Engine)
├── requirements.txt       # Python dependencies
├── configs/
│   ├── system_prompt.txt  # The "Laws of the Universe" for the LLM
│   ├── human_prompt.txt   # The template for the content to translate
│   ├── never_translate.yaml # List of terms to strictly preserve (e.g., "Leader")
│   ├── synonyms.yaml      # Pre-processing rules (e.g., "high-concurrency" -> "high concurrency")
│   └── language_dicts/    # Glossaries for specific languages
│       ├── ja.yaml        # English -> Japanese terms
│       └── zh.yaml        # English -> Chinese terms
```

## 🛠️ Setup

> Note:
>
> Always use a Python `venv` (virtual environment) to avoid conflicts with other Python script requirements. In the setup and examples section the steps for a `venv` are included.


### Prerequisites

* Python 3.10+
* A Google Cloud Project with the Gemini API enabled.
* An API Key exported as `GEMINI_API_KEY`.

### Installation

```bash
cd docs/translation
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

```

---

## 🚀 Usage

### 1. Manual Usage (CLI)

You can run the translator locally to test changes or translate files without a PR.

```bash
# Syntax: python translate.py -l <target_lang> --files <path_to_file>

# Example: Translate a single file to Chinese

export GEMINI_API_KEY="your key here"
source docs/translation/.venv/bin/activate
python docs/translation/translate.py -l zh --files docs/en/introduction/Architecture.md

# Example: Multiple Files to Japanese

export GEMINI_API_KEY="your key here"
source docs/translation/.venv/bin/activate
python docs/translation/translate.py -l ja \
  --files \
    docs/en/file1.md \
    docs/en/file2.mdx

# Example: Dry Run (Check paths and normalization without calling API)

export GEMINI_API_KEY="your key here"
source docs/translation/.venv/bin/activate
python docs/translation/translate.py -l zh --files docs/en/quick_start.md --dry-run

```

**Supported Languages:**

* `ja` (Japanese)
* `zh` (Simplified Chinese)
* `en` (English - typically for translating Chinese docs back to English)

### 2. GitHub Actions (The Bot)

The primary way to use this tool is via GitHub Pull Requests.

1. **Open a PR** with documentation changes.
2. **Wait for the Bot:** A workflow (`ci-doc-translation-check.yml`) will scan your changed files.
* If missing translations are found, it posts a "Translation Required" checklist.


3. **Trigger Translation:**
* **Maintainers only:** Reply to the comment with `/translate`.
* The system will translate the checked files and push the results back to the PR (even if it's from a Fork).



---

## ⚙️ Configuration & Customization

### 1. Forbidden Terms (`never_translate.yaml`)

Terms listed here are **strictly protected**.

* **Mechanism:** They are injected into the LLM's dictionary as Identity Rules (e.g., `Leader: Leader`) to force the model to copy them exactly.
* **Update:** Add new product names or proper nouns here.

### 2. Synonyms (`synonyms.yaml`)

Used to normalize inconsistent terminology *before* translation.

* **Example:** `high-concurrency: high concurrency` ensures the model translates the concept correctly as a noun phrase, avoiding awkward grammar in target languages.

### 3. Dictionaries (`configs/language_dicts/`)

These YAML files define the "Golden Translation" for technical terms.

* **Format:** `English Term: Target Term`
* If the LLM is consistently mistranslating a specific technical concept, add it here.

---

## 🔒 Security Architecture

This pipeline uses a **Dual-Workflow** design to securely handle Pull Requests from Forks.

### Workflow 1: The Reporter (`ci-doc-translation-check.yml`)

* **Trigger:** `pull_request_target`
* **Role:** Identifies missing translations and posts the checklist.
* **Security:** Runs in the context of the Base Repo to get write permissions for commenting, but only reads file names.

### Workflow 2: The Executor (`ci-doc-translater.yml`)

* **Trigger:** `issue_comment` (specifically `/translate`)
* **Role:** Performs the actual translation.
* **Security Features:**
1. **Maintainer Gate:** Only users in the `docs-maintainer` team can trigger it.
2. **Trusted Tools:** It checks out the `main` branch to get the trusted `translate.py` script.
3. **Untrusted Data:** It checks out the Fork's code to a separate `pr_code/` directory.
4. **Execution:** It runs the **Trusted Script** against the **Untrusted Data**, preventing malicious code injection from PRs.

---

## ❓ Troubleshooting

### 429 RESOURCE_EXHAUSTED

* **Cause:** The Gemini API rate limit was hit.
* **Solution:** The script has built-in exponential backoff (retries). If it persists, check the quota for your Google Cloud Project.

### 404 NOT_FOUND (Model)

* **Cause:** The API Key cannot access the specific model version (e.g., `gemini-1.5-flash-002`).
* **Solution:** Edit `translate.py` and switch `MODEL_NAME` to a model your key supports (e.g., `gemini-2.0-flash` or `gemini-1.5-flash`).

### "User X is not authorized"

* **Cause:** A non-maintainer tried to use `/translate`.
* **Solution:** Only members of the `docs-maintainer` team can trigger the bot.

