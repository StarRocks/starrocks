# Translating Docs with Google Gemini

This tool uses Gemini 2.0 to translate StarRocks documentation while preserving
Docusaurus MDX components and enforcing official terminology.

## Setup

1. **API Key:** Obtain a Gemini API Key and export it
    ```sh
    export GEMINI_API_KEY="your_key_here"
    ```
2. **Python:** Ensure Python 3.10+ and `pip` are installed.
3. **Environment:** (Optional) Add `export GEMINI_API_KEY="your_key_here"` to your `~/.zshrc` or `~/.bashrc` to avoid manual exports.

## Installation

Run these commands from the root of the `starrocks` repository:

> Note:
>
> Always use a `venv` (virtual environment) to avoid conflicts with other Python script requirements.

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -U google-genai pyyaml
```

## Usage

```sh
source .venv/bin/activate
python docs/translation/translate.py -l <target_lang> --files <paths_to_files>
```

### Parameters

- `-l`: Target language code (`en`, `zh`, `ja`).
- `--files`: Space-separated list of file paths.
- `--dry-run`: (Optional) Preview the source-to-target path mapping without calling the API.

## Examples

### Single File

Translate the English shared-data quick start to Simplified Chinese:

```sh
source .venv/bin/activate
python docs/translation/translate.py -l zh --files docs/en/quick_start/shared-data.md
```

### Multiple Files

```sh
source .venv/bin/activate
python docs/translation/translate.py -l ja \
  --files \
    docs/en/file1.md \
    docs/en/file2.mdx
```

## Important Notes

- **Output Paths:** The script automatically maps paths. `docs/en/item.md` translated to `zh` will automatically be saved to `docs/zh/item.md`.
- **Safety Skip:** If the target file already exists and has been modified more recently than the source file, the script will skip it to protect manual edits.
- **Terminology:** Translations are guided by dictionaries in `docs/translation/configs/language_dicts/`. Update these YAML files to fix recurring translation errors.
- **MDX Tags:** The script validates that the number of `<`Components `/>` matches the original. Check the console output for "Validation warnings."

