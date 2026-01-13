# Translating docs with Google Gemini

## Setup

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -U google-genai pyyaml
```

## Syntax

```sh
python docs/translation/translate.py --files <path to source file> -l <target language short code>
```

### Target language short codes
 - en
 - zh
 - ja

## Example

> Tips:
>
> 1. Run these commands from the `starrocks` dir
> 2. Always source the virtual environment before running a translation:
>    ```sh
>    source .venv/bin/activate
>    ```

Translate the English shared-data quick start to Simplified Chinese:

```sh
# cd into the starrocks repo
source .venv/bin/activate
python docs/translation/translate.py --files docs/en/quick_start/shared-data.md -l zh
```

