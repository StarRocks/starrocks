# Translating docs with GPT

This README describes using GPT-4o to translate from Chinese to English, or from English to Chinese. The system used is specific to Docusaurus Markdown and MDX. We are using code provided by Weights and Biases, as they also use Docusaurus and have expertise with LLMs.

To translate an English doc:

## Set up the environment

There are three environment variables that need to be set in the file starrocks/docs/translation/.env:

> Tip
>
> Copy the file `starrocks/translation/.env.sample` to `starrocks/translation/.env` and edit with your API keys, or contact the Doc team leader for keys.

- OPENAI_API_KEY
- WANDB_API_KEY
- GIT_PYTHON_REFRESH

`GIT_PYTHON_REFRESH` should be set to `quiet` because we are not interacting with Git within the container. The other two environment variables will be provided by the Documentation Team leader.

These should be set in the file in `starrocks/docs/translation/.env`

This is the format:

```bash
OPENAI_API_KEY=sk-proj-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
WANDB_API_KEY=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
GIT_PYTHON_REFRESH=quiet
```

## Files to translate

Provide the paths of the files to translate in the file `starrocks/docs/translation/files.txt`

The entries in the file should be relative to the `starrocks/docs/translation/` folder, for example:

> Tip
>
> The only difference in the `files.txt` content below is the name of the directory, `zh` or `en`. Add the path to the files that need to be translated.

<table>
<tr>
<td>

### From English to Chinese

`starrocks/docs/translation/files.txt`

```bash
../en/quick_start/quick_start.md
../en/deployment/helm.md
```

</td>
<td>

### From Chinese to English

`starrocks/docs/translation/files.txt`

```bash
../zh/quick_start/quick_start.md
../zh/deployment/helm.md
```

</td>
</tr>
</table>

## Build the Docker image

This probably only needs to be done once unless the folks from Weights and Biases modify the Python package `gpt_translate`.

```bash
cd docs/translation
docker build -f translation.Dockerfile -t translate .
```

## Translate the docs

Change dir back up to the `starrocks` folder so that you can mount the `docs/` folder in the container.

```bash
cd ../../
```

Translate the files:

<table>
<tr>
<td>

### From English to Chinese

```bash
docker run -v ./docs:/docs \
  --env-file ./docs/translation/.env \
  translate \
  bash /docs/translation/scripts/en-to-zh.translate.sh
```

</td>
<td>

### From Chinese to English

```bash
docker run -v ./docs:/docs \
  --env-file ./docs/translation/.env \
  translate \
  bash /docs/translation/scripts/zh-to-en.translate.sh
```

</td>
</tr>
</table>

### Sample output

You should see output similar to this if you are translating a single file:

```bash
Logged in as Weights & Biases user: danroscigno.
View Weave data at https://wandb.ai/danroscigno-starrocks/gpt-translate/weave
[14:56:11] INFO     config_folder: ./configs                           cli.py:70
                    debug: false
                    do_translate_header_description: true
                    input_file: ./files.txt
                    input_folder: ../en
                    language: zh
                    limit: null
                    max_openai_concurrent_calls: 7
                    max_tokens: 4096
                    model: gpt-4o
                    out_file: ' intro_ja.md'
                    out_folder: ../zh
                    remove_comments: true
                    replace: true
                    silence_openai: true
                    temperature: 0.2
                    weave_project: gpt-translate

           INFO     Reading ./files.txt                         translate.py:195
           INFO     Translating 1 file                          translate.py:202
Translating files:   0%|          | 0/1 [00:00<?, ?it/s][14:56:34] INFO     ✅ Translated file saved to                 translate.py:169
                    ../zh/developers/trace-tools/Trace.md
Translating files: 100%|██████████| 1/1 [00:22<00:00, 22.63s/it]
📦 Published to https://wandb.ai/danroscigno-starrocks/gpt-translate/weave/objects/Translation-zh/versions/UaI7t2Vtn8iI2Zcw7bQVHV1BMPUHYDY8cqBfcTW3QYQ
🍩 https://wandb.ai/danroscigno-starrocks/gpt-translate/r/call/0192dded-553f-7492-a82b-11539ad42bfa
```

## Check the files

Once the translation is complete the container will exit. Check the status with `git status` and check the translated file(s).

### Sample `git status`

My `starrocks/docs/translation/files.txt` contained a single path to the English `Trace.md` file, so a translated file was produced at `docs/zh/developers/trace-tools/Trace.md`:

```bash
$ git status

Changes not staged for commit:
  (use "git add <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
	modified:   docs/zh/developers/trace-tools/Trace.md
```
