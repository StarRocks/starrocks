# Translating docs with GPT

This README describes using GPT-4o to translate from Chinese to English, or from English to Chinese. The system used is specific to Docusaurus Markdown and MDX. We are using code provided by Weights and Biases, as they also use Docusaurus and have expertise with LLMs.

To translate an English doc:

## Set up the environment

There are three environment variables that need to be set in the file starrocks/docs/translation/.env:

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

## Check the files

Once the translation is complete the container will exit. Check the status with `git status` and check the translated file(s).

