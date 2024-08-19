
## Environment

There are three environment variables that need to be set:

- OPENAI_API_KEY
- WANDB_API_KEY
- GIT_PYTHON_REFRESH

`GIT_PYTHON_REFRESH` should be set to `quiet` because we are not interacting with Git within the container. The other two environment variables will be provided by the Documenttion Team leader.

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

```bash
../en/quick_start/quick_start.md
../en/deployment/helm.md
```

## Command

```bash
cd docs/translation
docker build -f translation.Dockerfile .
```

```bash
docker run -v ./docs:/docs \
  --env-file ./docs/translation/.env \
  translate \
  bash /docs/translation/scripts/translate.sh
```
