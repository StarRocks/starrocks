# Doc SQL-example verification clusters

Disposable StarRocks clusters used by the documentation SQL-example checker
(`docs/scripts/run_sql_samples.py`) to confirm the ` ```sql ` examples in the docs
still run on a given release.

## Version alignment

Test the docs for a version against a cluster of **that same version**. The image
tag is set by the `SR_VERSION` environment variable (default: the current
release). So to verify the 3.5 docs, run the 3.5 cluster and point the checker at
a `branch-3.5` docs checkout.

## Usage

```bash
# current release (default)
docker compose -f docker-compose-shared-nothing.yml up -d --wait

# a specific release — the docs branch you test must match
SR_VERSION=3.5 docker compose -f docker-compose-shared-nothing.yml up -d --wait

# shared-data (FE + CN + MinIO), for cloud-native / storage-volume examples
SR_VERSION=4.1 docker compose -f docker-compose-shared-data.yml up -d --wait
```

The FE is reachable on `127.0.0.1:9030` (MySQL protocol), user `root`, no
password. Tear down with `docker compose -f <file> down`.

## Which compose?

- **shared-nothing** (FE + BE) — the default for most `sql-reference` examples.
- **shared-data** (FE + CN + MinIO) — for examples that need cloud-native tables,
  storage volumes, or datacache (run the checker with `--profile shared-data`).
