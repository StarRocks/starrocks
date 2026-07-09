---
displayed_sidebar: docs
sidebar_position: 5
description: "Analyze complex data with Claude and the StarRocks MCP server on a shared-data cluster."
---

# Claude + StarRocks MCP

Stand up a small StarRocks cluster on object storage, load a real public dataset, and ask **Claude** plain-English questions about it through the **StarRocks MCP server**. Claude discovers the schema, writes the SQL (including multi-table joins), and renders charts. All data lives in object storage on **MinIO**.

This tutorial covers:

- Running StarRocks in shared-data mode (1 FE + 1 CN) and MinIO in Docker
- Creating an S3 (MinIO) storage volume for separate storage and compute
- Loading the Olist Brazilian E-Commerce dataset (8 related tables)
- Wiring the StarRocks and AIStor MCP servers to Claude
- Asking Claude plain-English questions and rendering charts

The dataset is the **Olist Brazilian E-Commerce** set — 8 related tables, good for genuinely complex joins. Everything runs on **1 FE + 1 CN**, on a laptop or a free cloud tier.

There is a lot of information in this document, and it is presented with the step by step content at the beginning and the reference material at the end. This is done so that you can stand up the environment and start asking questions first, and read the supporting details afterward.

---

## Prerequisites

### Docker

- [Docker](https://www.docker.com/get-started/) (Docker Desktop or engine + compose)
- ~4 GB RAM assigned to Docker

### MySQL client

A MySQL client (e.g. `mysql`) to run the SQL files. This is provided by the StarRocks FE server, so all of the SQL commands in this guide are run with `docker compose exec`.

### uv

[`uv`](https://docs.astral.sh/uv/) runs the StarRocks MCP server.

### Claude Code or Claude Desktop

[Claude Code](https://claude.com/product/claude-code) or Claude Desktop connects the MCP servers.

:::tip
This demo includes steps using Claude Code. Other LLMs work with MCP servers, but this demo has only been tested with Claude Code. Please open an issue and let us know what you experience with other LLMs.
:::

### The Olist dataset

The **Olist dataset** is downloaded automatically when you load the data via `kagglehub` (no Kaggle account needed): [https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

:::note
Apple Silicon: StarRocks leans on AVX2 (x86); use an ARM build or expect slower emulation.
:::

---

## Terminology

### MCP

The Model Context Protocol (MCP) is an open protocol that lets an AI assistant such as Claude discover and call external tools. This tutorial wires up two MCP servers:

- The **StarRocks MCP server** (`mcp-server-starrocks`) exposes tools that let Claude read the schema and run SQL against StarRocks.
- The **AIStor MCP server** (`aistor`) exposes tools that let Claude work with the MinIO object storage where the data lives — for example, browsing buckets and inspecting the objects StarRocks writes.

### FE

Frontend nodes are responsible for metadata management, client connection management, query planning, and query scheduling.

### CN

Compute Nodes are responsible for executing query plans in shared-data deployments.

---

## Install the prerequisites

### `uv`

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or via Homebrew / pipx / pip
brew install uv
# pipx install uv
# pip install uv
```

See the [official `uv` installation guide](https://docs.astral.sh/uv/getting-started/installation/) for other options. After installing, verify it is on your `PATH`:

```bash
uv --version
```

### Docker

[https://www.docker.com/get-started/](https://www.docker.com/get-started/)

### Claude Code

[https://claude.com/product/claude-code](https://claude.com/product/claude-code)

---

## Clone the demo repo

Clone [https://github.com/StarRocks/demo](https://github.com/StarRocks/demo):

```bash
gh repo clone StarRocks/demo
```

or

```bash
git clone git@github.com:StarRocks/demo.git
```

:::note
This directory contains just enough to get you going. It deliberately contains *no* canned queries, no expected answers, and no walkthrough — so that everything Claude shows is reasoned from the live schema, not recalled from material in the checkout.
:::

---

## Bring up StarRocks and MinIO

```bash
cd demo/documentation-samples/MCP
docker compose up --detach --wait --wait-timeout 120
```

A frontend (FE), a compute node (CN), and MinIO come up locally.

Check for healthy status on the MinIO, FE, and CN services:

```bash
docker compose ps -a --format "table {{.Service}}\t{{.Status}}"
```

:::tip
If the CN is not reporting healthy just wait a few seconds and check again — it is the last service to start.
:::

---

## Create a bucket in MinIO

Open the MinIO console at [http://localhost:9001](http://localhost:9001) (login `miniouser` / `M!n10R0cks`), click **Create Bucket**, and create the bucket `my-starrocks-bucket`.

---

## Create the storage volume

The file `storage_volume.sql` creates a StarRocks storage volume in the bucket you created in the previous step and sets it as the default. The contents of the file are explained in detail at the end of this tutorial; for now, run it:

```bash
docker compose exec -T starrocks-fe \
  mysql -P9030 -h127.0.0.1 -uroot < storage_volume.sql
```

This must succeed (and the volume must be the default) before any `CREATE TABLE`.

```sql
*************************** 1. row ***************************
     Name: s3_volume
     Type: S3
IsDefault: true
 Location: s3://my-starrocks-bucket/
   Params: {"aws.s3.access_key":"******","aws.s3.secret_key":"******","aws.s3.endpoint":"minio:9000","aws.s3.region":"us-east-1","aws.s3.use_instance_profile":"false","aws.s3.use_web_identity_token_file":"false","aws.s3.use_aws_sdk_default_behavior":"false"}
  Enabled: true
  Comment:
```

---

## Create the tables

```bash
docker compose exec -T starrocks-fe \
  mysql -h127.0.0.1 -P9030 -uroot < olist_schema.sql
```

:::tip
If you see an error `The specified bucket does not exist` you may have skipped part of the previous step. Open the MinIO UI and create the bucket specified above.
:::

:::tip
This is a good time to try out the MCP server. Start Claude Code from the current directory and allow the two MCP servers (`aistor` and `mcp-server-starrocks`), then ask Claude to list the databases and describe the schema in the `olist` DB.
:::

---

## Load the data

Download the Olist CSVs from Kaggle with **kagglehub** — anonymous, no Kaggle account or API token required. It caches the files locally and prints the folder they landed in.

Download the data and capture the folder it landed in as `CSV_DIR`. kagglehub prints a version warning to stdout, so take the last line (the path) with `tail -n1`:

```bash
export CSV_DIR="$(uv run --with "kagglehub==0.3.12" python \
  -c "import kagglehub; print(kagglehub.dataset_download('olistbr/brazilian-ecommerce'))" \
  | tail -n1)"
echo "$CSV_DIR"   # sanity check: should be a .../brazilian-ecommerce/versions/N path
```

:::note
The `kagglehub==0.3.12` pin is deliberate: newer releases (1.0.x) pull a `kagglesdk` build that fails to import (`ModuleNotFoundError: kagglesdk.competitions.legacy`). 0.3.12 downloads public datasets anonymously and works fine. kagglehub caches the files, so re-running is cheap.
:::

Then run the loader (`CSV_DIR` is already exported):

```bash
FE_HTTP_PORT=8040 bash load_olist.sh
```

`FE_HTTP_PORT=8040` posts Stream Load straight to the CN, avoiding a `starrocks-cn` hostname redirect (no `/etc/hosts` edit / no sudo needed). The script prints a row-count check at the end.

---

## Wire the MCP servers to Claude

```bash
cp .env.example .env
```

`.mcp.json` defines two MCP servers — `mcp-server-starrocks` (run from GitHub via `uv`, so there's **nothing to install by hand**) and `aistor` (run via Docker) — both reading `.env`. Launch Claude Code from this directory (or add the same block to Claude Desktop's config).

:::note
**Approve the servers on first launch.** Project-scoped servers from `.mcp.json` are *not* trusted automatically — Claude Code prompts *"New MCP servers found — approve?"* the first time you launch here. Accept it. (The pre-approval lives in `.claude/settings.local.json`, which is gitignored, so it is **absent on a fresh clone** — that's expected; approving via the prompt recreates it.)

**Confirm the connection with `/mcp`.** Run `/mcp` inside Claude Code and check that both `mcp-server-starrocks` and `aistor` show **connected**. This is the authoritative status check — if a server isn't connected, its tools won't load no matter how you ask.

**First launch is slow.** The first `uv run` downloads the server's dependencies (pyarrow, kaleido, …, ~115 MB), so the StarRocks server can take a couple of minutes to come up the first time. Subsequent launches are fast.
:::

Once `/mcp` shows both servers connected, confirm the tools are available (e.g. ask *"What tools does the StarRocks MCP server provide?"*).

The StarRocks cluster is reachable over the MySQL protocol as `root` (no password) at `localhost:9030`, database `olist`.

---

## Ask questions

Ask Claude plain-English questions about the data — start by having it orient itself, e.g. *"What tables are in this database, and how do they relate to each other?"*, then explore from there. Claude inspects the live schema and writes the SQL itself.

### Suggested questions

These are the questions asked in the demo video, in order. Each one builds on the last — start with the first to let Claude orient itself, then work down:

- *What databases and tables are in here?*
- *How do these tables fit together?*
- *Where does this data actually live?*
- *Plot orders by customer state.*
- *Does it cost more to ship things farther?*
- *Add a second line — freight per kilo — to separate weight from distance.*
- *How did you build that query?*
- *Do late deliveries hurt our reviews?*
- *Are you using canned answers, or did every number come from the live tables?*

:::tip
**Charts:** ask for an interactive chart in **HTML** format and the StarRocks MCP server writes it to `DEMO_Output/` (set by `STARROCKS_CHART_OUTPUT_DIR` in `.env`), with a static preview shown inline — open the HTML file in a browser to hover / zoom / pan.
:::

---

## Troubleshooting

- **StarRocks MCP tools never load / server "still connecting":** the project servers were never approved. Run `/mcp` — if they aren't **connected**, relaunch Claude Code from this directory and accept the *"New MCP servers found — approve?"* prompt (or add `"enabledMcpjsonServers": ["mcp-server-starrocks", "aistor"]` to `.claude/settings.local.json`). You do **not** need to install the server separately — `.mcp.json` runs it from GitHub via `uv`.
- **First launch takes a couple of minutes:** the first `uv run` downloads the server's deps (~115 MB). This is expected once, not a failure.
- **`mysql` client fails with `Authentication plugin 'mysql_native_password' cannot be loaded`:** a Homebrew `mysql`-client quirk, not a StarRocks problem. Query through the container instead: `docker compose exec -T starrocks-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW DATABASES;"`.
- **`CREATE TABLE` hangs / times out:** the CN's AWS SDK is probing the EC2 metadata service. The compose file already sets `AWS_EC2_METADATA_DISABLED=true` on `starrocks-cn`; if you use your own compose file, add it.
- **Stream Load redirect errors:** use `FE_HTTP_PORT=8040` (as above) to post to the CN directly, or add `127.0.0.1 starrocks-cn` to `/etc/hosts`.
- **`order_reviews` rejects rows:** the free-text comment columns contain embedded newlines; use the LEAN variant noted in `load_olist.sh`.

---

## Summary

In this tutorial you:

- Deployed StarRocks shared-data (1 FE + 1 CN) and MinIO in Docker
- Created an S3 (MinIO) storage volume and set it as the default
- Loaded the Olist Brazilian E-Commerce dataset (8 related tables)
- Wired the StarRocks and AIStor MCP servers to Claude
- Asked Claude plain-English questions and let it discover the schema, write the SQL, and render charts

The build/setup material — narrated walkthrough and the video script — is maintained separately so this environment stays free of canned answers:

- **StarRocks MCP server:** [https://github.com/StarRocks/mcp-server-starrocks](https://github.com/StarRocks/mcp-server-starrocks)

---

## What's in here

The demo files live in the `documentation-samples/MCP` directory of the [StarRocks/demo](https://github.com/StarRocks/demo) repo:

| File | What it is |
|---|---|
| `CLAUDE.md` | Instructions to Claude Code to show its work, rely on the schema and not outside information, etc. You should read this file. |
| `.claude/settings.json` | Content is `"autoMemoryEnabled": false` to prevent Claude from remembering the questions (and answers) asked in previous sessions. You can remove this if you like; it is in here to prevent the author's questions from biasing your experimentation. |
| `docker-compose.yml` | StarRocks shared-data quick start (1 FE + 1 CN + MinIO), patched for laptop/Docker use. |
| `storage_volume.sql` | Creates the S3 (MinIO) storage volume and sets it as default. |
| `olist_schema.sql` | DDL for the 8 (+1 optional) Olist tables. |
| `load_olist.sh` | Stream Load for the Olist CSVs + a row-count check. |
| `.mcp.json` | Wires the StarRocks and AIStor MCP servers to Claude. |
| `.env.example` | Credentials/endpoints template — `cp` to `.env` and edit if needed. |

---

## Notes on `storage_volume.sql`

`storage_volume.sql` does two things: it raises a timeout, then creates and activates the storage volume.

First it increases the timeout for tablet creation and creates the storage volume:

```sql
-- default is 10 s, too tight for object-store tablet creation
ADMIN SET FRONTEND CONFIG ('tablet_create_timeout_second'='60');

CREATE STORAGE VOLUME s3_volume
    TYPE = S3
    LOCATIONS = ("s3://my-starrocks-bucket/")
    PROPERTIES (
        "enabled" = "true",
        "aws.s3.endpoint" = "minio:9000",
        "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
        "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
        "aws.s3.use_instance_profile" = "false",
        "aws.s3.use_aws_sdk_default_behavior" = "false"
    );
```

It then sets the new storage volume as the default and shows its details:

```sql
SET s3_volume AS DEFAULT STORAGE VOLUME;   -- REQUIRED before CREATE DATABASE/TABLE
DESC STORAGE VOLUME s3_volume\G            -- verify: enabled=true, IsDefault=true
```

---

## More information

- StarRocks shared-data quick start — [shared-data](shared-data.md)
- StarRocks MCP server — [https://github.com/StarRocks/mcp-server-starrocks](https://github.com/StarRocks/mcp-server-starrocks)
- AIStor MCP server — [https://github.com/minio/mcp-server-aistor](https://github.com/minio/mcp-server-aistor)
- Olist dataset — [https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

## Credits & license

- **Olist Brazilian E-Commerce** dataset via Kaggle, licensed **CC BY-NC-SA 4.0 (non-commercial)** — used here for educational demonstration; credit retained to Olist. Confirm terms before any commercial reuse.
- StarRocks and the StarRocks MCP server are open source (Apache-2.0 / project license).
- MinIO and `mcp-server-aistor` © MinIO, Inc.
