---
displayed_sidebar: docs
description: "Generates an embedding vector for text by calling an OpenAI-compatible embeddings endpoint, and returns it as an array of floats."
---

# embedding

Generates an embedding vector for a text value by calling an OpenAI-compatible `/v1/embeddings` endpoint, and returns the result as an `ARRAY<FLOAT>`.

The function runs on the BE nodes and issues one synchronous HTTP request per row. Because the call executes on the BEs rather than on the FE leader, embedding compute for bulk inserts scales out across the cluster. It is primarily used by the semantic context (AgentBase) module to embed text at write time. See [Semantic context](../../../using_starrocks/semantic_context.md).

## Syntax

```SQL
embedding(text, config)
```

## Parameters

- `text`: the text to embed. Must be of type `VARCHAR`.
- `config`: the provider configuration. Must be of type `JSON`. The supported fields are:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `endpoint` | STRING | Yes | - | Full URL of the OpenAI-compatible embeddings endpoint, for example `https://api.openai.com/v1/embeddings`. |
| `model` | STRING | Yes | - | Embedding model name, for example `text-embedding-3-small`. |
| `api_key` | STRING | No | `""` | Bearer token used for authentication. When omitted or empty, no `Authorization` header is sent, which suits local or self-hosted providers that run without authentication. |
| `dimensions` | INT | No | `0` | Requested vector width. When greater than `0`, it is forwarded to the provider so the returned vector matches the configured width. When `0`, the provider's default dimensionality is used. |
| `timeout_ms` | BIGINT | No | `60000` | Per-request timeout in milliseconds. |

## Return value

Returns an `ARRAY<FLOAT>` — the embedding vector for the input text.

The result is `NULL` for a row in any of these cases:

- The `text` argument is `NULL`, or the `config` argument is `NULL`.
- The provider call fails (for example, the endpoint is unreachable, times out, or returns an unexpected response), or the provider returns an empty vector.

A failed or `NULL` row does not fail the query; only that row's value becomes `NULL`.

The whole statement fails only when the `config` argument is malformed — that is, it is not a JSON object, or it is missing the required `endpoint` or `model` string fields.

## Usage notes

- When `config` is a constant (the common case, where a single literal config is passed for the whole query), it is parsed once instead of per row.
- Each non-`NULL` row triggers one HTTP call, so the function is best used for batch embedding at write time rather than in latency-sensitive point queries.

## Examples

Embed a literal string with an inline config:

```SQL
SELECT embedding(
    'StarRocks is a fast analytical database.',
    PARSE_JSON('{
        "endpoint": "https://api.openai.com/v1/embeddings",
        "model": "text-embedding-3-small",
        "api_key": "sk-xxxxxxxx",
        "dimensions": 1536
    }')
);
```

Embed a column when loading data, writing the vectors into a target table:

```SQL
INSERT INTO docs_with_vectors
SELECT
    id,
    content,
    embedding(
        content,
        PARSE_JSON('{
            "endpoint": "https://api.openai.com/v1/embeddings",
            "model": "text-embedding-3-small",
            "dimensions": 1536
        }')
    )
FROM raw_docs;
```

Use a self-hosted provider that runs without authentication (omit `api_key`):

```SQL
SELECT embedding(
    'hello world',
    PARSE_JSON('{
        "endpoint": "http://127.0.0.1:8080/v1/embeddings",
        "model": "bge-small-en",
        "dimensions": 384
    }')
);
```

## keyword

EMBEDDING, VECTOR, AI, ARRAY
