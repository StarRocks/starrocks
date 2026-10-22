---
displayed_sidebar: docs
description: "Generates a text embedding using an `embedding` Provider with the `openai` protocol. Returns a nullable `ARRAY<FLOAT>`."
sidebar_position: 22
---

# ai_embed

Generates a text embedding using an `embedding` Provider with the `openai` protocol. Returns a nullable `ARRAY<FLOAT>`.

Both `USAGE ON AI FUNCTION <function_name>` and `USAGE ON AI PROVIDER <provider_name>` are required, including for the selected default Provider. See [AI function privileges](ai_functions.mdx#ai-function-privileges).

## Syntax

```sql
ai_embed(input)
ai_embed(input, options)
ai_embed(file)
ai_embed(file, options)
ai_embed(model, input)
ai_embed(model, input, options)
ai_embed(model, file)
ai_embed(model, file, options)
ai_embed(provider, model, input)
ai_embed(provider, model, input, options)
ai_embed(provider, model, file)
ai_embed(provider, model, file, options)
```

## Parameters

Omitting `provider` selects the cluster default Provider for the function type: `chat` for completion and text helpers, `embedding` for embeddings. Omitting `model` uses that Provider's required default model. An explicit model takes precedence and may vary by row. `provider` must be a nonblank constant string; a constant explicit model must not be blank. An explicit Provider call does not require a cluster default.

NULL text, explicit model, or FILE input returns NULL without a model request, whether it is a constant or a runtime column value. A NULL options MAP means no options; a NULL translation source language requests automatic detection. Provider names and helper arrays retain their parameter validation. Nested NULL values inside options retain their JSON-compatible semantics. If an untyped NULL matches different argument types across overloads, specify its type with `CAST`, for example `CAST(NULL AS VARCHAR)` or `CAST(NULL AS MAP<VARCHAR, JSON>)`.

`input` is a VARCHAR expression. A URL string is embedded as text without fetching it. The FILE overloads retain typed FILE validation, but the supported OpenAI embedding protocol accepts text only: FILE input cannot generate an embedding and follows `ai_function_on_error` for unsupported input. Reference FILE inputs fail with `NotSupported`.

Provider `dimensions` is the default; SQL options may override it. The provider and model must support the requested dimension. The recursive MAP rules of [ai_complete](ai_complete.md#options-map-rules) apply, with `input` and `encoding_format` reserved instead of `messages` and `stream`. Connection/model/protocol keys remain reserved.

The response must contain exactly one embedding at index 0, with a nonempty array of finite FLOAT values. Invalid typed responses follow `ai_function_on_error`. The [runtime controls](ai_complete.md#runtime-limits-and-retries) and [SQL restrictions](ai_complete.md#limitations-and-security) apply. Embedding attempts use `ai_function_rate_limit_qps_embedding`.

## Configuration

Create a Provider with `protocol = "openai"`. Both HTTP and HTTPS endpoints are supported, with or without an API key. A nonempty key produces an `Authorization: Bearer ...` header. URL format, control-character, DNS, and address validation still apply. Use a trusted endpoint and data approved for transmission.

Set a default with `SET <provider_name> AS DEFAULT AI PROVIDER`. If the corresponding default is absent, FE rejects implicit Provider calls, including `EXPLAIN`. Each execution plan captures its selected Provider configuration. ALTER and default changes affect newly planned queries; each prepared `EXECUTE` replans against current metadata.

## Examples

```sql
CREATE AI PROVIDER text_embedding TYPE embedding
PROPERTIES ("endpoint" = "http://localhost:8000/v1/embeddings",
            "model" = "embedding-model", "protocol" = "openai", "dimensions" = "256");
SET text_embedding AS DEFAULT AI PROVIDER;

EXPLAIN SELECT ai_embed('A local test sentence.');
EXPLAIN SELECT ai_embed('another-model', 'A local test sentence.');
EXPLAIN SELECT ai_embed('text_embedding', 'another-model', 'A local test sentence.',
                       map{'dimensions': 128});
EXPLAIN SELECT ai_embed('text_embedding', 'another-model', CAST(NULL AS VARCHAR));
EXPLAIN SELECT ai_embed('another-model', 'text', CAST(NULL AS MAP<VARCHAR, JSON>));
```
