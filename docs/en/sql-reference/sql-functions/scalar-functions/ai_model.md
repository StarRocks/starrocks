---
displayed_sidebar: docs
description: "Create, manage, and authorize cluster-wide AI models for external chat and text embedding services."
sidebar_position: 23
---

# AI models

An AI model is an independent, cluster-wide object. It is not a Resource and does not belong to a database or catalog. Its case-sensitive name identifies an approved external inference service, not a trained model stored in StarRocks. It stores a capability, provider, complete endpoint, provider model name, and a reference to a BE-local credential. It never stores an API key.

Upgrade every FE and BE before creating AI models or using the new functions. Older nodes cannot interpret the new journal operations, privileges, or execution metadata. Mixed-version use and downgrade after creating this metadata are not supported; dropping models alone does not remove their journal or privilege history.

:::warning
Inference sends input data outside the cluster and can incur charges. Grant CREATE AI MODEL only to trusted administrators: they can select operator-provisioned credential references. Neither CREATE nor EXPLAIN contacts the provider.
:::

## Create and manage

```sql
CREATE AI MODEL [IF NOT EXISTS] model_name
[COMMENT 'description']
PROPERTIES (
    "capability" = "CHAT",
    "provider" = "openai_compatible",
    "endpoint" = "https://models.example.com/v1/chat/completions",
    "model" = "approved-chat-model",
    "credential_ref" = "SUPPORT"
);

ALTER AI MODEL [IF EXISTS] model_name SET ("model" = "approved-chat-model-v2");
ALTER AI MODEL [IF EXISTS] model_name COMMENT = 'Updated description';
SHOW AI MODELS [LIKE 'pattern'];
DESC AI MODEL model_name;
DROP AI MODEL [IF EXISTS] model_name;
```

Square brackets above denote optional syntax. Use an unqualified identifier, such as `support_chat`, for `model_name`. CREATE requires all five properties shown. Unknown properties, including `type` and raw `api_key`, are rejected. CREATE OR REPLACE and RENAME are not supported.

| Property | Requirement |
|----------|-------------|
| `capability` | Exactly `CHAT` or `TEXT_EMBEDDING`. Immutable after creation. |
| `provider` | `openai_compatible`. |
| `endpoint` | Complete HTTPS POST URL with a host and a valid explicit port (1–65535). No user information, query string, fragment, or control characters. StarRocks does not append a path. |
| `model` | Nonblank provider model name without control characters. |
| `credential_ref` | `[A-Z0-9_]{1,64}`. Immutable after creation. This is a public reference, not a secret. |

ALTER validates the complete replacement before publishing a new immutable revision. A failed update leaves the original model unchanged. Repeating existing values is a no-op. An endpoint change must also match the BE-local credential binding before new queries execute. To change capability or credential reference, create a new model.

SHOW returns names visible to the caller. DESC returns Id, Name, Revision, Capability, Provider, Endpoint, Model, CredentialRef, and Comment; no secret is stored or returned.

For text embeddings, create a model with `"capability" = "TEXT_EMBEDDING"` and a complete embeddings endpoint, for example `https://models.example.com/v1/embeddings`.

### BE-local credential binding

For each reference `<REF>`, provision these process environment variables on every executing BE:

- `AI_FUNCTION_CREDENTIAL_<REF>_ENDPOINT`: Must exactly equal the model endpoint.
- `AI_FUNCTION_CREDENTIAL_<REF>_API_KEY`: The corresponding Bearer credential.

For `credential_ref = SUPPORT`, use `AI_FUNCTION_CREDENTIAL_SUPPORT_ENDPOINT` and `AI_FUNCTION_CREDENTIAL_SUPPORT_API_KEY`. Only this fixed naming scheme is accepted. Restart affected BEs after changing environment variables. Missing bindings or endpoint mismatches fail execution; named models never fall back to SYSTEM credentials. Endpoint DNS validation and address pinning follow [ai_complete](ai_complete.md#be-local-credential).

## Authorization

The new object uses the existing roles, GRANT/REVOKE, native authorization, and Ranger framework. There is no implicit PUBLIC USAGE grant and no database-owner shortcut. For existing users:

```sql
GRANT CREATE AI MODEL ON SYSTEM TO 'model_admin'@'%';
GRANT USAGE ON AI MODEL support_chat TO 'analyst'@'%';
GRANT ALTER, DROP ON AI MODEL support_chat TO 'model_admin'@'%';
GRANT USAGE ON ALL AI MODELS TO 'ai_service'@'%';
REVOKE USAGE ON AI MODEL support_chat FROM 'analyst'@'%';
```

CREATE requires SYSTEM CREATE AI MODEL. ALTER and DROP require their respective model privileges. SHOW/DESC require any privilege on the model. Invocation requires USAGE, including nested queries, ordinary and security views, and each prepared-statement execution. A security view's table privileges do not substitute for the invoker's model USAGE privilege.

Native grants identify the model by stable ID. Deleting and recreating the same name produces a new ID and does not restore object-specific grants. Wildcard grants continue to apply. Ranger policies use the `ai_model` resource name, so a matching name policy can authorize a recreated name. Update the Ranger StarRocks service definition to include this resource; there is no fallback to native authorization on a Ranger denial.

## Invoke a model

```sql
ai_custom_query(model_name, text)
ai_custom_query(model_name, text, options)
ai_custom_embedding(model_name, text)
ai_custom_embedding(model_name, text, options)
```

- `model_name`: Constant, non-NULL, nonblank VARCHAR expression resolving to the exact AI model name. It is not a row-varying provider model name.
- `text`: VARCHAR expression. URL strings remain text; FILE, image, and multimodal inputs are not supported.
- `options`: Optional constant MAP. A typed NULL MAP is empty. Chat follows the [ai_complete options rules](ai_complete.md#options-map-rules); embedding follows [ai_embed](ai_embed.md#syntax).
- `ai_custom_query` requires CHAT and returns nullable VARCHAR. `ai_custom_embedding` requires TEXT_EMBEDDING and returns nullable `ARRAY<FLOAT>`. Missing models and capability mismatches fail before execution.

NULL text does not submit a provider request. Existing [error policy and SQL restrictions](ai_complete.md#limitations-and-security) apply. These calls require no SYSTEM chat or embedding defaults.

All named models referenced by a statement are captured together before authorization. Authorization and all AIProject nodes use those same immutable model revisions. ALTER or DROP after binding does not change an already authorized plan; subsequent planning uses current metadata. Prepared statements are replanned for every execution. Revoke/drop does not retroactively cancel an in-flight query.

Query dumps do not capture AI model metadata. Offline replay of named-model calls is not supported; replay must not substitute a live model with the same name for an uncaptured snapshot.

```sql
EXPLAIN SELECT ai_custom_query('support_chat', 'A local test prompt.');
EXPLAIN SELECT ai_custom_embedding('search_embedding', 'A local test sentence.');
```

EXPLAIN validates model metadata and permissions without sending HTTP requests. Existing SYSTEM `ai_complete`, text helpers, and `ai_embed` keep their current routing and do not resolve an AI model object.
