---
displayed_sidebar: docs
description: "Evaluate business state with a registered decision Provider and return validated JSON answers."
sidebar_position: 70
---

# ai_decide

Calls an administrator-configured Alibaba Cloud Model Studio decision model and returns its complete, validated JSON response. Questions can request classification (`choice`), a yes probability (`noul`), or an ordered score (`score`). The request uses the [System One decision API](https://help.aliyun.com/zh/model-studio/decision-model-api).

## Syntax

```sql
ai_decide(state VARCHAR, questions JSON) -> JSON
ai_decide(state JSON, questions JSON) -> JSON
ai_decide(provider_name VARCHAR, state VARCHAR, questions JSON) -> JSON
ai_decide(provider_name VARCHAR, state JSON, questions JSON) -> JSON
```

- `state`: A VARCHAR/CHAR or JSON expression. Text must be valid UTF-8 and is sent as text, even if it resembles JSON or a URL. JSON state must be a string, object, or array. Use an explicit cast or `parse_json` to convert other types.
- `questions`: A constant JSON object defining 1–16 questions. A JSON column or `parse_json(column)` is rejected. A string is not implicitly accepted as JSON.
- `provider_name`: An optional constant, non-NULL, nonblank VARCHAR expression naming a registered Provider. Omit it to select the default `decision` Provider. The remote model always comes from that Provider; this argument is not a remote model name.

There is no options MAP. Both forms require a Provider of type `decision` and protocol `bailian_decision`. They use the existing [AI function privileges](ai_functions.mdx#ai-function-privileges), SQL placement restrictions, asynchronous AIProject execution, and request lifecycle. Upgrade all FE and BE nodes before creating or using a decision Provider.

## Configuration

Register the model through [CREATE AI PROVIDER](../../sql-statements/cluster-management/ai_provider/CREATE_AI_PROVIDER.md). An administrator can set its per-type default; this does not change the chat, embedding, or rerank defaults. For an existing role:

```sql
CREATE AI PROVIDER support_decision TYPE decision
PROPERTIES (
    "endpoint" = "https://YOUR_WORKSPACE_ID.cn-beijing.maas.aliyuncs.com/compatible-mode/v1/systemone",
    "model" = "decision-model-preview",
    "protocol" = "bailian_decision",
    "api_key" = "<api-key>"
);
SET support_decision AS DEFAULT AI PROVIDER;

GRANT USAGE ON AI FUNCTION ai_decide TO ROLE decision_reader;
GRANT USAGE ON AI PROVIDER support_decision TO ROLE decision_reader;
```

Endpoint, model, credential, and optional `timeout_ms` come from Provider metadata. No decision FE configuration or BE-local decision credential is required, and calls never fall back to SYSTEM chat or embedding settings. Requests carrying a credential require HTTPS. Provider keys are persisted in FE metadata and sent to executing BEs through the existing internal RPC path; SHOW/DESC and diagnostic rendering mask them. See [Provider settings and snapshots](ai_custom_functions.md#query-snapshots-and-system-compatibility).

A plan fixes its first selection for the default Provider or each explicit name and captures an immutable snapshot by Provider UUID. Default and explicit selections of the same UUID share that snapshot. Later ALTER, DROP, or default changes do not redirect the plan; newly built plans resolve current metadata and permissions. Missing Providers, wrong types/protocols, or missing function/Provider USAGE privileges fail without SYSTEM fallback. Table privileges still apply. State and questions are sent to the configured service. Model access and regional availability depend on the provider account; see the [model documentation](https://help.aliyun.com/zh/model-studio/decision-model-preview).

## Questions and results

Each question ID must be nonempty and unique. A definition accepts only `type`, optional string `instructions`, and `criteria` as described below.

| Type | Criteria | Answer |
| --- | --- | --- |
| `choice` | Required object with 1–255 unique nonempty option names mapped to string descriptions. | A `choice` label, normalized `probabilities` for every option, and `confidence` in `[0,1]`. |
| `noul` | Optional object with string descriptions for `true` and/or `false`. | `noul` is P(yes) in `[0,1]`, not a BOOLEAN. The caller chooses any threshold. |
| `score` | Required array of 2–255 string descriptions in increasing order. | A possibly fractional zero-based level index in `[0,N-1]`, with `legend`, normalized `probabilities`, and `confidence`. |

The response must contain exactly the requested question IDs with matching answer types and valid ranges. The full response, including optional provider metadata, is preserved after validation. JSON conversion before the function may already normalize duplicate input keys; raw provider responses with duplicate keys are rejected.

```sql
SELECT ai_decide(
    'The customer reports a duplicate charge and asks for a refund.',
    parse_json('{
      "category":{"type":"choice","criteria":{"refund":"Refund or duplicate charge","other":"Other requests"}},
      "refund":{"type":"noul","instructions":"Is a refund explicitly requested?"},
      "urgency":{"type":"score","criteria":["Routine","Soon","Immediate"]}
    }')
);
```

Use ordinary JSON expressions to extract fields such as `result -> '$.answers.refund.noul'`. Each evaluated non-NULL row submits one logical request containing all questions; retries can add HTTP attempts. The function is nondeterministic.

## NULL, errors, and usage

SQL NULL in state or questions returns SQL NULL without a request. A NULL or blank explicit Provider name is an analysis error, and never selects the default. When an AIProject is actually initialized, invalid constant question definitions fail during fragment initialization, even if it subsequently receives no rows or its state is SQL NULL. Calls removed by the optimizer, for example by `WHERE 1=0` or `LIMIT 0`, are not initialized for question validation. Function and Provider authorization still follow the existing pre-optimization checks. Native JSON null is distinct from SQL NULL and is not a valid state or question definition.

Row input errors, HTTP failures, and invalid responses follow [ai_function_on_error](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error): `ignore` returns SQL NULL for the failed row, while `fail` aborts the query. Invalid configuration and query lifecycle failures retain the existing AI execution errors. An error is never interpreted as a negative decision or a zero score.

Valid reported `usage.input_tokens` contributes prompt and total tokens, with zero completion tokens. Trusted usage is retained even when answer validation fails. Missing usage stays unknown; malformed, duplicate, negative, or overflowing usage is not counted. No token estimate is introduced. Existing AI request, retry, failure, and usage profile counters cover these calls.

[ai_function_rate_limit_qps_decision](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_decision) defaults to 20 HTTP attempts per second per BE, endpoint, credential, and decision capability, including retries. It must be positive and can be updated live. The global AI in-flight limit also applies. This is not a cluster-wide provider quota.
