---
displayed_sidebar: docs
description: "Evaluate business state with a SYSTEM decision model and return validated JSON answers."
sidebar_position: 70
---

# ai_decide

Calls an administrator-configured Alibaba Cloud Model Studio decision model and returns its complete, validated JSON response. Questions can request classification (`choice`), a yes probability (`noul`), or an ordered score (`score`). The request uses the [System One decision API](https://help.aliyun.com/zh/model-studio/decision-model-api).

## Syntax

```sql
ai_decide(state VARCHAR, questions JSON) -> JSON
ai_decide(state JSON, questions JSON) -> JSON
ai_decide(model VARCHAR, state VARCHAR, questions JSON) -> JSON
ai_decide(model VARCHAR, state JSON, questions JSON) -> JSON
```

- `state`: A VARCHAR/CHAR or JSON expression. Text must be valid UTF-8 and is sent as text, even if it resembles JSON or a URL. JSON state must be a string, object, or array. Use an explicit cast or `parse_json` to convert other types.
- `questions`: A constant JSON object defining 1–16 questions. A JSON column or `parse_json(column)` is rejected. A string is not implicitly accepted as JSON.
- `model`: An optional constant VARCHAR expression. When omitted, uses `ai_default_decision_model`. It changes only the model, not the endpoint, protocol, or credentials. SQL NULL does not fall back to the default.

This function has no options MAP or AI PROVIDER overload. It uses the existing [AI function privileges](ai_functions.mdx#ai-function-privileges), placement restrictions, asynchronous AIProject execution, and request lifecycle. Upgrade all FE and BE nodes before using these new overloads.

## Configuration

Set the independent decision configuration on every FE:

```properties
ai_default_decision_endpoint = https://YOUR_WORKSPACE_ID.cn-beijing.maas.aliyuncs.com/compatible-mode/v1/systemone
ai_default_decision_provider = bailian_decision
ai_default_decision_model = decision-model-preview
```

All three settings default to empty. Endpoint and provider are required; the default model may remain empty when every call supplies a model. See [FE decision configuration](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_decision_endpoint). Changes apply to newly planned queries; existing plans retain their configuration snapshot.

Every executing BE must set `AI_FUNCTION_DECISION_ENDPOINT` to exactly the FE endpoint and provide the matching `AI_FUNCTION_DECISION_API_KEY` in its process environment. Restart the BE after changing these environment variables. FE plans contain no SYSTEM API key. Decision credentials and endpoint do not fall back to chat or embedding settings. The endpoint must be an approved HTTPS URL; state and question definitions are sent to that service.

For an existing role:

```sql
GRANT USAGE ON AI FUNCTION ai_decide TO ROLE decision_reader;
```

Normal table privileges still apply. Model access and regional availability depend on the configured provider account; see the [model documentation](https://help.aliyun.com/zh/model-studio/decision-model-preview).

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

SQL NULL in state, questions, or explicit model returns SQL NULL without a request. When an AIProject is actually initialized, invalid constant question definitions fail during fragment initialization, even if it subsequently receives no rows or its state/model is SQL NULL. Calls removed by the optimizer, for example by `WHERE 1=0` or `LIMIT 0`, are not initialized or validated. Native JSON null is distinct from SQL NULL and is not a valid state or question definition.

Row input errors, HTTP failures, and invalid responses follow [ai_function_on_error](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error): `ignore` returns SQL NULL for the failed row, while `fail` aborts the query. Invalid configuration and query lifecycle failures retain the existing AI execution errors. An error is never interpreted as a negative decision or a zero score.

Valid reported `usage.input_tokens` contributes prompt and total tokens, with zero completion tokens. Trusted usage is retained even when answer validation fails. Missing usage stays unknown; malformed, duplicate, negative, or overflowing usage is not counted. No token estimate is introduced. Existing AI request, retry, failure, and usage profile counters cover these calls.

[ai_function_rate_limit_qps_decision](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_decision) defaults to 20 HTTP attempts per second per BE, endpoint, credential, and decision capability, including retries. It must be positive and can be updated live. The global AI in-flight limit also applies. This is not a cluster-wide provider quota.
