---
displayed_sidebar: docs
description: "Translates text from the source language into the target language."
products: [oss, enterprise, cloud, anywhere]
---

# ai_translate

Translates text from the source language into the target language.

## Syntax

```sql
ai_translate(input, source_language, target_language[, provider[, model]])
```

## Arguments

- `input`: A VARCHAR expression containing the text to translate.
- `source_language`: A VARCHAR expression specifying the source language. Use an empty string `''` to request automatic detection.
- `target_language`: A VARCHAR expression specifying the target language.

Use a language code such as `en`, `zh`, or `fr`, or a language name the model recognizes. Available languages depend on the model. StarRocks has no fixed language list.

For the types and default behavior of `provider` and `model`, see [Common calling rules](ai_functions.mdx#common-calling-rules).

## Return value

Returns the translated text as a nullable VARCHAR. If `input` evaluates to NULL, the function returns NULL. If `source_language` evaluates to NULL, the model detects the source language. If `target_language` evaluates to NULL, an empty string, or a string containing only ASCII whitespace, the function returns NULL without sending a request.

All three semantic arguments are required and cannot be bare NULL literals. To detect the source language automatically, explicitly pass an empty string.

## Examples

```sql
SELECT ai_translate('Hello', '', 'zh');

SELECT ai_translate(
    input => 'Bonjour',
    source_language => 'fr',
    target_language => 'en',
    provider => 'support_chat'
);
```
