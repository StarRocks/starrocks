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

You can use identifiers such as `en`, `zh`, and `fr`, or language names the model understands. StarRocks does not define a fixed list of supported languages.

For the types and default behavior of `provider` and `model`, see [Common calling rules](ai_functions.mdx#common-calling-rules).

## Return value

Returns a nullable VARCHAR containing the translated text. Returns NULL if the input text is NULL at runtime. A runtime NULL source language is treated as automatic detection. A runtime NULL, empty, or ASCII-whitespace-only target language returns NULL without sending a request.

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
