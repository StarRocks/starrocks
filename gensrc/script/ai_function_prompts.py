# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re


# The BE prompt input contracts, not SQL function registrations. Overloads bind
# argument positions in functions.py. Substitution order stays in build_prompt().
AI_PROMPT_INPUT_TYPES = {
    'PASSTHROUGH': ['VARCHAR'],
    'SENTIMENT': ['VARCHAR'],
    'CLASSIFY': ['VARCHAR', 'ARRAY_VARCHAR'],
    'EXTRACT': ['VARCHAR', 'ARRAY_VARCHAR'],
    'FIX_GRAMMAR': ['VARCHAR'],
    'REDACT': ['VARCHAR', 'ARRAY_VARCHAR'],
    'TRANSLATE': ['VARCHAR', 'VARCHAR', 'VARCHAR'],
    'SIMILARITY': ['VARCHAR', 'VARCHAR'],
    'SUMMARIZE': ['VARCHAR'],
    'FILTER': ['VARCHAR', 'VARCHAR'],
}

# These user-prompt literals generate both BE templates and FE fixed byte counts.
# Keep placeholders in strings::Substitute order, as used by build_prompt().
AI_PROMPT_TEMPLATES = {
    'PASSTHROUGH': '$0',
    'SENTIMENT': (
        'Analyze the overall sentiment of the following text. '
        'Output exactly one lowercase word from this list: positive, negative, neutral, mixed, unknown. '
        'No punctuation, no explanation.\n\nText: $0'),
    'CLASSIFY': (
        'Classify the following text into exactly one of these categories: $0.\n'
        'Return a JSON object in this exact format: {"labels": ["<chosen_category>"]}\n'
        'The array must contain exactly one string that matches one of the given categories.\n'
        'Output only valid JSON, no markdown, no explanation.\n\nText: $1'),
    'EXTRACT': (
        'Extract a value for each of the following keys from the text below.\n'
        "Keys: $0\nFor each key, extract exactly one value. If a key's value is not found, use null.\n"
        'Return a JSON object in this exact format: {"response": {"key1": "value1", "key2": null}}\n'
        'Output only valid JSON, no markdown, no explanation.\n\nText: $1'),
    'FIX_GRAMMAR': (
        'Fix the grammar and spelling of the following text. '
        'Preserve the original meaning and tone. Output only the corrected text, nothing else.\n\nText: $0'),
    'REDACT': (
        'Redact personally identifiable information (PII) in the text below.\n'
        'Categories to redact: $0\n'
        'Replace each detected PII value with its uppercase category name in square brackets, '
        'e.g. [NAME], [ADDRESS], [EMAIL], [PHONE], [SSN].\n'
        'If no PII is found, return the original text unchanged. '
        'Output only the redacted text, nothing else.\n\nText: $1'),
    'TRANSLATE': (
        'Translate the following text from $0 into $1. '
        'Preserve the original meaning and tone. Output only the translated text, nothing else.\n\nText: $2'),
    'SIMILARITY': (
        'Calculate the semantic similarity between the following two texts.\n'
        'Output only a single decimal number between 0.00 and 1.00 (0 = completely different, '
        '1 = identical meaning). No explanation, no extra text.\n\nText 1: $0\nText 2: $1'),
    'SUMMARIZE': (
        'Summarize the following text concisely, capturing the key points. '
        'Output only the summary, nothing else.\n\nText: $0'),
    'FILTER': (
        'Given the following text, determine if this condition is true. '
        'You MUST respond with exactly true or false and nothing else.\n'
        'Text: $0\nCondition: $1'),
}

# The auto-detect branch substitutes target language and text; source is omitted.
AI_TRANSLATE_AUTO_DETECT_TEMPLATE = (
    'Translate the following text into $0. Auto-detect the source language. '
    'Preserve the original meaning and tone. Output only the translated text, nothing else.\n\nText: $1')


def prompt_fixed_bytes(template, input_count):
    placeholders = re.findall(r'\$[0-9]+|\$', template)
    if sorted(placeholders) != [f'${index}' for index in range(input_count)]:
        raise ValueError('AI prompt must use each input placeholder exactly once')
    return len(re.sub(r'\$[0-9]+', '', template).encode('utf-8'))


def validate_ai_prompt_templates(templates=None):
    templates = AI_PROMPT_TEMPLATES if templates is None else templates
    if templates.keys() != AI_PROMPT_INPUT_TYPES.keys():
        raise ValueError('AI prompt templates must cover exactly the supported prompt kinds')
    for kind, template in templates.items():
        prompt_fixed_bytes(template, len(AI_PROMPT_INPUT_TYPES[kind]))
    prompt_fixed_bytes(AI_TRANSLATE_AUTO_DETECT_TEMPLATE, 2)
