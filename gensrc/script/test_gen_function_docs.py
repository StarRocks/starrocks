#!/usr/bin/env python
# encoding: utf-8

"""
  Copyright 2021-present StarRocks, Inc. All rights reserved.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
"""

"""Tests for gen_function_docs.py.

Run with:  python3 -m unittest test_gen_function_docs -v
"""

import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import functions
import gen_function_docs as gfd


DOC = {
    "category": "bit-functions",
    "description": "Returns the bitwise AND of two numeric expressions.",
    "syntax": "bitand(x, y)",
    "arguments": [("x", "INT", "The left operand."), ("y", "INT", "The right operand.")],
    "returned_value": ("Same as `x`", "The bitwise AND."),
    "examples": [("", "SELECT bitand(3, 0);", "+---+\n| 0 |\n+---+")],
}

PAGE = """---
displayed_sidebar: docs
description: "Returns the bitwise AND of two numeric expressions."
---

# bitand

Returns the bitwise AND of two numeric expressions.

## Syntax

```Haskell
BITAND(x,y);
```

## Parameters

- `x`: an integer.

## Return value

The return value has the same type as `x`.

## Examples

```Plain Text
mysql> select bitand(3,0);
```

## keyword

BITAND
"""


class SectionSplitting(unittest.TestCase):
    def test_finds_level_two_headings(self):
        _, sections = gfd.split_sections(PAGE)
        ids = [section_id for section_id, _, _ in sections]
        self.assertEqual(ids, ["syntax", "parameters", "returns", "examples", None])

    def test_preamble_keeps_frontmatter_and_intro(self):
        preamble, _ = gfd.split_sections(PAGE)
        text = "\n".join(preamble)
        self.assertIn("displayed_sidebar: docs", text)
        self.assertIn("# bitand", text)

    def test_heading_inside_fence_is_not_a_heading(self):
        page = "# f\n\n## Examples\n\n```sql\n-- a fenced block\n## not a heading\n```\n"
        _, sections = gfd.split_sections(page)
        self.assertEqual(len(sections), 1)
        self.assertIn("## not a heading", "\n".join(sections[0][2]))

    def test_localized_and_english_headings_both_identified(self):
        self.assertEqual(gfd._identify("Syntax"), "syntax")
        self.assertEqual(gfd._identify("语法"), "syntax")
        self.assertEqual(gfd._identify("構文"), "syntax")
        self.assertEqual(gfd._identify("Return value"), "returns")
        self.assertEqual(gfd._identify("戻り値"), "returns")
        self.assertEqual(gfd._identify("keyword"), None)
        self.assertEqual(gfd._identify("References"), None)


class Generation(unittest.TestCase):
    def test_replaces_sections_in_place_without_duplicating(self):
        out = gfd.generate_page("bitand", DOC, "en", PAGE)
        self.assertEqual(out.count("## Syntax"), 1)
        self.assertEqual(out.count("## Examples"), 1)
        self.assertIn("bitand(x, y)", out)
        self.assertNotIn("BITAND(x,y);", out)

    def test_preserves_hand_written_sections(self):
        out = gfd.generate_page("bitand", DOC, "en", PAGE)
        self.assertIn("## keyword", out)
        self.assertIn("BITAND\n", out)

    def test_preserves_frontmatter_and_intro_untouched(self):
        out = gfd.generate_page("bitand", DOC, "en", PAGE)
        self.assertTrue(out.startswith("---\ndisplayed_sidebar: docs\n"))
        self.assertIn("# bitand", out)

    def test_section_order_is_preserved(self):
        out = gfd.generate_page("bitand", DOC, "en", PAGE)
        order = [gfd._identify(m) for m in re.findall(r"^## (.+)$", out, re.MULTILINE)]
        self.assertEqual(order, ["syntax", "parameters", "returns", "examples", None])

    def test_is_idempotent(self):
        once = gfd.generate_page("bitand", DOC, "en", PAGE)
        twice = gfd.generate_page("bitand", DOC, "en", once)
        self.assertEqual(once, twice)

    def test_markers_wrap_each_generated_section(self):
        out = gfd.generate_page("bitand", DOC, "en", PAGE)
        for section in ("syntax", "parameters", "returns", "examples"):
            self.assertIn(gfd.MARKER_START % section, out)
            self.assertIn(gfd.MARKER_END % section, out)
        # Markers must not accumulate on repeated runs.
        twice = gfd.generate_page("bitand", DOC, "en", out)
        self.assertEqual(twice.count(gfd.MARKER_START % "syntax"), 1)

    def test_inserts_missing_section_in_canonical_order(self):
        page = "# f\n\n## Syntax\n\nold\n\n## Examples\n\nold\n"
        doc = dict(DOC)
        out = gfd.generate_page("f", doc, "en", page)
        order = [gfd._identify(m) for m in re.findall(r"^## (.+)$", out, re.MULTILINE)]
        # parameters and returns did not exist; they belong between the two.
        self.assertEqual(order, ["syntax", "parameters", "returns", "examples"])

    def test_missing_section_appended_when_no_successor_exists(self):
        page = "# f\n\n## Syntax\n\nold\n"
        out = gfd.generate_page("f", DOC, "en", page)
        order = [gfd._identify(m) for m in re.findall(r"^## (.+)$", out, re.MULTILINE)]
        self.assertEqual(order, ["syntax", "parameters", "returns", "examples"])

    def test_generated_section_inserted_before_trailing_hand_written(self):
        page = "# f\n\n## Syntax\n\nold\n\n## keyword\n\nF\n"
        out = gfd.generate_page("f", DOC, "en", page)
        headings = re.findall(r"^## (.+)$", out, re.MULTILINE)
        self.assertEqual(headings[-1], "keyword")


class LocalizedGeneration(unittest.TestCase):
    def test_zh_gets_only_language_neutral_sections(self):
        page = "# f\n\n## 语法\n\nold\n\n## 参数说明\n\n手写\n\n## 示例\n\nold\n"
        out = gfd.generate_page("f", DOC, "zh", page)
        self.assertIn("## 语法", out)
        self.assertIn("## 示例", out)
        # Argument prose stays with the translation workflow.
        self.assertIn("手写", out)
        self.assertNotIn("The left operand.", out)

    def test_english_headings_in_localized_page_are_normalized(self):
        # Four of the seven ja bit-functions pages were written with English
        # headings; generation must replace them rather than add a duplicate.
        page = "# f\n\n## Syntax\n\nold\n\n## Examples\n\nold\n"
        out = gfd.generate_page("f", DOC, "ja", page)
        self.assertIn("## 構文", out)
        self.assertIn("## 例", out)
        self.assertNotIn("## Syntax", out)
        self.assertNotIn("## Examples", out)

    def test_localized_pages_keep_hand_written_trailing_sections(self):
        page = "# f\n\n## 構文\n\nold\n\n## 参考文献\n\n- link\n"
        out = gfd.generate_page("f", DOC, "ja", page)
        self.assertIn("## 参考文献", out)
        self.assertIn("- link", out)


class Anchors(unittest.TestCase):
    def test_latin_and_cjk_slugs(self):
        self.assertEqual(gfd.anchor("Return value"), "return-value")
        self.assertEqual(gfd.anchor("Examples"), "examples")
        self.assertEqual(gfd.anchor("例"), "例")
        self.assertEqual(gfd.anchor("示例"), "示例")

    def test_renaming_a_heading_carries_its_inbound_links(self):
        page = "# f\n\n## Usage notes\n\nSee [Examples](#examples).\n\n## Examples\n\nold\n"
        out = gfd.generate_page("f", DOC, "ja", page)
        self.assertIn("(#例)", out)
        self.assertNotIn("(#examples)", out)

    def test_dangling_anchor_is_detected(self):
        page = "# f\n\n## Examples\n\nSee [gone](#nowhere).\n"
        self.assertEqual(gfd.dangling_anchors(page), ["nowhere"])

    def test_resolved_anchor_is_not_reported(self):
        page = "# f\n\n## Usage notes\n\nSee [x](#examples).\n\n## Examples\n\nx\n"
        self.assertEqual(gfd.dangling_anchors(page), [])

    def test_every_generated_page_has_resolvable_anchors(self):
        bad = []
        for name, doc in functions.function_docs.items():
            for lang in gfd.LANGUAGES:
                path = gfd.page_path(name, doc, lang)
                if not os.path.exists(path):
                    continue
                with open(path) as handle:
                    broken = gfd.dangling_anchors(handle.read())
                if broken:
                    bad.append("%s/%s: %s" % (lang, name, broken))
        self.assertEqual(bad, [])


class HeadingNormalization(unittest.TestCase):
    def test_non_generated_localized_heading_is_normalized(self):
        page = "# f\n\n## Syntax\n\nold\n\n## Parameters\n\n手写内容\n\n## Examples\n\nold\n"
        out = gfd.generate_page("f", DOC, "zh", page)
        self.assertIn("## 参数说明", out)
        self.assertNotIn("## Parameters", out)
        # The body is a translation concern and must survive untouched.
        self.assertIn("手写内容", out)

    def test_normalization_does_not_generate_body_for_that_section(self):
        page = "# f\n\n## Parameters\n\n手写内容\n"
        out = gfd.generate_page("f", DOC, "zh", page)
        self.assertNotIn("The left operand.", out)

    def test_unknown_headings_are_left_alone(self):
        page = "# f\n\n## Syntax\n\nold\n\n## 参考文献\n\n- link\n"
        out = gfd.generate_page("f", DOC, "ja", page)
        self.assertIn("## 参考文献", out)


class Captions(unittest.TestCase):
    ZH = """# reverse

## 语法

```sql
reverse(param)
```

## 示例

反转字符串。

```Plain Text
MySQL > SELECT REVERSE('hello');
+---+
```

反转数组。

```Plain Text
MYSQL> SELECT REVERSE([4,1,5,8]);
+---+
```
"""

    def test_extracts_one_caption_per_example(self):
        self.assertEqual(gfd.existing_captions(self.ZH), ["反转字符串。", "反转数组。"])

    def test_paired_sql_and_result_fences_count_as_one_example(self):
        page = "# f\n\n## Examples\n\nCaption A.\n\n```sql\nq\n```\n\n```plaintext\nr\n```\n"
        self.assertEqual(gfd.existing_captions(page), ["Caption A."])

    def test_translated_captions_survive_generation(self):
        doc = dict(DOC, examples=[
            ("Reverse a string.", "SELECT reverse('hello');", "+---+"),
            ("Reverse an array.", "SELECT reverse([4,1,5,8]);", "+---+"),
        ])
        out = gfd.generate_page("reverse", doc, "zh", self.ZH)
        self.assertIn("反转字符串。", out)
        self.assertIn("反转数组。", out)
        self.assertNotIn("Reverse a string.", out)

    def test_english_page_uses_payload_captions(self):
        page = "# f\n\n## Examples\n\nold\n"
        out = gfd.generate_page("f", DOC, "en", page)
        doc = dict(DOC, examples=[("Payload caption.", "SELECT 1;", "+---+")])
        out = gfd.generate_page("f", doc, "en", page)
        self.assertIn("Payload caption.", out)

    def test_no_english_payload_caption_reaches_localized_docs(self):
        english = set()
        for doc in functions.function_docs.values():
            for caption, _, _ in doc.get("examples", ()):
                if caption:
                    english.add(caption)
        leaked = []
        for name, doc in functions.function_docs.items():
            for lang in ("zh", "ja"):
                path = gfd.page_path(name, doc, lang)
                if not os.path.exists(path):
                    continue
                with open(path) as handle:
                    body = handle.read()
                for caption in english:
                    if caption in body:
                        leaked.append("%s/%s: %s" % (lang, name, caption))
        self.assertEqual(leaked, [])


class Rendering(unittest.TestCase):
    def test_query_uses_sql_fence_so_the_rot_checker_sees_it(self):
        out = gfd.render_examples(DOC, "en")
        self.assertIn("```sql\nSELECT bitand(3, 0);\n```", out)

    def test_result_uses_plaintext_fence(self):
        out = gfd.render_examples(DOC, "en")
        self.assertIn("```plaintext", out)

    def test_parameters_note_is_kept(self):
        doc = dict(DOC, parameters_note="> `x` and `y` must agree in data type.")
        out = gfd.render_parameters(doc, "en")
        self.assertIn("must agree in data type", out)

    def test_angle_bracket_types_are_backticked(self):
        # Bare ARRAY<VARCHAR> is parsed as JSX by MDX and breaks the build.
        self.assertEqual(gfd._format_types("ARRAY<VARCHAR>"), "`ARRAY<VARCHAR>`")

    def test_type_lists_are_backticked_per_type(self):
        self.assertEqual(gfd._format_types("INT, BIGINT"), "`INT`, `BIGINT`")

    def test_type_spec_with_existing_markup_is_left_alone(self):
        self.assertEqual(gfd._format_types("Same as `x`"), "Same as `x`")

    def test_no_bare_angle_brackets_reach_the_page(self):
        for name, doc in functions.function_docs.items():
            for lang in gfd.LANGUAGES:
                blocks = [
                    gfd.render_section(section, doc, lang)
                    for section in gfd.GENERATED_SECTIONS[lang]
                ]
                for block in blocks:
                    if block is None:
                        continue
                    for match in re.finditer(r"<[A-Za-z]", block):
                        line = block[: match.start()].split("\n")[-1]
                        self.assertIn(
                            "`", line, "%s/%s: bare angle bracket in %r" % (name, lang, line)
                        )

    def test_sections_without_data_are_not_emitted(self):
        doc = {"category": "c", "description": "d", "syntax": "f()"}
        self.assertIsNone(gfd.render_returns(doc, "en"))
        self.assertIsNone(gfd.render_usage(doc, "en"))
        self.assertIsNone(gfd.render_examples(doc, "en"))


class Validation(unittest.TestCase):
    def setUp(self):
        self.names = gfd.declared_function_names()

    def test_real_payload_is_valid(self):
        self.assertEqual(gfd.validate_payload(functions.function_docs, self.names), [])

    def test_unknown_function_name_is_reported(self):
        problems = gfd.validate_payload({"not_a_function": DOC}, self.names)
        self.assertTrue(any("no signature named" in p for p in problems))

    def test_missing_required_field_is_reported(self):
        doc = dict(DOC)
        del doc["syntax"]
        problems = gfd.validate_payload({"bitand": doc}, self.names)
        self.assertTrue(any("missing required field 'syntax'" in p for p in problems))

    def test_overlong_description_is_reported(self):
        doc = dict(DOC, description="x" * 200)
        problems = gfd.validate_payload({"bitand": doc}, self.names)
        self.assertTrue(any("frontmatter limit is 160" in p for p in problems))

    def test_malformed_example_is_reported(self):
        doc = dict(DOC, examples=[("caption", "query")])
        problems = gfd.validate_payload({"bitand": doc}, self.names)
        self.assertTrue(any("(caption, query, result)" in p for p in problems))


class PayloadContent(unittest.TestCase):
    """Checks over the real payload, not the machinery."""

    def test_every_documented_function_has_a_page_in_every_language(self):
        missing = []
        for name, doc in functions.function_docs.items():
            for lang in gfd.LANGUAGES:
                path = gfd.page_path(name, doc, lang)
                if not os.path.exists(path):
                    missing.append(os.path.relpath(path, gfd.REPO_ROOT))
        self.assertEqual(missing, [])

    def test_result_tables_are_aligned(self):
        """A misaligned ASCII table renders as visibly broken output."""
        bad = []
        for name, doc in functions.function_docs.items():
            for index, (_, _, result) in enumerate(doc.get("examples", [])):
                lines = [line for line in result.split("\n") if line]
                if not lines or not lines[0].startswith("+"):
                    continue
                widths = set(len(line) for line in lines)
                if len(widths) != 1:
                    bad.append("%s example %d: row widths %s" % (name, index, sorted(widths)))
        self.assertEqual(bad, [])

    def test_examples_do_not_carry_a_client_prompt(self):
        """`MySQL > ` prefixes make a sample unrunnable for the rot checker."""
        bad = []
        for name, doc in functions.function_docs.items():
            for _, query, _ in doc.get("examples", []):
                if re.match(r"^\s*(mysql|MySQL|MYSQL)\s*[>\[]", query):
                    bad.append("%s: %s" % (name, query))
        self.assertEqual(bad, [])

    def test_syntax_matches_the_function_name(self):
        bad = []
        for name, doc in functions.function_docs.items():
            if not doc["syntax"].startswith(name + "("):
                bad.append("%s: syntax is %r" % (name, doc["syntax"]))
        self.assertEqual(bad, [])


if __name__ == "__main__":
    unittest.main()
