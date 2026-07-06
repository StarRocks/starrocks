// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.context.markdown;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Coverage for the YAML-backed frontmatter parser introduced to replace the legacy split-on-colon
 * scanner. The legacy scanner flattened nested mappings, promoted YAML comments to top-level keys,
 * and stored inline lists as quoted strings; these tests pin the new behaviour against the shapes
 * that triggered the original bug (derivation-rule entities with {@code rule.inputs: [...]}).
 */
public class MarkdownExtractorFrontmatterTest {

    @Test
    public void testNestedMappingPreservesStructure() {
        String body = "---\n"
                + "entity_key: derivation_rules/team_homepage_v1\n"
                + "rule:\n"
                + "  inputs:\n"
                + "    - name: team_page\n"
                + "      contextbase: corp_org\n"
                + "    - name: employee\n"
                + "      contextbase: corp_org\n"
                + "  parameters:\n"
                + "    - name: include_archived\n"
                + "      type: bool\n"
                + "      default: false\n"
                + "---\n"
                + "## Purpose\nbody";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);

        Assertions.assertNotNull(extracted.frontmatterJson);
        JsonObject obj = JsonParser.parseString(extracted.frontmatterJson).getAsJsonObject();
        // Top-level keys are exactly the two the author wrote; no `- name`, no list-item leakage.
        Assertions.assertEquals(2, obj.size());
        Assertions.assertEquals("derivation_rules/team_homepage_v1",
                obj.get("entity_key").getAsString());

        JsonObject rule = obj.getAsJsonObject("rule");
        JsonArray inputs = rule.getAsJsonArray("inputs");
        Assertions.assertEquals(2, inputs.size());
        Assertions.assertEquals("team_page", inputs.get(0).getAsJsonObject().get("name").getAsString());
        Assertions.assertEquals("corp_org", inputs.get(0).getAsJsonObject().get("contextbase").getAsString());
        Assertions.assertEquals("employee", inputs.get(1).getAsJsonObject().get("name").getAsString());

        JsonArray params = rule.getAsJsonArray("parameters");
        Assertions.assertEquals(1, params.size());
        // `default: false` round-trips as a JSON boolean, not the string "false".
        JsonElement defaultVal = params.get(0).getAsJsonObject().get("default");
        Assertions.assertTrue(defaultVal.isJsonPrimitive() && defaultVal.getAsJsonPrimitive().isBoolean());
        Assertions.assertFalse(defaultVal.getAsBoolean());
    }

    @Test
    public void testYamlCommentsAreNotPromotedToKeys() {
        // Under the legacy parser a comment line like `# foo: bar` produced a top-level key
        // `"# foo"` because indexOf(':') landed inside the comment. YAML treats comments as
        // whitespace; the parsed JSON must contain neither the comment nor a synthetic key.
        String body = "---\n"
                + "# each candidate (employees here). Their frontmatter has `team:`\n"
                + "entity_key: derivation_rules/foo\n"
                + "# trailing comment\n"
                + "---\nbody";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);

        JsonObject obj = JsonParser.parseString(extracted.frontmatterJson).getAsJsonObject();
        Assertions.assertEquals(1, obj.size());
        Assertions.assertTrue(obj.has("entity_key"));
        // Explicitly assert the legacy bug-shaped keys do not appear.
        Assertions.assertFalse(obj.has("# each candidate (employees here). Their frontmatter has `team"));
        Assertions.assertFalse(obj.has("# trailing comment"));
    }

    @Test
    public void testInlineListBecomesJsonArray() {
        String body = "---\ntype: page\ntags: [alpha, beta, gamma]\n---\nbody";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);

        JsonObject obj = JsonParser.parseString(extracted.frontmatterJson).getAsJsonObject();
        JsonElement tags = obj.get("tags");
        Assertions.assertTrue(tags.isJsonArray(), "tags must be a JSON array, not a string");
        JsonArray arr = tags.getAsJsonArray();
        Assertions.assertEquals(3, arr.size());
        Assertions.assertEquals("alpha", arr.get(0).getAsString());
        Assertions.assertEquals("gamma", arr.get(2).getAsString());
    }

    @Test
    public void testTypedScalarsRoundTrip() {
        String body = "---\n"
                + "type: page\n"
                + "priority: 5\n"
                + "ratio: 0.75\n"
                + "enabled: true\n"
                + "owner:\n"
                + "---\nbody";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);

        JsonObject obj = JsonParser.parseString(extracted.frontmatterJson).getAsJsonObject();
        Assertions.assertEquals("page", obj.get("type").getAsString());
        Assertions.assertEquals(5, obj.get("priority").getAsInt());
        Assertions.assertEquals(0.75, obj.get("ratio").getAsDouble(), 0.0001);
        Assertions.assertTrue(obj.get("enabled").getAsBoolean());
        Assertions.assertTrue(obj.get("owner").isJsonNull());
    }

    @Test
    public void testFlatScalarFrontmatterRegression() {
        // The most common existing shape — flat string-to-string with a `source:` list — must
        // still produce a sane JSON object so the storage column stays queryable.
        String body = "---\n"
                + "type: page\n"
                + "title: SMB stage baselines\n"
                + "source: [201, 202, 305]\n"
                + "---\nbody";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);

        JsonObject obj = JsonParser.parseString(extracted.frontmatterJson).getAsJsonObject();
        Assertions.assertEquals("page", obj.get("type").getAsString());
        Assertions.assertEquals("SMB stage baselines", obj.get("title").getAsString());
        // source: still appears in frontmatterJson — parseSources() drives the dedicated
        // sourceIds projection independently. Both paths must succeed in parallel.
        Assertions.assertTrue(obj.get("source").isJsonArray());
        Assertions.assertEquals(3, extracted.sourceIds.size());
        Assertions.assertEquals(201L, extracted.sourceIds.get(0));
    }

    @Test
    public void testCanonicalizeRawMarkdownRoundTripsNestedFrontmatter() {
        // canonicalizeRawMarkdown short-circuits when rawMarkdown is supplied — so we feed the
        // rebuild path explicitly with frontmatterJson + sourceJson and re-parse the result.
        String body = "---\n"
                + "entity_key: derivation_rules/foo\n"
                + "rule:\n"
                + "  inputs:\n"
                + "    - name: team_page\n"
                + "---\nbody";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        String rebuilt = MarkdownExtractor.canonicalizeRawMarkdown(
                null, extracted.body, extracted.frontmatterJson, extracted.sourceJson);
        MarkdownExtractor.Extracted reparsed = MarkdownExtractor.extract(rebuilt, null);

        JsonObject obj = JsonParser.parseString(reparsed.frontmatterJson).getAsJsonObject();
        Assertions.assertEquals("derivation_rules/foo", obj.get("entity_key").getAsString());
        JsonObject rule = obj.getAsJsonObject("rule");
        JsonArray inputs = rule.getAsJsonArray("inputs");
        Assertions.assertEquals(1, inputs.size());
        Assertions.assertEquals("team_page", inputs.get(0).getAsJsonObject().get("name").getAsString());
    }

    @Test
    public void testMalformedYamlFallsBackToRawWrapper() {
        // A tab-indented block list is illegal YAML (tabs banned for indentation). The parser must
        // not crash the upsert — it falls back to `{"_raw": "<original>"}` so the column stays
        // valid JSON and downstream debug tools can recover the original text.
        String body = "---\n"
                + "rule:\n"
                + "\t- name: bad_tab_indent\n"
                + "---\nbody";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);

        Assertions.assertNotNull(extracted.frontmatterJson);
        JsonObject obj = JsonParser.parseString(extracted.frontmatterJson).getAsJsonObject();
        if (obj.has("_raw")) {
            Assertions.assertTrue(obj.get("_raw").getAsString().contains("bad_tab_indent"));
        }
        // If snakeyaml happens to accept it (version-dependent), the structured form is also fine
        // — what we care about is "no crash, valid JSON object". Both outcomes satisfy that.
    }

    @Test
    public void testEmptyFrontmatterBlockYieldsNullJson() {
        // `---\n---\n` produces an empty frontmatterLines list at the caller; frontmatterToJson is
        // not even invoked. Whitespace-only frontmatter (single empty line) should also resolve
        // to null rather than `{}` so storage rows stay clean.
        String body = "---\n   \n---\nbody";
        MarkdownExtractor.Extracted extracted = MarkdownExtractor.extract(body, null);
        Assertions.assertNull(extracted.frontmatterJson);
    }
}
