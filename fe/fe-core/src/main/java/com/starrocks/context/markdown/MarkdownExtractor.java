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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the semantic-context markdown body into the three derived artifacts that the write path
 * needs to materialize: the preview, the section fragments, and the {@code [[e:id]]} reference list.
 *
 * <p>The semantic-context design ({@code 1-agentbase-starrocks-semantic-context-architecture-design.md})
 * specifies:
 * <ul>
 *   <li><b>preview</b> — a short (~512 char) summary; first paragraph if the caller did not provide one.</li>
 *   <li><b>section fragments</b> — non-overlapping slices delimited by ATX headers ({@code ##}, {@code ###}),
 *       each tagged with its 1-based line range.</li>
 *   <li><b>inline refs</b> — {@code [[e:<id>]]} or {@code [[e:<entity_key>]]} occurrences; each
 *       one is one entry in the output list. Numeric IDs flow through unchanged; entity_key
 *       tokens are carried forward as strings and resolved against
 *       {@code context_entity_heads(contextbase_id, entity_key)} by the write path. The leading
 *       character of a key must be a letter or {@code _} so a digit-only entity_key — banned at
 *       CREATE — cannot collide with a numeric id.</li>
 * </ul>
 *
 * <p>Frontmatter is parsed as YAML via Jackson's {@link YAMLFactory}, so nested mappings,
 * list-of-maps, and inline lists round-trip through {@code context_entity_versions.frontmatter_json}
 * with their full structure intact. The {@code source:} / {@code refs:} / {@code links:} ref
 * extraction still goes through a dedicated regex scanner ({@link #parseSources}) that feeds
 * {@code context_entity_versions.source_json} — that scanner's lenient token-classification
 * behaviour is covered by {@code MarkdownExtractorKeyRefsTest} and is intentionally independent
 * of the general YAML parse.
 */
public final class MarkdownExtractor {

    // Numeric branch must stay first so an all-digit token still goes through the fast
    // Long.parseLong path. The key branch requires a leading letter or underscore — that single
    // constraint is what removes the [[e:12552]] ambiguity (a digit-only entity_key is rejected
    // at CREATE/UPSERT time via INVALID_ENTITY_KEY).
    private static final String NUM_GROUP = "(\\d+)";
    private static final String KEY_GROUP = "([A-Za-z_][A-Za-z0-9_./:-]{0,255})";

    private static final Pattern INLINE_REF =
            Pattern.compile("\\[\\[e:(?:" + NUM_GROUP + "|" + KEY_GROUP + ")\\]\\]");
    private static final Pattern SECTION_HEADER = Pattern.compile("^(#{2,6})\\s+(.+)$");
    private static final Pattern FRONTMATTER_FENCE = Pattern.compile("^---\\s*$");
    // Keys whose YAML value (scalar / inline list / block list) carries a list of related-entity
    // refs (numeric ids or entity_key strings). The first matching key per document wins (kept
    // compatible with the legacy `source:` contract). Recognising the AgentBase-style
    // `source_pages:` and the architecture doc's documented `refs:` / `links:` / `references:`
    // aliases lets us extract graph edges from existing corpora without changing the upstream
    // ingest format.
    private static final List<String> REF_KEYS = ImmutableList.of(
            "source", "source_pages", "refs", "links", "references");
    private static final Pattern KEY_SCALAR_LINE = Pattern.compile(
            "^(source|source_pages|refs|links|references):\\s*(?:" + NUM_GROUP + "|" + KEY_GROUP + ")$");
    private static final Pattern KEY_INLINE_LIST_LINE = Pattern.compile(
            "^(source|source_pages|refs|links|references):\\s*\\[([^]]*)]$");
    // Block-list opener: `key:` (nothing else on the line after optional whitespace) starts a
    // YAML block list; subsequent indented `- <number>` or `- <entity_key>` lines belong to it
    // until the indent returns to column 0 or another top-level key appears.
    private static final Pattern KEY_BLOCK_OPENER = Pattern.compile(
            "^(source|source_pages|refs|links|references):\\s*$");
    private static final Pattern BLOCK_LIST_ITEM =
            Pattern.compile("^\\s+-\\s*(?:" + NUM_GROUP + "|" + KEY_GROUP + ")\\s*$");
    private static final Pattern TOP_LEVEL_KEY = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*:.*$");
    // Validation pattern for the inline-list token splitter — `key: [a, "b", 17]`. A token is
    // numeric (all digits) or a key (leading letter/_ followed by the key character class).
    // Single/double quotes are stripped before classification so YAML's quoted-string form is
    // accepted.
    private static final Pattern INLINE_LIST_NUM_TOKEN = Pattern.compile("\\d+");
    private static final Pattern INLINE_LIST_KEY_TOKEN =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_./:-]{0,255}");
    private static final int PREVIEW_MAX_CHARS = 512;
    // Section fragments are capped so each gets a focused, undiluted embedding. A body with no
    // ATX headers (or with an over-long header section) would otherwise collapse into a single
    // whole-body section whose one vector averages many topics — useless for passage-level vector
    // search. We window such ranges into ~512-token slices (~4 chars/token) with ~15% overlap so
    // an answer buried mid-document still gets its own fragment to match against.
    private static final int SECTION_MAX_CHARS = 2000;
    private static final int SECTION_OVERLAP_CHARS = 300;

    // Jackson ObjectMappers are documented thread-safe once configured; allocating once per class
    // avoids per-upsert reflection / module-loading cost. YAMLFactory uses snakeyaml's
    // SafeConstructor by default, so untrusted YAML cannot instantiate arbitrary classes.
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    /**
     * Inline reference to another entity. {@code ord} is the occurrence index within this document,
     * used as the third column of {@code context_entity_refs} primary key.
     *
     * <p>Exactly one of {@code dstEntityId} / {@code dstEntityKey} is populated by the extractor:
     * numeric {@code [[e:231]]} sets {@code dstEntityId} and leaves {@code dstEntityKey == null};
     * key-shaped {@code [[e:smb_baseline]]} sets {@code dstEntityKey} and leaves
     * {@code dstEntityId == null}. The write path resolves the key against
     * {@code context_entity_heads} and substitutes the numeric id before persisting; refs whose
     * key cannot be resolved cause the UPSERT to fail with {@code ENTITY_NOT_FOUND}.
     */
    public static final class InlineRef {
        public final Long dstEntityId;
        public final String dstEntityKey;
        public final int ord;

        public InlineRef(Long dstEntityId, String dstEntityKey, int ord) {
            this.dstEntityId = dstEntityId;
            this.dstEntityKey = dstEntityKey;
            this.ord = ord;
        }

        /** Back-compat factory for numeric-only callers. */
        public static InlineRef ofId(long id, int ord) {
            return new InlineRef(id, null, ord);
        }

        /** Factory for key-bearing refs. */
        public static InlineRef ofKey(String key, int ord) {
            return new InlineRef(null, key, ord);
        }
    }

    /**
     * One token from a frontmatter ref list ({@code source:} / {@code refs:} / etc.). Exactly one
     * of {@code id} / {@code key} is non-null — same dual-shape contract as {@link InlineRef}.
     */
    public static final class RefToken {
        public final Long id;
        public final String key;

        private RefToken(Long id, String key) {
            this.id = id;
            this.key = key;
        }

        public static RefToken ofId(long id) {
            return new RefToken(id, null);
        }

        public static RefToken ofKey(String key) {
            return new RefToken(null, key);
        }
    }

    /**
     * One section fragment. {@code lineStart}/{@code lineEnd} are 1-based and inclusive.
     */
    public static final class Section {
        public final int ordinal;
        public final int lineStart;
        public final int lineEnd;
        public final String text;
        public final String preview;

        public Section(int ordinal, int lineStart, int lineEnd, String text, String preview) {
            this.ordinal = ordinal;
            this.lineStart = lineStart;
            this.lineEnd = lineEnd;
            this.text = text;
            this.preview = preview;
        }
    }

    /**
     * Parsed artifacts for one markdown body.
     *
     * <p>{@code sourceIds} is a back-compat mirror that holds only the numeric-id entries of the
     * frontmatter ref list, in original order. {@code sourceRefs} carries the full token list
     * (numeric and key, both shapes) and is the source of truth for the write path's resolution
     * step. Readers that only need numeric ids (e.g. {@code renderFrontmatter},
     * {@code sourceJson}) keep using {@code sourceIds} unchanged.
     */
    public static final class Extracted {
        public final String preview;
        public final String body;
        public final List<Section> sections;
        public final List<InlineRef> inlineRefs;
        public final List<Long> sourceIds;
        public final List<RefToken> sourceRefs;
        public final String frontmatterJson;
        public final String sourceJson;

        public Extracted(String preview, String body, List<Section> sections, List<InlineRef> inlineRefs,
                         List<Long> sourceIds, List<RefToken> sourceRefs,
                         String frontmatterJson, String sourceJson) {
            this.preview = preview;
            this.body = body;
            this.sections = sections;
            this.inlineRefs = inlineRefs;
            this.sourceIds = sourceIds;
            this.sourceRefs = sourceRefs;
            this.frontmatterJson = frontmatterJson;
            this.sourceJson = sourceJson;
        }
    }

    private MarkdownExtractor() {
    }

    /**
     * Extract artifacts from the raw body. {@code providedPreview} overrides the auto-derived preview
     * when non-null/non-empty so the caller's explicit summary always wins.
     */
    public static Extracted extract(String body, String providedPreview) {
        if (body == null) {
            body = "";
        }

        String[] lines = body.split("\n", -1);
        int frontmatterEnd = -1;
        List<String> frontmatterLines = new ArrayList<>();
        if (lines.length >= 1 && FRONTMATTER_FENCE.matcher(lines[0].trim()).matches()) {
            for (int i = 1; i < lines.length; i++) {
                if (FRONTMATTER_FENCE.matcher(lines[i].trim()).matches()) {
                    frontmatterEnd = i;
                    break;
                }
                frontmatterLines.add(lines[i]);
            }
        }

        // Slice the post-frontmatter line view once and pass it through to splitSections.
        // Previously splitSections re-ran body.split("\n", -1) on the rebuilt contentBody,
        // doubling the per-upsert tokenization cost over large documents.
        String[] contentLines;
        String contentBody;
        if (frontmatterEnd > 0) {
            int start = frontmatterEnd + 1;
            int contentLen = lines.length - start;
            contentLines = new String[contentLen];
            System.arraycopy(lines, start, contentLines, 0, contentLen);
            StringBuilder rest = new StringBuilder();
            for (int i = 0; i < contentLen; i++) {
                rest.append(contentLines[i]);
                if (i < contentLen - 1) {
                    rest.append('\n');
                }
            }
            contentBody = rest.toString();
        } else {
            contentLines = lines;
            contentBody = body;
        }

        List<RefToken> sourceRefs = parseSources(frontmatterLines);
        // sourceIds is the numeric-only projection of sourceRefs, kept in original order so the
        // legacy frontmatter renderer / source_json column stay byte-for-byte unchanged for any
        // body whose frontmatter only carried numeric ids.
        List<Long> sources = projectNumericIds(sourceRefs);
        List<InlineRef> refs = extractRefs(body);
        List<Section> sections = splitSections(contentLines);
        String preview = providedPreview != null && !providedPreview.isEmpty()
                ? providedPreview
                : firstParagraphPreview(contentBody);
        String frontmatterJson = frontmatterLines.isEmpty() ? null : frontmatterToJson(frontmatterLines);
        String sourceJson = sources == null || sources.isEmpty() ? null : sourcesToJson(sources);
        return new Extracted(preview, contentBody, sections, refs, sources, sourceRefs, frontmatterJson, sourceJson);
    }

    private static List<Long> projectNumericIds(List<RefToken> tokens) {
        List<Long> ids = new ArrayList<>();
        for (RefToken t : tokens) {
            if (t.id != null) {
                ids.add(t.id);
            }
        }
        return ImmutableList.copyOf(ids);
    }

    /**
     * Returns the stored raw markdown if present, otherwise rebuilds a canonical markdown document
     * from the parsed body/frontmatter columns.
     */
    public static String canonicalizeRawMarkdown(String rawMarkdown, String body,
                                                 String frontmatterJson, String sourceJson) {
        if (rawMarkdown != null && !rawMarkdown.isEmpty()) {
            return rawMarkdown;
        }
        String renderedFrontmatter = renderFrontmatter(frontmatterJson, sourceJson);
        if (renderedFrontmatter.isEmpty()) {
            return body == null ? "" : body;
        }
        StringBuilder out = new StringBuilder();
        out.append("---\n");
        out.append(renderedFrontmatter);
        out.append("---\n");
        if (body != null && !body.isEmpty()) {
            out.append('\n').append(body);
        }
        return out.toString();
    }

    private static String renderFrontmatter(String frontmatterJson, String sourceJson) {
        if ((frontmatterJson == null || frontmatterJson.isEmpty())
                && (sourceJson == null || sourceJson.isEmpty())) {
            return "";
        }
        StringBuilder out = new StringBuilder();
        if (frontmatterJson != null && !frontmatterJson.isEmpty()) {
            JsonElement parsed = JsonParser.parseString(frontmatterJson);
            if (parsed.isJsonObject()) {
                JsonObject obj = parsed.getAsJsonObject();
                for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
                    if ("source".equals(entry.getKey())) {
                        continue;
                    }
                    JsonElement value = entry.getValue();
                    out.append(entry.getKey()).append(": ");
                    if (value == null || value.isJsonNull()) {
                        out.append("null");
                    } else if (value.isJsonPrimitive()) {
                        out.append(value.getAsString());
                    } else {
                        out.append(value.toString());
                    }
                    out.append('\n');
                }
            }
        }
        List<Long> sourceIds = parseSourceJson(sourceJson);
        if (!sourceIds.isEmpty()) {
            if (sourceIds.size() == 1) {
                out.append("source: ").append(sourceIds.get(0)).append('\n');
            } else {
                out.append("source: [");
                for (int i = 0; i < sourceIds.size(); i++) {
                    if (i > 0) {
                        out.append(", ");
                    }
                    out.append(sourceIds.get(i));
                }
                out.append("]\n");
            }
        }
        return out.toString();
    }

    private static String frontmatterToJson(List<String> frontmatterLines) {
        // Reassemble the frontmatter block exactly as the author wrote it and hand the whole
        // thing to a real YAML parser. The previous implementation here was a hand-rolled
        // split-on-first-colon scanner that treated every line as a `key: value` pair regardless
        // of indent, comment markers, or list-item dashes — so nested mappings, list-of-maps,
        // and inline lists were silently flattened into a string-to-string map. Jackson's
        // YAMLFactory understands the full YAML subset relevant for frontmatter (nested
        // mappings, block + inline sequences, typed scalars, quoted strings, comments).
        String yamlText = String.join("\n", frontmatterLines);
        if (yamlText.trim().isEmpty()) {
            return null;
        }
        try {
            JsonNode node = YAML_MAPPER.readTree(yamlText);
            if (node == null || node.isMissingNode() || node.isNull()) {
                return null;
            }
            // frontmatter_json downstream consumers assume a JSON object/array root. A YAML
            // document whose top level is a bare scalar (rare for frontmatter but possible) is
            // wrapped under "value" so the column always parses as an object.
            if (!node.isObject() && !node.isArray()) {
                return JSON_MAPPER.writeValueAsString(
                        JSON_MAPPER.createObjectNode().set("value", node));
            }
            return JSON_MAPPER.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            // Never fail ingest on malformed frontmatter. Wrap the raw text in `_raw` so the
            // column stays valid JSON and nothing is lost. Matches the lenient policy in
            // parseSources(), which drops unparseable tokens rather than aborting the upsert.
            try {
                return JSON_MAPPER.writeValueAsString(
                        JSON_MAPPER.createObjectNode().put("_raw", yamlText));
            } catch (JsonProcessingException unreachable) {
                return null;
            }
        }
    }

    private static String sourcesToJson(List<Long> sources) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < sources.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(sources.get(i));
        }
        sb.append(']');
        return sb.toString();
    }

    private static List<Long> parseSourceJson(String sourceJson) {
        List<Long> ids = new ArrayList<>();
        if (sourceJson == null || sourceJson.isEmpty()) {
            return ids;
        }
        JsonElement parsed = JsonParser.parseString(sourceJson);
        if (parsed.isJsonArray()) {
            JsonArray arr = parsed.getAsJsonArray();
            for (JsonElement element : arr) {
                if (!element.isJsonNull()) {
                    ids.add(element.getAsLong());
                }
            }
        }
        return ids;
    }

    private static List<InlineRef> extractRefs(String body) {
        List<InlineRef> refs = new ArrayList<>();
        Matcher m = INLINE_REF.matcher(body);
        int ord = 0;
        while (m.find()) {
            // INLINE_REF has two named-by-position groups: group(1) is the numeric run, group(2)
            // is the entity_key run. Exactly one is non-null per match (the regex uses
            // alternation). For numeric tokens, an unbounded digit run can overflow Long (e.g.
            // `[[e:99999999999999999999]]`), so the parseLong is wrapped — user content must
            // never crash ingest. Key tokens are length-capped by the regex itself ({0,255}
            // characters after the leading letter/underscore), well within VARCHAR(512) on heads.
            String numericGroup = m.group(1);
            String keyGroup = m.group(2);
            if (numericGroup != null) {
                try {
                    long id = Long.parseLong(numericGroup);
                    refs.add(InlineRef.ofId(id, ord++));
                } catch (NumberFormatException ignored) {
                    // skip malformed/out-of-range ref; preserves ord so well-formed refs after
                    // this one keep their relative ordering vs the rest of the body.
                }
            } else if (keyGroup != null && !keyGroup.isEmpty()) {
                refs.add(InlineRef.ofKey(keyGroup, ord++));
            }
        }
        return ImmutableList.copyOf(refs);
    }

    private static List<Section> splitSections(String[] lines) {
        // Pass 1: cut the body into ranges delimited by ATX headers (## / ###).
        List<int[]> ranges = new ArrayList<>();
        int sectionStart = 0;
        for (int i = 0; i < lines.length; i++) {
            Matcher m = SECTION_HEADER.matcher(lines[i]);
            if (m.matches() && i > sectionStart) {
                ranges.add(new int[] {sectionStart, i - 1});
                sectionStart = i;
            }
        }
        if (sectionStart < lines.length) {
            ranges.add(new int[] {sectionStart, lines.length - 1});
        }

        // Pass 2: window any over-long range (incl. the whole-body range of a header-less doc)
        // into bounded, overlapping slices so passage-level vector search has real chunks.
        // Ordinals are assigned after windowing so they stay globally sequential.
        List<Section> out = new ArrayList<>();
        int ordinal = 0;
        for (int[] range : ranges) {
            for (int[] window : windowRange(lines, range[0], range[1])) {
                out.add(buildSection(ordinal++, window[0], window[1], lines));
            }
        }
        return ImmutableList.copyOf(out);
    }

    /**
     * Split an inclusive line range [{@code start}, {@code end}] into windows each at most
     * {@link #SECTION_MAX_CHARS} characters, consecutive windows overlapping by roughly
     * {@link #SECTION_OVERLAP_CHARS}. Windowing is line-granular so {@link Section} line numbers
     * stay meaningful. A single line longer than the cap becomes its own window (no infinite loop).
     */
    private static List<int[]> windowRange(String[] lines, int start, int end) {
        List<int[]> windows = new ArrayList<>();
        if (rangeChars(lines, start, end) <= SECTION_MAX_CHARS) {
            windows.add(new int[] {start, end});
            return windows;
        }
        int windowStart = start;
        while (windowStart <= end) {
            int windowEnd = windowStart;
            int acc = lineChars(lines[windowStart]);
            while (windowEnd < end
                    && acc + 1 + lineChars(lines[windowEnd + 1]) <= SECTION_MAX_CHARS) {
                windowEnd++;
                acc += 1 + lineChars(lines[windowEnd]);
            }
            windows.add(new int[] {windowStart, windowEnd});
            if (windowEnd >= end) {
                break;
            }
            // Start the next window a few lines back to overlap ~SECTION_OVERLAP_CHARS, but never
            // back past the current window start — guarantees forward progress (termination).
            int nextStart = windowEnd + 1;
            int back = 0;
            while (nextStart - 1 > windowStart
                    && back + lineChars(lines[nextStart - 1]) <= SECTION_OVERLAP_CHARS) {
                back += lineChars(lines[nextStart - 1]) + 1;
                nextStart--;
            }
            windowStart = Math.max(nextStart, windowStart + 1);
        }
        return windows;
    }

    private static int lineChars(String line) {
        return line == null ? 0 : line.length();
    }

    private static int rangeChars(String[] lines, int start, int end) {
        int total = 0;
        for (int i = start; i <= end; i++) {
            total += lineChars(lines[i]) + (i < end ? 1 : 0);
        }
        return total;
    }

    private static Section buildSection(int ordinal, int startLine, int endLine, String[] lines) {
        StringBuilder text = new StringBuilder();
        for (int i = startLine; i <= endLine; i++) {
            text.append(lines[i]);
            if (i < endLine) {
                text.append('\n');
            }
        }
        String combined = text.toString();
        String preview = firstParagraphPreview(combined);
        return new Section(ordinal, startLine + 1, endLine + 1, combined, preview);
    }

    private static String firstParagraphPreview(String body) {
        if (body == null || body.isEmpty()) {
            return "";
        }
        int firstBlankLine = body.indexOf("\n\n");
        String candidate = firstBlankLine < 0 ? body : body.substring(0, firstBlankLine);
        candidate = candidate.trim();
        return candidate.length() <= PREVIEW_MAX_CHARS
                ? candidate
                : candidate.substring(0, PREVIEW_MAX_CHARS);
    }

    private static List<RefToken> parseSources(List<String> frontmatterLines) {
        // Walk frontmatter lines once. Recognise three YAML shapes for any of the REF_KEYS:
        //   key: 123                   (scalar single id)
        //   key: smb_baseline          (scalar single key)
        //   key: [123, foo, "bar"]     (inline list, mixed)
        //   key:                       (block list opener)
        //     - 123
        //     - smb_baseline
        // Block-list parsing tracks state across lines: the opener pattern engages a collector
        // that consumes subsequent BLOCK_LIST_ITEM lines until a TOP_LEVEL_KEY (no indent) or
        // a non-item line breaks the run. The first ref-key with a non-empty result wins so the
        // legacy `source:` contract continues to take precedence when both are present.
        List<RefToken> collected = new ArrayList<>();
        boolean inBlockList = false;
        for (String raw : frontmatterLines) {
            // Block-list continuation line.
            if (inBlockList) {
                Matcher item = BLOCK_LIST_ITEM.matcher(raw);
                if (item.matches()) {
                    RefToken token = classifyToken(item.group(1), item.group(2));
                    if (token != null) {
                        collected.add(token);
                    }
                    continue;
                }
                // A blank line keeps us inside the same block list; any new top-level key (or
                // arbitrary text starting at column 0) closes it.
                if (raw.trim().isEmpty()) {
                    continue;
                }
                inBlockList = false;
                if (!collected.isEmpty()) {
                    return ImmutableList.copyOf(collected);
                }
                // fall through to also try the current line as a fresh ref key
            }

            String trimmedLine = raw.trim();
            Matcher scalar = KEY_SCALAR_LINE.matcher(trimmedLine);
            if (scalar.matches()) {
                // Groups: 1=keyword, 2=numeric, 3=key
                RefToken token = classifyToken(scalar.group(2), scalar.group(3));
                return token == null ? ImmutableList.of() : ImmutableList.of(token);
            }
            Matcher list = KEY_INLINE_LIST_LINE.matcher(trimmedLine);
            if (list.matches()) {
                List<RefToken> tokens = new ArrayList<>();
                for (String raw0 : list.group(2).split(",")) {
                    String token = stripQuotes(raw0.trim());
                    if (token.isEmpty()) {
                        continue;
                    }
                    if (INLINE_LIST_NUM_TOKEN.matcher(token).matches()) {
                        try {
                            tokens.add(RefToken.ofId(Long.parseLong(token)));
                        } catch (NumberFormatException ignored) {
                            // tolerate overflow; downstream consumers ignore malformed entries.
                        }
                    } else if (INLINE_LIST_KEY_TOKEN.matcher(token).matches()) {
                        tokens.add(RefToken.ofKey(token));
                    }
                    // Anything else (e.g. `12abc`, embedded spaces) is dropped — same lenient
                    // policy that protected the legacy `source: [...]` parser from crashing on
                    // malformed corpora.
                }
                return ImmutableList.copyOf(tokens);
            }
            // Block-list opener: `key:` with nothing after the colon. Match against the raw line
            // (not the trimmed one) because the opener must sit at column 0 — indented `key:`
            // is some other YAML structure, not a top-level ref key.
            Matcher opener = KEY_BLOCK_OPENER.matcher(raw);
            if (opener.matches()) {
                inBlockList = true;
                collected.clear();
            }
        }
        // EOF while still inside a block list — flush whatever we collected.
        if (!collected.isEmpty()) {
            return ImmutableList.copyOf(collected);
        }
        return ImmutableList.of();
    }

    private static RefToken classifyToken(String numericGroup, String keyGroup) {
        if (numericGroup != null) {
            try {
                return RefToken.ofId(Long.parseLong(numericGroup));
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        if (keyGroup != null && !keyGroup.isEmpty()) {
            return RefToken.ofKey(keyGroup);
        }
        return null;
    }

    private static String stripQuotes(String s) {
        if (s.length() >= 2) {
            char first = s.charAt(0);
            char last = s.charAt(s.length() - 1);
            if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
                return s.substring(1, s.length() - 1);
            }
        }
        return s;
    }
}
