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

package com.starrocks.metric;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Contract guard for FE (fe-core) metric {@code "name type unit"} entries.
 *
 * <p>FE metrics are an EXTERNAL contract: dashboards, alerting rules, and external monitoring
 * tooling scrape them from the Prometheus endpoint (prefixed with {@code starrocks_fe_}).
 * Adding, removing, or renaming a metric — or flipping its type (counter/gauge) or its unit —
 * silently breaks those consumers: the type drives the Prometheus {@code # TYPE} line
 * (PrometheusMetricVisitor) and the unit is exported on the JSON metrics endpoint
 * (JsonMetricVisitor). This test scans the fe-core main sources and records, for each metric,
 * a {@code "name type unit"} triple, then compares the result against a committed golden file
 * ({@code fe-core/src/test/resources/metric/fe_metrics_golden.txt}). Any drift fails the
 * build with an explicit added/removed report and a directive to notify downstream consumers.
 *
 * <p>The test is a pure source scan — it does NOT bootstrap the FE, so it runs fast and
 * deterministically without any thrift/metadata wiring. The extraction rules below MUST
 * stay identical to the spec documented in the golden file header.
 *
 * <p>To accept an intentional change, regenerate the golden file:
 * <pre>
 *   cd fe &amp;&amp; mvn -pl fe-core test \
 *     -Dtest=FeMetricsContractGuardTest -Dmetrics.guard.regenerate=true
 * </pre>
 * then re-run without the flag to confirm it passes.
 */
public class FeMetricsContractGuardTest {

    /** Source root to scan, rooted at {@code fe/}. */
    private static final String SRC_ROOT_RELATIVE = "fe-core/src/main/java/com/starrocks";

    /** Committed golden inventory, rooted at {@code fe/}. */
    private static final String GOLDEN_RELATIVE = "fe-core/src/test/resources/metric/fe_metrics_golden.txt";

    /** Set {@code -Dmetrics.guard.regenerate=true} to rewrite the golden file's data section. */
    private static final String REGENERATE_PROPERTY = "metrics.guard.regenerate";

    /**
     * Metric ctor class -&gt; exported type label. The first ctor argument of each of these
     * classes is the metric name, and the class itself fully determines the exported type:
     * {@code counter}/{@code gauge} (PrometheusMetricVisitor emits
     * {@code metric.getType().name().toLowerCase()}), while histogram families are exported as
     * {@code summary}. Kept in sync with the offline generator and the golden file header.
     */
    private static final Map<String, String> CLASS_TYPE = new LinkedHashMap<>();

    static {
        CLASS_TYPE.put("LongCounterMetric", "counter");
        CLASS_TYPE.put("DoubleCounterMetric", "counter");
        CLASS_TYPE.put("LeaderAwareCounterMetricExternalLong", "counter");
        CLASS_TYPE.put("LeaderAwareCounterMetricLong", "counter");
        CLASS_TYPE.put("GaugeMetricImpl", "gauge");
        CLASS_TYPE.put("GaugeMetric", "gauge");
        CLASS_TYPE.put("LeaderAwareGaugeMetricLong", "gauge");
        CLASS_TYPE.put("LeaderAwareGaugeMetricInteger", "gauge");
        CLASS_TYPE.put("HistogramMetric", "summary");
        CLASS_TYPE.put("LeaderAwareHistogramMetric", "summary");
    }

    /** Exported type label for the codahale and {@code HistogramMetric} families. */
    private static final String SUMMARY = "summary";

    /**
     * Placeholder unit for entries whose ctor declares no {@code MetricUnit} (the histogram
     * families) or whose name is a runtime-concatenated fragment (e.g. {@code brpc_pool_}), so
     * the unit cannot be bound statically.
     */
    private static final String NO_UNIT = "-";

    // A double-quoted string literal, capturing its inner content in group 1.
    private static final String STRING = "\"((?:[^\"\\\\]|\\\\.)*)\"";

    // `[static] final String IDENT = "literal";` -> group(1)=IDENT, group(2)=literal.
    private static final Pattern CONST_RE = Pattern.compile(
            "(?:static\\s+)?final\\s+String\\s+([A-Za-z_]\\w*)\\s*=\\s*" + STRING + "\\s*;");

    // `new <AllowlistedClass>[<...>]( <firstArg> [, MetricUnit.X] ...` ->
    // group(1)=class, group(2)=name literal, group(3)=name identifier, group(4)=unit enum.
    private static final Pattern CTOR_RE = Pattern.compile(
            "new\\s+(" + buildClassAlternation() + ")\\s*(?:<[^>]*>)?\\s*\\(\\s*(?:"
                    + STRING + "|([A-Za-z_][\\w.]*))"
                    + "(?:\\s*,\\s*(?:Metric\\.)?MetricUnit\\.([A-Za-z_]+))?");

    // Codahale histogram name builder: `MetricRegistry.name(<args>)`.
    private static final Pattern NAME_CALL_RE = Pattern.compile("MetricRegistry\\.name\\s*\\(([^)]*)\\)");

    // A single argument that is exactly one string literal (group 1 = its content).
    private static final Pattern ARG_STRING_RE = Pattern.compile("^\\s*" + STRING + "\\s*$");

    // Identifier first-args are only resolved through the global constant map when they look
    // like a constant (UPPER_SNAKE_CASE); camelCase params are treated as dynamic and skipped.
    private static final Pattern UPPER_SNAKE_RE = Pattern.compile("^[A-Z][A-Z0-9_]*$");

    @Test
    public void feMetricNamesMatchGoldenContract() {
        Path srcRoot = resolveUnderFe(SRC_ROOT_RELATIVE);
        Path goldenFile = resolveUnderFe(GOLDEN_RELATIVE);

        SortedSet<String> actual = scanMetricTriples(srcRoot);

        if (Boolean.getBoolean(REGENERATE_PROPERTY)) {
            writeGolden(goldenFile, actual);
            System.out.println("[metrics-guard] Regenerated " + goldenFile + " with " + actual.size()
                    + " metric (name, type, unit) entries. Re-run without -D" + REGENERATE_PROPERTY + " to verify.");
            return;
        }

        SortedSet<String> golden = readGolden(goldenFile);
        if (actual.equals(golden)) {
            return;
        }

        SortedSet<String> added = new TreeSet<>(actual);
        added.removeAll(golden);
        SortedSet<String> removed = new TreeSet<>(golden);
        removed.removeAll(actual);

        throw new AssertionError(buildFailureMessage(added, removed, goldenFile));
    }

    /**
     * Scan every {@code *.java} under {@code srcRoot} whose raw text contains "Metric" and
     * return the sorted, de-duplicated set of {@code "name type unit"} entries. Mirrors the
     * offline generator exactly.
     */
    private static SortedSet<String> scanMetricTriples(Path srcRoot) {
        List<Path> javaFiles;
        try (Stream<Path> stream = Files.walk(srcRoot)) {
            javaFiles = stream.filter(p -> p.toString().endsWith(".java")).sorted().collect(Collectors.toList());
        } catch (IOException e) {
            throw new AssertionError("Failed to walk source root " + srcRoot + ": " + e.getMessage(), e);
        }

        // Pass 1: build per-file and global constant maps. The global map resolves cross-file
        // references (e.g. HttpMetricRegistry holds names used elsewhere). An identifier defined
        // with conflicting values in more than one file is "ambiguous" and never resolved globally.
        Map<Path, String> fileCodes = new LinkedHashMap<>();
        Map<Path, Map<String, String>> localConsts = new HashMap<>();
        Map<String, String> globalConst = new HashMap<>();
        Set<String> ambiguous = new HashSet<>();

        for (Path path : javaFiles) {
            String raw = readFileChecked(path);
            if (!raw.contains("Metric")) {
                continue;
            }
            String code = stripComments(raw);
            fileCodes.put(path, code);

            Map<String, String> consts = new HashMap<>();
            Matcher cm = CONST_RE.matcher(code);
            while (cm.find()) {
                consts.put(cm.group(1), cm.group(2));
            }
            localConsts.put(path, consts);

            for (Map.Entry<String, String> e : consts.entrySet()) {
                String key = e.getKey();
                String value = e.getValue();
                if (globalConst.containsKey(key) && !globalConst.get(key).equals(value)) {
                    ambiguous.add(key);
                }
                globalConst.put(key, value);
            }
        }

        // Pass 2: extract "name type unit" entries from metric constructors and codahale
        // name() calls. The type comes from the ctor class; the unit from the optional second
        // MetricUnit argument (none for histograms / codahale -> NO_UNIT).
        SortedSet<String> entries = new TreeSet<>();
        for (Map.Entry<Path, String> entry : fileCodes.entrySet()) {
            Map<String, String> consts = localConsts.get(entry.getKey());
            String code = entry.getValue();

            Matcher ctor = CTOR_RE.matcher(code);
            while (ctor.find()) {
                String type = CLASS_TYPE.get(ctor.group(1));
                String literal = ctor.group(2);
                String ident = ctor.group(3);
                String unitEnum = ctor.group(4);
                String unit;
                if (SUMMARY.equals(type) || unitEnum == null) {
                    unit = NO_UNIT;
                } else {
                    unit = unitEnum.toLowerCase();
                }
                if (literal != null) {
                    entries.add(literal + " " + type + " " + unit);
                } else if (ident != null) {
                    String resolved = resolve(ident, consts, globalConst, ambiguous);
                    if (resolved != null) {
                        entries.add(resolved + " " + type + " " + unit);
                    }
                }
            }

            Matcher nameCall = NAME_CALL_RE.matcher(code);
            while (nameCall.find()) {
                List<String> parts = new ArrayList<>();
                for (String arg : nameCall.group(1).split(",")) {
                    Matcher am = ARG_STRING_RE.matcher(arg);
                    if (am.matches()) {
                        parts.add(am.group(1));
                    } else {
                        break;
                    }
                }
                if (!parts.isEmpty()) {
                    entries.add(String.join(".", parts) + " " + SUMMARY + " " + NO_UNIT);
                }
            }
        }
        return entries;
    }

    /**
     * Resolve an identifier first-arg to its literal value: per-file constant first, then the
     * global UPPER_SNAKE constant map when unambiguous. Returns {@code null} for dynamic args.
     */
    private static String resolve(String ident, Map<String, String> consts,
                                  Map<String, String> globalConst, Set<String> ambiguous) {
        if (consts.containsKey(ident)) {
            return consts.get(ident);
        }
        if (UPPER_SNAKE_RE.matcher(ident).matches()
                && globalConst.containsKey(ident) && !ambiguous.contains(ident)) {
            return globalConst.get(ident);
        }
        return null;
    }

    private static String stripComments(String source) {
        String withoutBlockComments = source.replaceAll("(?s)/\\*.*?\\*/", "");
        return withoutBlockComments.replaceAll("//[^\\n]*", "");
    }

    /** Longest-first (stable) alternation so {@code GaugeMetricImpl} is tried before {@code GaugeMetric}. */
    private static String buildClassAlternation() {
        List<String> classes = new ArrayList<>(CLASS_TYPE.keySet());
        classes.sort((a, b) -> Integer.compare(b.length(), a.length()));
        return String.join("|", classes);
    }

    private static SortedSet<String> readGolden(Path goldenFile) {
        SortedSet<String> entries = new TreeSet<>();
        for (String line : readFileChecked(goldenFile).split("\n", -1)) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            entries.add(trimmed);
        }
        return entries;
    }

    /**
     * Rewrite only the data section of the golden file, preserving the human-maintained header
     * (leading comment / blank lines) so regeneration never churns the documentation block.
     */
    private static void writeGolden(Path goldenFile, SortedSet<String> entries) {
        List<String> headerLines = new ArrayList<>();
        if (Files.exists(goldenFile)) {
            for (String line : readFileChecked(goldenFile).split("\n", -1)) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    headerLines.add(line);
                } else {
                    break;
                }
            }
        }
        StringBuilder content = new StringBuilder();
        for (String header : headerLines) {
            content.append(header).append('\n');
        }
        for (String entry : entries) {
            content.append(entry).append('\n');
        }
        try {
            Files.write(goldenFile, content.toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new AssertionError("Failed to write golden file " + goldenFile + ": " + e.getMessage(), e);
        }
    }

    private static String buildFailureMessage(SortedSet<String> added, SortedSet<String> removed, Path goldenFile) {
        StringBuilder message = new StringBuilder();
        message.append("\nFE metric-name contract changed — this is an EXTERNAL contract.\n");
        message.append("Downstream consumers (dashboards, alerts, external monitoring tooling) depend on these.\n\n");
        if (!removed.isEmpty()) {
            message.append("REMOVED or RENAMED or RETYPED (").append(removed.size())
                    .append(") — these BREAK consumers, notify downstream metric consumers:\n");
            for (String entry : removed) {
                message.append("  - ").append(entry).append("\n");
            }
            message.append("\n");
        }
        if (!added.isEmpty()) {
            message.append("ADDED (").append(added.size()).append("):\n");
            for (String entry : added) {
                message.append("  + ").append(entry).append("\n");
            }
            message.append("\n");
        }
        message.append("If this change is intentional:\n");
        message.append("  1. Regenerate the golden file:\n");
        message.append("       cd fe && mvn -pl fe-core test -Dtest=FeMetricsContractGuardTest "
                + "-Dmetrics.guard.regenerate=true\n");
        message.append("  2. Re-run this test without the regenerate flag to confirm it passes.\n");
        message.append("  3. For any REMOVED/RENAMED name, notify downstream metric consumers before\n");
        message.append("     merging so dashboards, alerts, and external collectors can be updated.\n");
        message.append("  4. Update user-facing docs under docs/{en,zh}/administration/management/monitoring/"
                + " if applicable.\n");
        message.append("Golden file: ").append(goldenFile).append("\n");
        return message.toString();
    }

    /**
     * Resolve {@code relativePath} (rooted at {@code fe/}) by walking ancestors of the test
     * working directory. Surefire forks from {@code fe-core/}, Maven sometimes from {@code fe/},
     * and IDE runners often from the repository root — all three are handled without hardcoding.
     */
    private static Path resolveUnderFe(String relativePath) {
        Path workingDir = Paths.get("").toAbsolutePath();
        for (Path candidate = workingDir; candidate != null; candidate = candidate.getParent()) {
            Path direct = candidate.resolve(relativePath);
            if (Files.exists(direct)) {
                return direct;
            }
            Path underFe = candidate.resolve("fe").resolve(relativePath);
            if (Files.exists(underFe)) {
                return underFe;
            }
        }
        throw new AssertionError("Failed to locate " + relativePath + " from working directory " + workingDir);
    }

    private static String readFileChecked(Path path) {
        try {
            return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        } catch (IOException readFailure) {
            throw new AssertionError("Failed to read " + path + ": " + readFailure.getMessage(), readFailure);
        }
    }
}
