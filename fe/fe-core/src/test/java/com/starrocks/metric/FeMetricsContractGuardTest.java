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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
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
 * Adding, removing, or renaming a metric &mdash; or flipping its type (counter/gauge) or its unit
 * &mdash; silently breaks those consumers: the type drives the Prometheus {@code # TYPE} line
 * (PrometheusMetricVisitor) and the unit is exported on the JSON metrics endpoint
 * (JsonMetricVisitor). This test scans the fe-core main sources and records, for each metric,
 * a {@code "name type unit"} triple, then compares the result against a committed golden file
 * ({@code fe-core/src/test/resources/metric/fe_metrics_golden.txt}). Any drift fails the
 * build with an explicit added/removed report and a directive to notify downstream consumers.
 *
 * <p>The inventory scan can only see what it knows how to classify, so a second test
 * ({@link #everyMetricClassIsClassified()}) closes that loop: any metric class instantiated
 * under the scanned sources must either be classified in {@link #CLASS_TYPE} or be listed in
 * {@link #UNINVENTORIED_CLASSES} with a reason. A new metric base class therefore fails the
 * build instead of silently exporting series the golden file never sees.
 *
 * <p>The test is a pure source scan &mdash; it does NOT bootstrap the FE, so it runs fast and
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
        // Abstract, but instantiated with an anonymous body (MetricRepo's statistics-cache
        // counters); its ctor hardcodes MetricType.COUNTER.
        CLASS_TYPE.put("CounterMetric", "counter");
        CLASS_TYPE.put("GaugeMetricImpl", "gauge");
        CLASS_TYPE.put("GaugeMetric", "gauge");
        CLASS_TYPE.put("LeaderAwareGaugeMetricLong", "gauge");
        CLASS_TYPE.put("LeaderAwareGaugeMetricInteger", "gauge");
        CLASS_TYPE.put("HistogramMetric", "summary");
        CLASS_TYPE.put("LeaderAwareHistogramMetric", "summary");
    }

    /**
     * Metric classes deliberately kept OUT of the inventory, mapped to the reason they cannot
     * contribute a statically known name. Every other metric class that is instantiated under
     * the scanned sources must appear in {@link #CLASS_TYPE}; see
     * {@link #everyMetricClassIsClassified()}.
     */
    private static final Map<String, String> UNINVENTORIED_CLASSES = new LinkedHashMap<>();

    static {
        UNINVENTORIED_CLASSES.put("PrometheusMetric",
                "wrapper around externally scraped Prometheus metrics - name, type and unit all come from "
                        + "the runtime MetricMetadata/snapshot, so there is no FE-authored name to pin");
    }

    /**
     * Base types whose subclasses are exported as FE metrics: {@code Metric} for the FE
     * hierarchy and codahale's {@code Histogram} for {@code HistogramMetric}.
     */
    private static final Set<String> METRIC_ROOTS = new HashSet<>(Arrays.asList("Metric", "Histogram"));

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
            "new\\s+(" + buildClassAlternation(CLASS_TYPE.keySet()) + ")\\s*(?:<[^>]*>)?\\s*\\(\\s*(?:"
                    + STRING + "|([A-Za-z_][\\w.]*))"
                    + "(?:\\s*,\\s*(?:Metric\\.)?MetricUnit\\.([A-Za-z_]+))?");

    // Codahale histogram name builder: `MetricRegistry.name(<args>)`.
    private static final Pattern NAME_CALL_RE = Pattern.compile("MetricRegistry\\.name\\s*\\(([^)]*)\\)");

    // A single argument that is exactly one string literal (group 1 = its content).
    private static final Pattern ARG_STRING_RE = Pattern.compile("^\\s*" + STRING + "\\s*$");

    // A single argument that is exactly one identifier (group 1), e.g. a name constant.
    private static final Pattern ARG_IDENT_RE = Pattern.compile("^\\s*([A-Za-z_][\\w.]*)\\s*$");

    // Identifier first-args are only resolved through the global constant map when they look
    // like a constant (UPPER_SNAKE_CASE); camelCase params go through helper-call resolution.
    private static final Pattern UPPER_SNAKE_RE = Pattern.compile("^[A-Z][A-Z0-9_]*$");

    // `class Foo[<...>] extends Bar` -> group(1)=class, group(2)=supertype simple name.
    private static final Pattern CLASS_DECL_RE = Pattern.compile(
            "\\bclass\\s+([A-Za-z_]\\w*)\\s*(?:<[^>]*>)?\\s+extends\\s+([A-Za-z_]\\w*)");

    // A method declaration that starts its line with at least one modifier keyword, so control
    // flow (`if (...) {`) can never match -> group(1)=method name, group(2)=raw parameter list.
    private static final Pattern METHOD_DECL_RE = Pattern.compile(
            "(?m)^[ \\t]*(?:(?:public|protected|private|static|final|abstract|synchronized|default|native|strictfp)"
                    + "[ \\t]+)+[\\w.$<>,?\\[\\]\\s]+?\\s+([A-Za-z_]\\w*)\\s*\\(([^;{}]*?)\\)\\s*(?:throws[^{;]*?)?\\{");

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
     * Fail-closed companion to {@link #feMetricNamesMatchGoldenContract()}.
     *
     * <p>The inventory scan recognises metric constructors by class name, so a metric class the
     * scanner has never heard of contributes nothing and the golden comparison still passes
     * &mdash; the new series would ship completely unguarded. This test walks the {@code Metric} /
     * {@code Histogram} class family declared under the scanned sources and fails on any member
     * that is instantiated but neither classified in {@link #CLASS_TYPE} nor documented in
     * {@link #UNINVENTORIED_CLASSES}.
     */
    @Test
    public void everyMetricClassIsClassified() {
        Map<Path, String> fileCodes = loadSources(resolveUnderFe(SRC_ROOT_RELATIVE));

        Map<String, String> superOf = new LinkedHashMap<>();
        for (String code : fileCodes.values()) {
            Matcher declaration = CLASS_DECL_RE.matcher(code);
            while (declaration.find()) {
                superOf.put(declaration.group(1), declaration.group(2));
            }
        }

        // Transitive closure: a class is in the family when walking up its `extends` chain
        // reaches one of the roots. The hop cap is a cheap guard against a malformed chain.
        SortedSet<String> family = new TreeSet<>();
        for (Map.Entry<String, String> declared : superOf.entrySet()) {
            String current = declared.getValue();
            for (int hop = 0; current != null && hop <= superOf.size(); hop++) {
                if (METRIC_ROOTS.contains(current)) {
                    family.add(declared.getKey());
                    break;
                }
                current = superOf.get(current);
            }
        }
        family.removeAll(CLASS_TYPE.keySet());
        family.removeAll(UNINVENTORIED_CLASSES.keySet());
        if (family.isEmpty()) {
            return;
        }

        Pattern instantiation = Pattern.compile("new\\s+(" + buildClassAlternation(family) + ")\\s*[<(]");
        SortedMap<String, String> violations = new TreeMap<>();
        for (Map.Entry<Path, String> entry : fileCodes.entrySet()) {
            Matcher matcher = instantiation.matcher(entry.getValue());
            while (matcher.find()) {
                violations.putIfAbsent(matcher.group(1), entry.getKey().getFileName().toString());
            }
        }
        if (violations.isEmpty()) {
            return;
        }

        StringBuilder message = new StringBuilder();
        message.append("\nUnclassified FE metric class(es) - these export series the metric contract guard "
                + "cannot see.\n\n");
        for (Map.Entry<String, String> violation : violations.entrySet()) {
            message.append("  - ").append(violation.getKey())
                    .append("  (instantiated in ").append(violation.getValue()).append(")\n");
        }
        message.append("\nFix one of:\n");
        message.append("  a. Classify it: add the class to CLASS_TYPE in ")
                .append(FeMetricsContractGuardTest.class.getSimpleName())
                .append(" with its exported type\n");
        message.append("     (counter | gauge | summary - whatever MetricType its ctor passes up), then regenerate\n");
        message.append("     the golden file so the new names are reviewed:\n");
        message.append("       cd fe && mvn -pl fe-core test -Dtest=FeMetricsContractGuardTest "
                + "-Dmetrics.guard.regenerate=true\n");
        message.append("  b. Exclude it: add the class to UNINVENTORIED_CLASSES with the reason its name cannot be\n");
        message.append("     known statically (e.g. the name arrives at runtime from an external source).\n");
        message.append("Do NOT leave it unlisted - an unlisted class means its metrics ship with no contract guard.\n");
        throw new AssertionError(message.toString());
    }

    /**
     * Read every {@code *.java} under {@code srcRoot} whose raw text contains "Metric" and return
     * its comment-stripped source, keyed by path in a stable (sorted) order.
     */
    private static Map<Path, String> loadSources(Path srcRoot) {
        List<Path> javaFiles;
        try (Stream<Path> stream = Files.walk(srcRoot)) {
            javaFiles = stream.filter(p -> p.toString().endsWith(".java")).sorted().collect(Collectors.toList());
        } catch (IOException e) {
            throw new AssertionError("Failed to walk source root " + srcRoot + ": " + e.getMessage(), e);
        }

        Map<Path, String> fileCodes = new LinkedHashMap<>();
        for (Path path : javaFiles) {
            String raw = readFileChecked(path);
            if (!raw.contains("Metric")) {
                continue;
            }
            fileCodes.put(path, stripComments(raw));
        }
        return fileCodes;
    }

    /**
     * Scan the fe-core sources and return the sorted, de-duplicated set of
     * {@code "name type unit"} entries. Mirrors the offline generator exactly.
     */
    private static SortedSet<String> scanMetricTriples(Path srcRoot) {
        Map<Path, String> fileCodes = loadSources(srcRoot);

        // Pass 1: build per-file and global constant maps. The global map resolves cross-file
        // references (e.g. HttpMetricRegistry holds names used elsewhere). An identifier defined
        // with conflicting values in more than one file is "ambiguous" and never resolved globally.
        Map<Path, Map<String, String>> localConsts = new HashMap<>();
        Map<String, String> globalConst = new HashMap<>();
        Set<String> ambiguous = new HashSet<>();

        for (Map.Entry<Path, String> file : fileCodes.entrySet()) {
            Map<String, String> consts = new HashMap<>();
            Matcher cm = CONST_RE.matcher(file.getValue());
            while (cm.find()) {
                consts.put(cm.group(1), cm.group(2));
            }
            localConsts.put(file.getKey(), consts);

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
            List<MethodDecl> methods = parseMethods(code);

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
                    // Locale.ROOT: the default locale must not change the snapshot. In a Turkish
                    // locale toLowerCase() maps `I` to a dotless `i`, which would retype every
                    // NOUNIT/MILLISECONDS entry and break the committed ASCII inventory.
                    unit = unitEnum.toLowerCase(Locale.ROOT);
                }
                if (literal != null) {
                    entries.add(literal + " " + type + " " + unit);
                    continue;
                }
                if (ident == null) {
                    continue;
                }
                String resolved = resolve(ident, consts, globalConst, ambiguous);
                if (resolved != null) {
                    entries.add(resolved + " " + type + " " + unit);
                    continue;
                }
                for (String fromCallSite : resolveThroughHelper(
                        code, methods, ctor.start(), ident, consts, globalConst, ambiguous)) {
                    entries.add(fromCallSite + " " + type + " " + unit);
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

    /**
     * Resolve a metric name that reaches the ctor as a {@code String} parameter of a registration
     * helper, by reading the literals its call sites pass in that position &mdash; e.g.
     * {@code emitGauge(visitor, "mv_global_refresh_pending_jobs", ...)} feeding
     * {@code new GaugeMetricImpl<>(name, ...)}. The argument may also be a name constant rather than
     * an inline literal. Such names are fixed at compile time, so leaving them out would let a
     * rename pass the guard untouched.
     *
     * <p>Only call sites in the SAME file are followed. These helpers are private, and matching
     * a bare method name across files would bind unrelated same-named methods and invent entries.
     *
     * @return the literals passed in that position, empty when the argument is genuinely dynamic.
     */
    private static List<String> resolveThroughHelper(String code, List<MethodDecl> methods, int ctorStart,
                                                     String ident, Map<String, String> consts,
                                                     Map<String, String> globalConst, Set<String> ambiguous) {
        List<MethodDecl> enclosing = new ArrayList<>();
        for (MethodDecl method : methods) {
            if (method.bodyStart <= ctorStart && ctorStart < method.bodyEnd) {
                enclosing.add(method);
            }
        }
        // Innermost enclosing method first, so a nested anonymous-class method cannot shadow the
        // helper that actually declares the parameter.
        enclosing.sort((a, b) -> Integer.compare(b.bodyStart, a.bodyStart));

        for (MethodDecl method : enclosing) {
            int index = method.stringParamIndex(ident);
            if (index < 0) {
                continue;
            }
            List<String> names = new ArrayList<>();
            Matcher call = Pattern.compile("\\b" + Pattern.quote(method.name) + "\\s*\\(").matcher(code);
            while (call.find()) {
                if (call.start() >= method.headerStart && call.end() <= method.bodyStart) {
                    continue;   // the declaration itself, not a call
                }
                List<String> args = splitCallArgs(code, call.end() - 1);
                if (args == null || index >= args.size()) {
                    continue;
                }
                String argument = args.get(index);
                Matcher literalArg = ARG_STRING_RE.matcher(argument);
                if (literalArg.matches()) {
                    names.add(literalArg.group(1));
                    continue;
                }
                // The call site may pass a name constant rather than an inline literal
                // (ResourceGroupMetricMgr does); those names are just as fixed.
                Matcher identArg = ARG_IDENT_RE.matcher(argument);
                if (identArg.matches()) {
                    String resolvedArg = resolve(identArg.group(1), consts, globalConst, ambiguous);
                    if (resolvedArg != null) {
                        names.add(resolvedArg);
                    }
                }
            }
            return names;
        }
        return Collections.emptyList();
    }

    /** A method declaration and the span of its body, used to bind a ctor arg to a parameter. */
    private static final class MethodDecl {
        private final String name;
        private final List<String> params;
        private final int headerStart;
        private final int bodyStart;
        private final int bodyEnd;

        private MethodDecl(String name, String rawParams, int headerStart, int bodyStart, int bodyEnd) {
            this.name = name;
            this.params = splitParams(rawParams);
            this.headerStart = headerStart;
            this.bodyStart = bodyStart;
            this.bodyEnd = bodyEnd;
        }

        /** Position of the {@code String} parameter named {@code ident}, or {@code -1}. */
        private int stringParamIndex(String ident) {
            for (int i = 0; i < params.size(); i++) {
                String[] tokens = params.get(i).trim().split("\\s+");
                if (tokens.length < 2) {
                    continue;
                }
                if (tokens[tokens.length - 1].equals(ident) && "String".equals(tokens[tokens.length - 2])) {
                    return i;
                }
            }
            return -1;
        }
    }

    private static List<MethodDecl> parseMethods(String code) {
        List<MethodDecl> methods = new ArrayList<>();
        Matcher matcher = METHOD_DECL_RE.matcher(code);
        while (matcher.find()) {
            int openBrace = matcher.end() - 1;
            int bodyEnd = findBlockEnd(code, openBrace);
            if (bodyEnd < 0) {
                continue;
            }
            methods.add(new MethodDecl(matcher.group(1), matcher.group(2), matcher.start(), openBrace + 1, bodyEnd));
        }
        return methods;
    }

    /** Split a declaration's parameter list on top-level commas, keeping generics intact. */
    private static List<String> splitParams(String rawParams) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        for (int i = 0; i < rawParams.length(); i++) {
            char c = rawParams.charAt(i);
            if (c == '<' || c == '(' || c == '[') {
                depth++;
            } else if (c == '>' || c == ')' || c == ']') {
                depth--;
            } else if (c == ',' && depth == 0) {
                parts.add(current.toString());
                current.setLength(0);
                continue;
            }
            current.append(c);
        }
        if (!current.toString().trim().isEmpty()) {
            parts.add(current.toString());
        }
        return parts;
    }

    /**
     * Split the argument list of the call whose {@code '('} sits at {@code openParen}. Nesting of
     * {@code ()}, {@code []} and {@code {}} is tracked and string/char literals are skipped;
     * {@code <} is deliberately NOT tracked because an argument may contain a less-than comparison.
     *
     * @return the raw arguments, or {@code null} when the list is unbalanced.
     */
    private static List<String> splitCallArgs(String code, int openParen) {
        List<String> args = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        for (int i = openParen; i < code.length(); i++) {
            char c = code.charAt(i);
            if (c == '"' || c == '\'') {
                int end = skipLiteral(code, i);
                if (end < 0) {
                    return null;
                }
                current.append(code, i, end + 1);
                i = end;
                continue;
            }
            if (c == '(' || c == '[' || c == '{') {
                depth++;
                if (depth == 1) {
                    continue;   // the opening paren of this call itself
                }
            } else if (c == ')' || c == ']' || c == '}') {
                depth--;
                if (depth == 0) {
                    args.add(current.toString());
                    return args;
                }
            } else if (c == ',' && depth == 1) {
                args.add(current.toString());
                current.setLength(0);
                continue;
            }
            current.append(c);
        }
        return null;
    }

    /** Index of the closing brace matching the {@code '{'} at {@code openBrace}, or {@code -1}. */
    private static int findBlockEnd(String code, int openBrace) {
        int depth = 0;
        for (int i = openBrace; i < code.length(); i++) {
            char c = code.charAt(i);
            if (c == '"' || c == '\'') {
                int end = skipLiteral(code, i);
                if (end < 0) {
                    return -1;
                }
                i = end;
            } else if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    /** Index of the quote closing the literal that opens at {@code start}, or {@code -1}. */
    private static int skipLiteral(String code, int start) {
        char quote = code.charAt(start);
        for (int i = start + 1; i < code.length(); i++) {
            char c = code.charAt(i);
            if (c == '\\') {
                i++;
            } else if (c == quote) {
                return i;
            }
        }
        return -1;
    }

    private static String stripComments(String source) {
        String withoutBlockComments = source.replaceAll("(?s)/\\*.*?\\*/", "");
        return withoutBlockComments.replaceAll("//[^\\n]*", "");
    }

    /** Longest-first (stable) alternation so {@code GaugeMetricImpl} is tried before {@code GaugeMetric}. */
    private static String buildClassAlternation(Set<String> classNames) {
        List<String> classes = new ArrayList<>(classNames);
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
        message.append("\nFE metric-name contract changed - this is an EXTERNAL contract.\n");
        message.append("Downstream consumers (dashboards, alerts, external monitoring tooling) depend on these.\n\n");
        if (!removed.isEmpty()) {
            message.append("REMOVED or RENAMED or RETYPED (").append(removed.size())
                    .append(") - these BREAK consumers, notify downstream metric consumers:\n");
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
     * and IDE runners often from the repository root - all three are handled without hardcoding.
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
