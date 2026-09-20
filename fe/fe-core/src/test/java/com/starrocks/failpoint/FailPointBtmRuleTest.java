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

package com.starrocks.failpoint;

import org.jboss.byteman.check.RuleCheck;
import org.jboss.byteman.check.RuleCheckResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Keeps conf/failpoint.btm honest.
 *
 * <p>A Byteman rule whose CLASS does not resolve, or whose METHOD name no longer exists, does not
 * fail loudly: Byteman simply never installs it, and {@code ADMIN ENABLE FAILPOINT} still reports
 * success because the frontend tracks the point by name and never checks that anything is
 * listening. The result is a rule that is enabled, never fires, and makes whatever test relies on
 * it pass while testing nothing. A rename or a signature change during an ordinary refactor is
 * enough to cause it, and nothing else in the build would notice.
 *
 * <p>So: for every rule over a {@code com.starrocks} class, assert the class loads and declares a
 * method of that name. Rules over third-party classes (the thrift server, for instance) are listed
 * and skipped rather than asserted — they are not on this repo's compile path, and failing on a
 * classpath difference would make this test the flaky one.
 *
 * <p>That check is reflection over the CLASS and METHOD lines, and it is deliberately kept: it
 * needs nothing but the JDK, it runs on every rule including the ones this repo cannot compile
 * against, and its failure message names the rule and the drift. {@link
 * #bytemanRuleCheckerAcceptsEveryRule()} then covers what reflection cannot see — the HELPER, the
 * {@code IF} and {@code DO} expressions, and the location specifier — by running Byteman's own
 * checker over the file. The two overlap on purpose; the cheap one still points at the cause when
 * the thorough one cannot run.
 */
public class FailPointBtmRuleTest {

    private static final String BTM_RELATIVE_PATH = "conf/failpoint.btm";

    private static class Rule {
        String name;
        String className;
        String methodName;
        /** Parameter types exactly as written in the script, or null when the rule omitted the list. */
        List<String> paramTypes;
    }

    /** Walks up from the working directory until it finds conf/failpoint.btm. */
    private static Path locateBtm() {
        Path dir = Paths.get("").toAbsolutePath();
        for (int i = 0; i < 6 && dir != null; i++) {
            Path candidate = dir.resolve(BTM_RELATIVE_PATH);
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            dir = dir.getParent();
        }
        return null;
    }

    private static List<Rule> parse(Path btm) throws IOException {
        List<Rule> rules = new ArrayList<>();
        Rule current = null;
        for (String raw : Files.readAllLines(btm, StandardCharsets.UTF_8)) {
            String line = raw.trim();
            if (line.startsWith("#") || line.isEmpty()) {
                continue;
            }
            if (line.startsWith("RULE ")) {
                current = new Rule();
                current.name = line.substring("RULE ".length()).trim();
            } else if (current == null) {
                continue;
            } else if (line.startsWith("CLASS ")) {
                current.className = line.substring("CLASS ".length()).trim();
            } else if (line.startsWith("METHOD ")) {
                String m = line.substring("METHOD ".length()).trim();
                // A METHOD line may carry a parameter list: foo(Bar, int). Keep it -- Byteman
                // installs the rule only on that exact overload, so dropping the list here would
                // let a mistyped or drifted signature pass this test while the rule stays dead.
                int paren = m.indexOf('(');
                if (paren < 0) {
                    current.methodName = m;
                    current.paramTypes = null;          // matches every overload, as Byteman does
                } else {
                    current.methodName = m.substring(0, paren).trim();
                    String inside = m.substring(paren + 1, m.lastIndexOf(')') < 0
                            ? m.length() : m.lastIndexOf(')')).trim();
                    List<String> params = new ArrayList<>();
                    if (!inside.isEmpty()) {
                        for (String part : inside.split(",")) {
                            if (!part.trim().isEmpty()) {
                                params.add(part.trim());
                            }
                        }
                    }
                    current.paramTypes = params;        // empty list == "()" == the no-arg overload
                }
            } else if (line.equals("ENDRULE")) {
                rules.add(current);
                current = null;
            }
        }
        Assertions.assertNull(current, "failpoint.btm has a RULE without a matching ENDRULE");
        return rules;
    }

    /**
     * Does this method's parameter list match what the script wrote?
     *
     * <p>Byteman accepts a bare type name as well as a fully qualified one, so {@code ExecPlan}
     * and {@code com.starrocks.sql.plan.ExecPlan} both name the same parameter, and primitives
     * ({@code int}) match by their plain name. Matching is therefore: exact fully-qualified name,
     * exact simple name, or the reflected name ending in {@code .Name} / {@code $Name} for nested
     * classes. Anything looser would defeat the point of checking at all.
     */
    private static boolean signatureMatches(Method m, List<String> declared) {
        Class<?>[] actual = m.getParameterTypes();
        if (actual.length != declared.size()) {
            return false;
        }
        for (int i = 0; i < actual.length; i++) {
            if (!parameterMatches(actual[i], declared.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean parameterMatches(Class<?> actual, String declared) {
        String want = declared.trim();
        // Byteman writes array types as Foo[]; getName() gives [Lcom.Foo; so compare canonical too.
        String canonical = actual.getCanonicalName();
        return want.equals(actual.getName())
                || want.equals(actual.getSimpleName())
                || (canonical != null && want.equals(canonical))
                || actual.getName().endsWith("." + want)
                || actual.getName().endsWith("$" + want);
    }

    /**
     * The signature check has to actually discriminate, otherwise it is decoration.
     *
     * <p>{@code String.indexOf} is used because it carries four overloads in the JDK, which is the
     * shape that makes a name-only check useless: if the rule targets one overload and that one
     * drifts, the others keep a name-only check green while Byteman quietly fails to install.
     */
    @Test
    public void signatureCheckDiscriminatesBetweenOverloads() throws Exception {
        Method indexOfInt = String.class.getDeclaredMethod("indexOf", int.class);
        Method indexOfStr = String.class.getDeclaredMethod("indexOf", String.class);
        Method indexOfIntInt = String.class.getDeclaredMethod("indexOf", int.class, int.class);

        // primitives match by plain name
        Assertions.assertTrue(signatureMatches(indexOfInt, Collections.singletonList("int")));
        // reference types match by simple name and by fully qualified name
        Assertions.assertTrue(signatureMatches(indexOfStr, Collections.singletonList("String")));
        Assertions.assertTrue(signatureMatches(indexOfStr, Collections.singletonList("java.lang.String")));

        // wrong type, wrong arity, and wrong order must all be rejected
        Assertions.assertFalse(signatureMatches(indexOfInt, Collections.singletonList("String")));
        Assertions.assertFalse(signatureMatches(indexOfStr, Collections.singletonList("int")));
        Assertions.assertFalse(signatureMatches(indexOfIntInt, Collections.singletonList("int")));
        Assertions.assertFalse(signatureMatches(indexOfInt, Arrays.asList("int", "int")));
        Assertions.assertFalse(signatureMatches(indexOfIntInt, Arrays.asList("int", "String")));

        // "()" means the no-arg overload, not "any overload"
        Assertions.assertFalse(signatureMatches(indexOfInt, Collections.emptyList()));
        Assertions.assertTrue(signatureMatches(
                String.class.getDeclaredMethod("trim"), Collections.emptyList()));
    }

    @Test
    public void everyStarRocksRuleResolves() throws IOException {
        Path btm = locateBtm();
        // Skip rather than fail when the file is not reachable: some build layouts run tests from
        // a directory the repo root is not an ancestor of, and a false red here would teach people
        // to ignore this test.
        Assumptions.assumeTrue(btm != null,
                "conf/failpoint.btm not found relative to the working directory");

        List<Rule> rules = parse(btm);
        Assertions.assertFalse(rules.isEmpty(), "parsed no rules out of " + btm);

        Set<String> names = new LinkedHashSet<>();
        List<String> problems = new ArrayList<>();
        List<String> skipped = new ArrayList<>();

        for (Rule rule : rules) {
            // Duplicate rule names are their own silent failure: ADMIN ENABLE FAILPOINT arms one
            // of them and the other stays dark.
            if (!names.add(rule.name)) {
                problems.add("duplicate RULE name: " + rule.name);
            }
            if (rule.className == null || rule.methodName == null) {
                problems.add(rule.name + ": missing CLASS or METHOD");
                continue;
            }
            if (!rule.className.startsWith("com.starrocks.")) {
                skipped.add(rule.name + " -> " + rule.className);
                continue;
            }
            Class<?> clazz;
            try {
                clazz = Class.forName(rule.className, false, getClass().getClassLoader());
            } catch (ClassNotFoundException | NoClassDefFoundError e) {
                problems.add(rule.name + ": CLASS does not resolve: " + rule.className);
                continue;
            }
            boolean nameFound = false;
            boolean signatureFound = false;
            for (Class<?> c = clazz; c != null && !(nameFound && signatureFound); c = c.getSuperclass()) {
                for (Method m : c.getDeclaredMethods()) {
                    if (!m.getName().equals(rule.methodName)) {
                        continue;
                    }
                    nameFound = true;
                    if (rule.paramTypes == null || signatureMatches(m, rule.paramTypes)) {
                        signatureFound = true;
                        break;
                    }
                }
            }
            if (!nameFound) {
                problems.add(rule.name + ": " + rule.className + " declares no method named "
                        + rule.methodName + " (a rename leaves the rule silently dead)");
            } else if (!signatureFound) {
                problems.add(rule.name + ": " + rule.className + "." + rule.methodName
                        + " has no overload matching (" + String.join(", ", rule.paramTypes)
                        + ") -- Byteman installs the rule only on that exact signature, so it "
                        + "would stay dead while a same-named overload keeps this check happy");
            }
        }

        if (!skipped.isEmpty()) {
            System.out.println("[FailPointBtmRuleTest] not checked (third-party classes): " + skipped);
        }
        Assertions.assertTrue(problems.isEmpty(), "failpoint.btm rules that cannot fire:\n  "
                + String.join("\n  ", problems));
    }

    /**
     * Hands the whole script to Byteman's own rule checker -- the engine behind {@code bmcheck}.
     *
     * <p>{@link #everyStarRocksRuleResolves()} answers one question by reflection: does the CLASS
     * load, and does it declare a method of that name and signature. That leaves the rest of a rule
     * unchecked, and every one of those parts can keep a rule from installing just as quietly:
     *
     * <ul>
     *   <li>a {@code HELPER} that does not declare the method the {@code IF} calls, or calls it
     *       with argument types it will not take;</li>
     *   <li>a {@code DO} action that does not type check against the method it was injected into --
     *       {@code DO return;} in a method that returns a value, or {@code DO return true;} in one
     *       that returns void;</li>
     *   <li>a location specifier naming an injection point that is not there, such as
     *       {@code AT INVOKE foo} when nothing in the body calls {@code foo}.</li>
     * </ul>
     *
     * <p>The checker loads the target's bytecode, runs the real transform, then type checks and
     * compiles the rule -- the same work the agent does at startup, which is what makes a pass here
     * meaningful rather than approximate.
     *
     * <p>What is fatal and what is not follows the same line the rest of this file draws. Errors,
     * parse errors and type errors are asserted: they are the checker saying a rule cannot fire.
     * Warnings are printed, with one exception -- "unable to transform" over a class in this repo
     * means no injection point was found, so the rule installs nowhere, which is exactly the
     * silent death being guarded against. Warnings about third-party classes stay non-fatal for
     * the reason those classes are skipped above: a classpath difference must not turn this into
     * the flaky test.
     */
    @Test
    public void bytemanRuleCheckerAcceptsEveryRule() {
        Path btm = locateBtm();
        Assumptions.assumeTrue(btm != null,
                "conf/failpoint.btm not found relative to the working directory");

        RuleCheckResult result;
        try {
            RuleCheck check = new RuleCheck();
            // A read failure is recorded on the result as an error, so the return value needs no
            // separate assertion -- it is collected with everything else below.
            check.addRuleFile(btm.toString());
            check.checkRules();
            result = check.getResult();
        } catch (LinkageError e) {
            // The checker builds Byteman's transformer, which needs the agent's own ASM. If that is
            // absent from this classpath the checker cannot run at all. That is a fact about the
            // environment, not about the rules, and a red build for it would only teach people to
            // ignore this test.
            Assumptions.assumeTrue(false, "Byteman's rule checker is not usable here: " + e);
            return;
        }

        List<String> fatal = new ArrayList<>();
        fatal.addAll(result.getErrorMessages());
        fatal.addAll(result.getParseErrorMessages());
        fatal.addAll(result.getTypeErrorMessages());

        List<String> warnings = new ArrayList<>(result.getWarningMessages());
        warnings.addAll(result.getTypeWarningMessages());
        for (String warning : warnings) {
            if (warning.contains("Unable to transform class com.starrocks.")) {
                fatal.add(warning);
            } else {
                System.out.println("[FailPointBtmRuleTest] " + warning);
            }
        }

        Assertions.assertTrue(fatal.isEmpty(),
                "Byteman's rule checker rejected rules in " + btm + ":\n  "
                        + String.join("\n  ", fatal));

        // The checker logs a line per rule it looks at. If it logged nothing, it parsed no rules --
        // every list above would be empty and this test would pass having checked nothing, which is
        // the same shape of lie it exists to catch.
        Assertions.assertTrue(result.hasInfo(),
                "Byteman's rule checker processed no rules out of " + btm);
    }
}
