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

package com.starrocks.common.util.concurrent.lock;

import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Reporting channel for violations of the invariants the FE metadata lock layer relies on but
 * cannot express in a type.
 * <p>
 * A check built on top of this knows the rule but not the set of code that breaks it, so the
 * channel has to survive being switched on in production against an unknown violation set. That
 * shapes every decision here:
 * <ul>
 *     <li>three modes, so a rule can be observed ({@code warn}) before it is enforced
 *         ({@code error}), and switched off entirely ({@code off}) without a restart;</li>
 *     <li>throttling is <b>per call site</b>, not global. A single global gate turns the log into
 *         a lottery: the busiest violating site wins every interval and a newly introduced site may
 *         never be printed at all. Keyed per site, a site that has never been seen always prints
 *         its first occurrence;</li>
 *     <li>every log line carries a stack. This is not optional — the offending frame sits several
 *         levels below the check, so a stackless line only ever points at the check itself;</li>
 *     <li>counts are exact even though logging is throttled, so "how often" stays answerable after
 *         the log lines have been dropped.</li>
 * </ul>
 * All of that runs only once a violation has already been established. A conforming lock
 * acquisition never reaches {@link #report}, so counting, resolving the call site and capturing the
 * stack are off the hot path.
 */
public class LockInvariantViolations {
    private static final Logger LOG = LogManager.getLogger(LockInvariantViolations.class);

    public enum Mode {
        /** Do not check at all. */
        OFF,
        /** Check, log the violation, let the operation proceed. */
        WARN,
        /** Check, refuse the operation. */
        ERROR;

        public static Mode parse(String value) {
            if (value == null) {
                return WARN;
            }
            switch (value.trim().toLowerCase()) {
                case "off":
                    return OFF;
                case "error":
                    return ERROR;
                default:
                    return WARN;
            }
        }
    }

    /**
     * Raises {@code warn} to {@code error} inside the unit-test JVM, so a newly introduced
     * violation fails the test that introduced it instead of scrolling past in a log.
     * <p>
     * Deliberately not {@code FeConstants.runningUnitTest}: that flag is opt-in per test class
     * (every one of its uses in main code means "skip this when testing", none means "be stricter
     * when testing"), so it is set for far too few tests to be a usable gate. This property is set
     * once for the whole surefire JVM in {@code fe-core/pom.xml}.
     * <p>
     * {@code off} and an explicit {@code error} pass through untouched — code that means to lock a
     * synthetic id has to turn the check off and say why.
     */
    private static final boolean STRICT_IN_TEST = Boolean.getBoolean("starrocks.lock.invariant.strict.in.test");

    /**
     * Upper bound on distinct call sites tracked. Beyond it every further site shares one bucket:
     * the throttling budget degrades, memory does not.
     */
    private static final int MAX_TRACKED_SITES = 512;
    private static final String OVERFLOW_SITE = "<site-table-full>";
    private static final String UNKNOWN_SITE = "<unknown>";

    private static final String LOCK_PACKAGE = LockInvariantViolations.class.getPackage().getName() + ".";

    /**
     * Leading token on every violation report, in the log and in the exception alike, so one grep
     * covers both a running FE's {@code fe.warn.log} and a CI test failure.
     * <p>
     * The log pattern already carries {@code [LockInvariantViolations.report():NNN]}, but that names
     * the reporter rather than the offence and dies the moment the class is renamed. The fields that
     * follow the tag are {@code key=value} for the same reason -- {@code kind} and {@code site} are
     * meant to be cut out and counted, not read:
     * <pre>
     * grep LOCK_INVARIANT_VIOLATION fe.warn.log                        # every line that was emitted
     * grep -o 'site=[^ ]*' fe.warn.log | sort | uniq -c | sort -rn     # which call sites are involved
     * grep -o 'kind=[^ ]*' fe.warn.log | sort | uniq -c                # by rule
     * </pre>
     * Those counts are of <em>emitted lines</em>, not of violations: throttling drops lines, so a site that
     * violated a hundred times within one interval contributes one. The true number is the {@code occurrences=}
     * field on the line, which is the exact running count for that site. Set
     * {@link Config#lock_invariant_violation_log_interval_ms} to 0 while investigating if you want the two to
     * coincide.
     */
    public static final String LOG_TAG = "LOCK_INVARIANT_VIOLATION";

    private static final ConcurrentHashMap<String, Site> SITES = new ConcurrentHashMap<>();

    /** Cached parse of {@link Config#lock_target_validation_mode}, which is mutable at runtime. */
    private static volatile ModeCache modeCache;

    private static final class Site {
        private final AtomicLong count = new AtomicLong();
        /** Timestamp of the last emitted log line; 0 means this site has never been logged. */
        private final AtomicLong lastLogTimeMs = new AtomicLong(0);
    }

    private static final class ModeCache {
        private final String raw;
        private final Mode mode;

        private ModeCache(String raw, Mode mode) {
            this.raw = raw;
            this.mode = mode;
        }
    }

    private LockInvariantViolations() {
    }

    /**
     * The mode the lock-target check runs in right now, honouring the unit-test escalation.
     * <p>
     * Called on every guarded lock acquisition, so the string parse is cached against the config
     * value's identity; {@code Config} replaces the reference when the value is set, and a stale
     * comparison costs at most one redundant parse.
     */
    public static Mode currentMode() {
        return effectiveMode(Config.lock_target_validation_mode);
    }

    public static Mode effectiveMode(String configured) {
        ModeCache cached = modeCache;
        if (cached != null && cached.raw == configured) {
            return cached.mode;
        }
        Mode mode = Mode.parse(configured);
        if (mode == Mode.WARN && STRICT_IN_TEST) {
            mode = Mode.ERROR;
        }
        modeCache = new ModeCache(configured, mode);
        return mode;
    }

    /**
     * Record one violation of {@code kind}.
     *
     * @param kind    the invariant that was broken, as a short snake_case token. It is both the
     *                throttling key's prefix and the report's {@code kind=} field, so it must stay a
     *                constant with no whitespace: per-call data in it would give every call its own
     *                bucket and defeat the throttle, and a space in it would break the field.
     * @param detail  what was wrong with this particular call, including the offending identity.
     *                This is the human half of the report; {@code kind} is the machine half.
     * @param remedy  what the caller should do instead; also carried in the thrown exception,
     *                because whoever trips this in CI has usually never touched the lock layer.
     * @param mode    already-resolved mode; the caller has it because it short-circuits on
     *                {@link Mode#OFF} before doing any work.
     * @throws IllegalStateException in {@link Mode#ERROR}, refusing the operation.
     */
    public static void report(String kind, String detail, String remedy, Mode mode) {
        if (mode == Mode.OFF) {
            return;
        }

        String callSite = currentCallSite();
        Site site = siteFor(kind + "@" + callSite);
        long occurrences = site.count.incrementAndGet();
        String message = LOG_TAG + " kind=" + kind + " site=" + callSite + " detail=" + quoted(detail);

        if (mode == Mode.ERROR) {
            throw new IllegalStateException(message + " remedy=" + quoted(remedy));
        }

        if (shouldLog(site, System.currentTimeMillis(), Config.lock_invariant_violation_log_interval_ms)) {
            // The two-argument overload is deliberate: it always renders the stack. Passing the
            // Throwable as a trailing parameter of a formatted call would only work while the
            // placeholder count stays below the argument count, so adding one "{}" later would
            // silently swallow the stack -- and the stack is the whole point of the log line.
            LOG.warn(message + " occurrences=" + occurrences + " remedy=" + quoted(remedy),
                    new Throwable("lock invariant violation, stack of the offending caller"));
        }
    }

    /**
     * Keeps a value on one line and inside one quoted field, so a report never breaks the
     * line-oriented greps the tag exists for. Off the hot path, like everything else here.
     */
    private static String quoted(String value) {
        return '"' + value.replace('\n', ' ').replace('\r', ' ').replace('"', '\'') + '"';
    }

    private static Site siteFor(String siteKey) {
        Site site = SITES.get(siteKey);
        if (site != null) {
            return site;
        }
        if (SITES.size() >= MAX_TRACKED_SITES) {
            return SITES.computeIfAbsent(OVERFLOW_SITE, k -> new Site());
        }
        return SITES.computeIfAbsent(siteKey, k -> new Site());
    }

    /**
     * A site that has never printed always prints (its {@code lastLogTimeMs} is 0). The CAS keeps a
     * burst of concurrent violations at the same site to a single line per interval.
     */
    private static boolean shouldLog(Site site, long nowMs, long intervalMs) {
        if (intervalMs <= 0) {
            return true;
        }
        long last = site.lastLogTimeMs.get();
        if (last != 0 && nowMs - last < intervalMs) {
            return false;
        }
        return site.lastLogTimeMs.compareAndSet(last, nowMs);
    }

    /**
     * The innermost frame that is neither the JDK's nor the lock layer's own — i.e. the code that
     * asked for the lock.
     */
    private static String currentCallSite() {
        for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
            String className = frame.getClassName();
            if (className.startsWith("java.") || className.startsWith("jdk.")
                    || className.startsWith(LOCK_PACKAGE)) {
                continue;
            }
            return className + "." + frame.getMethodName() + ":" + frame.getLineNumber();
        }
        return UNKNOWN_SITE;
    }

    // --------------- Introspection, for tests and for diagnosing a noisy deployment ---------------

    /** Exact per-site counts. Logging is throttled; these are not. */
    public static Map<String, Long> violationsBySite() {
        return SITES.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().count.get()));
    }

    public static long totalViolations() {
        return SITES.values().stream().mapToLong(site -> site.count.get()).sum();
    }

    public static void clearViolations() {
        SITES.clear();
    }
}
