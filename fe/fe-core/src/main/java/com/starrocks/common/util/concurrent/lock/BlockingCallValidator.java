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

import com.starrocks.common.util.concurrent.lock.LockInvariantViolations.Mode;

/**
 * Enforces the second half of the metadata-lock contract:
 *
 * <blockquote>a critical section may mutate state already in hand; it may not go and find
 * state</blockquote>
 *
 * Finding state means waiting on a system the FE does not control, and while that request is
 * outstanding the lock is held. Every waiter on that lock pays the round-trip: transaction publish
 * takes the table lock with a 1000ms {@code tryLock}, so a single second of metastore latency
 * inside a critical section does not merely slow a query down, it fails a load.
 *
 * <h3>Where the check is mounted, and why there</h3>
 *
 * At the last layer the FE owns before a socket is used -- the thrift RPC executor, the HMS client,
 * the JDBC connection pool, the Iceberg REST catalog. Not at the API that eventually leads there.
 *
 * <p>Mounting it on a facade such as {@code MetadataMgr} was tried first and is worse on every
 * axis. That facade is blocking only because something underneath it is, so classifying its methods
 * means asking a person to predict the behaviour of code two layers down -- a judgement that is
 * both error-prone and perishable. It also has to be conservative: {@code getTable} answers from
 * cache almost every time, but "almost" is not "always", so it must be treated as blocking and
 * every cache hit is reported as a violation that never happened. And it is not even complete --
 * {@code IcebergTable.getNativeTable}, {@code PartitionUtil} and the connector caches reach remote
 * systems without passing through it at all.
 *
 * <p>Here none of that applies. A cache hit never reaches this guard, so there is nothing to
 * suppress and no catalog-based narrowing to maintain. Nobody has to predict anything: the guard
 * sits where a socket is about to be used, so it is right by construction. And a path that skips
 * the facade cannot skip the transport.
 *
 * <h3>Where the guards are</h3>
 *
 * Two kinds of point, and a transport usually wants both. <b>Construction</b>: building a
 * connector's client contacts the system it talks to -- a JDBC pool opens its first connection, an
 * Iceberg REST catalog fetches {@code /v1/config}. That happens on the first resolve of a catalog
 * restored from the journal, inside whatever lock that first caller happens to hold, and it is
 * invisible from the per-call door. <b>Per call</b>: the request itself, guarded where the thread
 * waits -- which for an asynchronous send is the {@code Future.get()}, not the send.
 *
 * <p>Guarding both is not redundant. Each marks a real wait, at a different moment, reported
 * against a different caller.
 *
 * <p><b>A construction guard belongs inside the memoization, not at the getter's entry.</b> Most of
 * these clients are built lazily and cached -- {@code IcebergConnector.getNativeCatalog} and
 * {@code PaimonConnector.getPaimonNativeCatalog} both return a field once it is set, and the first
 * is called on every {@code getMetadata}. A guard at the entry would report the cache hit, which is
 * the false positive this whole design exists to avoid.
 *
 * <p>Where the client itself is third-party and has no FE-owned wrapper, the guard goes on the
 * methods that call it -- {@code KuduMetadata} is guarded that way, on its six client-calling
 * methods, because {@code KuduClientBuilder.build()} is not known to connect and guarding it would
 * report a wait that may never happen.
 *
 * <h3>What it does not cover yet</h3>
 *
 * The set of guarded points is meant to grow; each addition shrinks the blind spot rather than
 * changing the design. Known gaps today:
 * <ul>
 *     <li>BRPC to the BEs outside the publish path -- vacuum, compaction, delete, tablet stats --
 *         is awaited at a dozen scattered {@code Future.get()} sites with no shared helper.</li>
 *     <li>ODPS holds a third-party {@code Odps} object and calls it directly; constructing it is
 *         local, so there is no door on the way out.</li>
 *     <li>Delta Lake goes through {@code io.delta.kernel}'s engine, which reaches storage on its
 *         own rather than through the FE's file-system layer.</li>
 *     <li>{@code HdfsFsManager} -- the file-system layer used by broker-less load and
 *         {@code TableFunctionTable} -- caches an {@code HdfsFs} per identity and creates the
 *         underlying file system inside its per-scheme helpers, so a guard has to go in each of
 *         those rather than at {@code getFileSystem}'s entry, where it would report cache hits.</li>
 * </ul>
 *
 * <h3>What it deliberately does not do</h3>
 *
 * It reports; it does not stop the call, except in {@code error} mode, which nothing sets yet.
 * The violation set is known to be non-empty -- that is why the check exists -- and it is not yet
 * known to be bounded, so refusing violations on day one would break paths that work today. Each
 * violation is one line tagged {@code LOCK_INVARIANT_VIOLATION kind=blocking_call_under_lock},
 * carrying the transport, the lock depth and the offending caller's stack, which turns a slow-lock
 * incident from "the metastore was flaky" into a named call site.
 *
 * @see LockTargetValidator the same channel, enforcing who may be locked at all
 */
public class BlockingCallValidator {
    /** Grep token, the report's {@code kind=} field. See {@link LockInvariantViolations#LOG_TAG}. */
    static final String KIND_BLOCKING_CALL_UNDER_LOCK = "blocking_call_under_lock";

    private static final String REMEDY =
            "An FE metadata lock must not be held across a request to an external system: the lock's hold "
                    + "time becomes that system's round-trip time, and every waiter pays it -- transaction "
                    + "publish takes the table lock with a 1000ms tryLock, so one slow call inside a critical "
                    + "section fails a load. Resolve the metadata before taking the lock. If the call site has a "
                    + "legitimate answer for 'not known yet', read a cache and degrade on a miss; if it does not, "
                    + "the call has to move out of the critical section. A test that holds a lock across such a "
                    + "call on purpose should call LockTestUtils.disableBlockingCallValidation() and say why";

    private BlockingCallValidator() {
    }

    /**
     * Report if the current thread holds a metadata lock while this transport is about to be used.
     *
     * @param transport short tag for the system being contacted, e.g. {@code "hive-metastore"}. It
     *                  is the report's most useful field after the call site, because it says which
     *                  external system's latency the lock is now bound to, and it is what makes
     *                  "lock-held time grouped by transport" an aggregatable metric.
     */
    public static void validateNotUnderLock(String transport) {
        Mode mode = LockInvariantViolations.currentBlockingCallMode();
        if (mode == Mode.OFF || !LockHoldDepth.isUnderLock()) {
            return;
        }

        String callSite = callerOfTheGuard();
        String detail = "about to contact " + transport + " while holding " + LockHoldDepth.current()
                + " FE metadata lock(s)";
        LockInvariantViolations.reportAtSite(KIND_BLOCKING_CALL_UNDER_LOCK, detail, REMEDY, mode, callSite);
    }

    /**
     * The frame that asked for the remote call, skipping the transport's own frames.
     *
     * <p>Naming the transport would be useless: it is the same frame every time, and it is never
     * the code that has to change. What a report has to point at is the FE code that decided to go
     * remote while holding a lock -- {@code HiveMetastore.getTable}, not
     * {@code HiveMetaClient.getClient}.
     *
     * <p>The transport is identified as "whatever class called this method", so nothing has to be
     * configured, named in a constant, or kept in step with a rename. Its internal layering is
     * skipped for free -- {@code ThriftRPCRequestExecutor.call} delegating to another of its own
     * overloads, a lambda inside it, or an inner class of the HMS client are all the same top-level
     * class and all get skipped.
     *
     * <p>Walks the stack, so it runs only once a violation has already been established.
     */
    private static String callerOfTheGuard() {
        String transportClass = null;
        for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
            String className = frame.getClassName();
            if (className.startsWith("java.") || className.startsWith("jdk.")
                    || className.startsWith(LockInvariantViolations.LOCK_PACKAGE)) {
                continue;
            }
            String topLevel = topLevelClass(className);
            if (transportClass == null) {
                transportClass = topLevel;
                continue;
            }
            if (topLevel.equals(transportClass)) {
                continue;
            }
            return className + "." + frame.getMethodName() + ":" + frame.getLineNumber();
        }
        return LockInvariantViolations.UNKNOWN_SITE;
    }

    /** {@code Foo$Bar$1} and {@code Foo} are the same layer for the purpose of skipping. */
    private static String topLevelClass(String className) {
        int nested = className.indexOf('$');
        return nested < 0 ? className : className.substring(0, nested);
    }
}
