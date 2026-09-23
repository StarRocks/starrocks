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

package com.starrocks.epack.connector.lakeformation;

import com.starrocks.catalog.UserIdentity;
import com.starrocks.connector.TableLoadPurpose;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TUniqueId;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Everything Lake Formation authorized during one planning attempt, and nothing that outlives it.
 *
 * <p>Not the metadata memo: MetadataMgr's cache is bounded by size as well as time, so a busy cluster can
 * evict a query that is still planning. A credential whose lifetime is an eviction policy disappears at an
 * unpredictable moment.
 *
 * <p>Opened and closed by the planner as a resource, like StatisticsLoadBudget.Scope next to it. Nested
 * planning - an INSERT planning its own query, an MV refresh - reuses the enclosing scope, so one statement
 * is one attempt; only the outermost opener closes it.
 */
public final class LakeFormationQueryScope {

    private static final ThreadLocal<LakeFormationQueryScope> CURRENT = new ThreadLocal<>();

    private final String attemptId;
    private final UserIdentity principal;
    private final TUniqueId executionId;
    private final Map<ScopeKey, LakeFormationTableResolution> resolutions = new ConcurrentHashMap<>();

    private LakeFormationQueryScope(UserIdentity principal, TUniqueId executionId) {
        this.attemptId = UUID.randomUUID().toString();
        this.principal = principal;
        this.executionId = executionId;
    }

    /**
     * Whether a scope already open on this thread is this same statement, which is what makes joining it
     * nesting rather than inheriting someone else's answers.
     *
     * The user alone would not be enough: one connection runs its statements one after another on one
     * thread under one user, so a scope an earlier statement failed to close would pass for an enclosing
     * attempt and hand over everything it authorized, including grants that have since been revoked.
     * Absent on both sides counts as equal - the internal paths that plan with neither a user nor an
     * execution id have nothing to tell apart.
     */
    private boolean isSameStatementAs(ConnectContext context) {
        UserIdentity user = context == null ? null : context.getCurrentUserIdentity();
        TUniqueId execution = context == null ? null : context.getExecutionId();
        return Objects.equals(principal, user) && Objects.equals(executionId, execution);
    }

    /**
     * Opens a scope for this planning attempt, or joins the one already open on this thread.
     *
     * Cheap enough to call unconditionally: a catalog that does not use Lake Formation simply never looks
     * the scope up.
     */
    public static Scope open(ConnectContext context) {
        return new Scope(context);
    }

    /**
     * The scope of the planning attempt running on this thread, if there is one.
     *
     * Absent means "not planning", which every caller treats as a refusal rather than as a reason to
     * authorize something on the spot. Background work that reaches a Lake Formation table without going
     * through the planner lands here, and refusing it is the intended answer for this version.
     */
    public static Optional<LakeFormationQueryScope> current() {
        return Optional.ofNullable(CURRENT.get());
    }

    public String attemptId() {
        return attemptId;
    }

    public UserIdentity principal() {
        return principal;
    }

    /**
     * Resolves a table once per attempt, or returns what a previous call decided - including a failure.
     *
     * Failures are memoized on purpose: a table that Lake Formation refused must keep being refused for the
     * rest of the attempt. Letting a second call try again would mean one statement could see two different
     * authorization answers, and the second one would win simply by being later.
     */
    LakeFormationTableResolution resolve(LakeFormationTableIdentity identity, TableLoadPurpose purpose,
                                         Function<ScopeKey, LakeFormationTableResolution> resolver) {
        return resolutions.computeIfAbsent(ScopeKey.of(identity, purpose), resolver);
    }

    LakeFormationTableResolution find(LakeFormationTableIdentity identity, TableLoadPurpose purpose) {
        return resolutions.get(ScopeKey.of(identity, purpose));
    }


    /**
     * The handle the planner holds. Installs a scope on the way in and puts back whatever it displaced on
     * the way out, unless this call is nested inside the same statement - then it joins and ends nothing.
     */
    public static final class Scope implements AutoCloseable {
        private final LakeFormationQueryScope scope;
        private final LakeFormationQueryScope displaced;

        private Scope(ConnectContext context) {
            LakeFormationQueryScope open = CURRENT.get();
            if (open != null && open.isSameStatementAs(context)) {
                this.scope = null;
                this.displaced = null;
                return;
            }
            // Anything else on this thread belongs to someone else - an outer planner call that rebound a
            // different context, or a scope a caller failed to close. It is installed over rather than
            // joined, and restored rather than removed, so neither statement reads the other's answers and
            // an outer attempt still has its scope after an inner one finishes.
            this.displaced = open;
            this.scope = new LakeFormationQueryScope(
                    context == null ? null : context.getCurrentUserIdentity(),
                    context == null ? null : context.getExecutionId());
            CURRENT.set(scope);
        }

        @Override
        public void close() {
            if (scope == null) {
                return;
            }
            if (displaced == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(displaced);
            }
        }
    }

    /**
     * The key an attempt's tables are stored under.
     *
     * <p>Database and table names are lower-cased, because Hive and Glue treat them case insensitively:
     * without that, {@code FROM DB.T JOIN db.t} would authorize twice, vend twice and build two Table
     * objects for one table. LakeFormationTableIdentity is a record, so its equality is case sensitive and
     * cannot do this on its own.
     *
     * <p>The purpose is part of the key because a metadata-only authorization is not a data-access one. The
     * design forbids upgrading the first into the second in place, so they have to be able to coexist.
     */
    record ScopeKey(LakeFormationTableIdentity identity, TableLoadPurpose purpose) {

        static ScopeKey of(LakeFormationTableIdentity identity, TableLoadPurpose purpose) {
            return new ScopeKey(new LakeFormationTableIdentity(
                    identity.catalogName(),
                    identity.awsCatalogId(),
                    identity.region(),
                    lower(identity.dbName()),
                    lower(identity.tableName())), purpose);
        }

        private static String lower(String value) {
            return value == null ? null : value.toLowerCase(Locale.ROOT);
        }
    }
}
