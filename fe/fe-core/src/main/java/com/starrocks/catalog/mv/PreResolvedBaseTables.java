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

package com.starrocks.catalog.mv;

import com.google.common.collect.Maps;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * External base tables of an MV, resolved before a metadata lock is taken, for MV code that runs under the
 * lock and resolves them again.
 *
 * <p>MV code resolves a base table through {@code MetadataMgr#getTable(ConnectContext, BaseTableInfo)} --
 * directly or via {@code MvUtils#getTable / getTableWithIdentifier / getTableChecked} -- and for an external
 * base table that is a connector call. Much of that code mutates the MV and so has to run under the MV's lock
 * (the relationship rebuild of ALTER ... ACTIVE, the retention-condition analysis of ALTER ... SET), but the
 * lookups need not. The caller resolves the tables first, then runs the locked section inside {@link #enter()};
 * while the scope is open, {@code MetadataMgr} answers those lookups on this thread from here.
 *
 * <p><b>Why no "still current" check.</b> No FE lock covers an external table, so holding a metadata lock
 * never made its metadata stable, and resolving it again under the lock would not either -- the second answer
 * is as current as the first. Whatever the lock does cover (such as the MV's definition) is the caller's to
 * check.
 *
 * <p>A lookup that failed is recorded with its exception and rethrown on every read, so the locked code sees
 * the outcome it would have seen resolving the table itself, at the same point. Internal base tables are never
 * recorded: they are an in-memory lookup and the caller wants the current object.
 */
public final class PreResolvedBaseTables {
    private record Resolution(Table table, RuntimeException failure) {
        Optional<Table> get() {
            if (failure != null) {
                throw failure;
            }
            return Optional.ofNullable(table);
        }
    }

    private static final ThreadLocal<PreResolvedBaseTables> CURRENT = new ThreadLocal<>();

    private final Map<BaseTableInfo, Resolution> byInfo = Maps.newHashMap();

    private PreResolvedBaseTables() {
    }

    /**
     * Resolves the way {@code MvUtils#getTable} does -- on the thread's context, reading the iceberg cache
     * only -- because that is the lookup the locked code makes and whose answer this stands in for.
     */
    public static PreResolvedBaseTables resolve(Collection<BaseTableInfo> baseTableInfos) {
        PreResolvedBaseTables result = new PreResolvedBaseTables();
        try (ConnectContext.ContextScope scope = ConnectContext.enterOnlyReadIcebergCacheScope(ConnectContext.get())) {
            for (BaseTableInfo baseTableInfo : baseTableInfos) {
                if (baseTableInfo == null || baseTableInfo.isInternalCatalog()
                        || result.byInfo.containsKey(baseTableInfo)) {
                    continue;
                }
                Resolution resolution;
                try {
                    resolution = new Resolution(GlobalStateMgr.getCurrentState().getMetadataMgr()
                            .getTable(scope.getContext(), baseTableInfo).orElse(null), null);
                } catch (RuntimeException e) {
                    resolution = new Resolution(null, e);
                }
                result.byInfo.put(baseTableInfo, resolution);
            }
        }
        return result;
    }

    /**
     * Makes lookups on the current thread use these tables until the returned scope is closed. Scopes nest:
     * closing one restores the one it replaced.
     */
    public Scope enter() {
        PreResolvedBaseTables previous = CURRENT.get();
        CURRENT.set(this);
        return () -> {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        };
    }

    /**
     * The lookup hook for {@code MetadataMgr}: the table pre-resolved for this base table in the scope open on
     * this thread, or else whatever {@code resolver} returns.
     *
     * @throws RuntimeException the exception the pre-resolve failed with
     */
    public static Optional<Table> getOrResolve(BaseTableInfo baseTableInfo, Supplier<Optional<Table>> resolver) {
        PreResolvedBaseTables current = CURRENT.get();
        Resolution resolution = current == null ? null : current.byInfo.get(baseTableInfo);
        return resolution != null ? resolution.get() : resolver.get();
    }

    public interface Scope extends AutoCloseable {
        @Override
        void close();
    }
}
