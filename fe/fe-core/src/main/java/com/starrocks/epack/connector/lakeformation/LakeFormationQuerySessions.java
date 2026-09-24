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

import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;

import java.io.Closeable;
import java.util.UUID;

/**
 * Builds the QuerySessionContext that every Lake Formation call carries.
 *
 * The query id is what ties a CloudTrail event back to a StarRocks statement, so it is never invented. A
 * random id would let the code run while making the audit trail quietly untrue: the id in CloudTrail would
 * match nothing on this side.
 *
 * The id is formatted with UUID.toString(), the same form the FE audit event uses, because an id that is
 * formatted differently on the two sides cannot be joined even though both are present.
 */
public final class LakeFormationQuerySessions {

    private LakeFormationQuerySessions() {
    }

    /**
     * @param context the context the caller was given, which for several metadata entry points is a bare
     *                new ConnectContext() with no query id at all
     */
    public static LakeFormationQuerySession of(ConnectContext context) {
        // The thread local context comes first, not the one passed along the call chain. MetadataMgr picks
        // the ConnectorMetadata instance - and therefore which memo this resolution lands in - from the
        // thread local one alone, so taking the query id from anywhere else could leave the memo belonging
        // to one query while CloudTrail names another, and that mismatch is invisible from either side.
        ConnectContext source = ConnectContext.get();
        UUID queryId = source == null ? null : source.getQueryId();

        if (queryId == null) {
            // The schema readers take this branch: Thrift handlers set the calling statement's identity on the context
            // they pass down and never publish it thread local. Publishing one would attribute Lake Formation calls to
            // another statement, and the worker thread reuses its thread local across requests.
            source = context;
            queryId = source == null ? null : source.getQueryId();
        }

        if (queryId == null) {
            // What is left cannot be attributed to any statement, and stays refused: the query id is what makes a call auditable.
            throw new LakeFormationTableAccessException(
                    "This code path reached Lake Formation without a query context, so no auditable query id"
                            + " is available. Metadata-only entry points are not supported on a Lake Formation"
                            + " catalog in this version.");
        }

        return new LakeFormationQuerySession(
                queryId.toString(),
                source.getStartTimeInstant(),
                String.valueOf(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterId()));
    }

    /** The thread a manual ANALYZE hands its collection to; it lists partitions before its statement is planned. */
    static boolean isStatisticsWorker() {
        ConnectContext context = ConnectContext.get();
        return context != null && context.isStatisticsJob();
    }

    /** Set while a materialized view refresh this catalog is part of runs, on that refresh's own thread. */
    private static final ThreadLocal<Boolean> MATERIALIZED_VIEW_REFRESH = new ThreadLocal<>();

    /**
     * Marks this thread as a materialized view refresh while the handle is open: after its DML it reads the base
     * partitions again, outside any attempt.
     */
    static Closeable enterMaterializedViewRefresh() {
        MATERIALIZED_VIEW_REFRESH.set(Boolean.TRUE);
        return MATERIALIZED_VIEW_REFRESH::remove;
    }

    static boolean isMaterializedViewRefresh() {
        return Boolean.TRUE.equals(MATERIALIZED_VIEW_REFRESH.get());
    }

    /**
     * Internal steps a user's statement set in motion - the ANALYZE worker, an MV refresh - may authorize for
     * themselves.
     */
    static boolean mayAuthorizeWithoutAnAttempt() {
        return isStatisticsWorker() || isMaterializedViewRefresh();
    }
}
