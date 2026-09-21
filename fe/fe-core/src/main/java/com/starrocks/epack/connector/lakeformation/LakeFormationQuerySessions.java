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
            // No ambient query: the caller's own context is the best identity available, and the metadata
            // instance is a throwaway one anyway.
            source = context;
            queryId = source == null ? null : source.getQueryId();
        }

        if (queryId == null) {
            // Deliberately not a NullPointerException: LakeFormationQuerySession requires a non null query
            // id, and an NPE here would be caught by the resolution memo and remembered as an authorization
            // failure for the table rather than reported as an unsupported entry point.
            //
            // Some metadata-only entry points legitimately have no query id - FrontendServiceImpl's Thrift
            // handlers build a bare ConnectContext and never publish it as thread local, which is how
            // SHOW COLUMNS and information_schema reach the connector. Serving those needs a session that
            // is explicitly marked as a metadata read rather than one that impersonates a query, and the
            // encoding of such a marker has to be validated against Glue before it ships. Until then this
            // path stays closed.
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
}
