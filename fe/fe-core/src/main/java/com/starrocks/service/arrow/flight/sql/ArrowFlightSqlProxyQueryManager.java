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

package com.starrocks.service.arrow.flight.sql;

import com.google.common.collect.Maps;
import com.starrocks.thrift.TUniqueId;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The manager for queries that are forwarded from the follower to the leader is stored in the manager
 * while the forward-to-leader request is in progress.
 */
public class ArrowFlightSqlProxyQueryManager {
    private final Map<TUniqueId, QueryInfo> queryIdToInfo = Maps.newConcurrentMap();

    private static final ArrowFlightSqlProxyQueryManager INSTANCE = new ArrowFlightSqlProxyQueryManager();

    public static ArrowFlightSqlProxyQueryManager getInstance() {
        return INSTANCE;
    }

    public void register(TUniqueId queryId, CompletableFuture<ArrowFlightSqlResultDescriptor> backendResultFuture) {
        QueryInfo queryInfo = new QueryInfo(backendResultFuture);
        queryIdToInfo.put(queryId, queryInfo);
    }

    public void deregister(TUniqueId queryId) {
        queryIdToInfo.remove(queryId);
    }

    public void notifyBackendResult(TUniqueId queryId, ArrowFlightSqlResultDescriptor backendResult) {
        QueryInfo queryInfo = queryIdToInfo.get(queryId);
        if (queryInfo != null) {
            queryInfo.backendResultFuture.complete(backendResult);
        }
    }

    public static class QueryInfo {
        private final CompletableFuture<ArrowFlightSqlResultDescriptor> backendResultFuture;

        public QueryInfo(CompletableFuture<ArrowFlightSqlResultDescriptor> backendResultFuture) {
            this.backendResultFuture = backendResultFuture;
        }
    }
}
