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

import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * The query identity Lake Formation records against every call it audits.
 *
 * Passed in by the caller rather than read from a thread local: the gateway is used from planning
 * threads and from background tasks, and a gateway that reached for an ambient ConnectContext would
 * quietly attribute one query's access to another.
 */
public record LakeFormationQuerySession(String queryId, Instant queryStartTime, String clusterId) {

    public LakeFormationQuerySession {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(queryStartTime, "queryStartTime is null");
        requireNonNull(clusterId, "clusterId is null");
    }
}
