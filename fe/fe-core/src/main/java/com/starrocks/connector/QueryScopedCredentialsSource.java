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

package com.starrocks.connector;

import java.util.Optional;

/**
 * A table whose credentials come from the query that resolved it rather than from its catalog.
 *
 * Implementations look them up rather than store them - a Table outlives its query on several cached
 * paths. Empty means stale, and callers refuse rather than fall back.
 */
public interface QueryScopedCredentialsSource {

    /** Empty when this table was not resolved by the query being planned now - a cached table included. */
    Optional<QueryScopedCredentials> currentQueryScopedCredentials();
}
