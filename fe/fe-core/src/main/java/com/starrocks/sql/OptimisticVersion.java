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

package com.starrocks.sql;

import com.starrocks.catalog.OlapTable;

/**
 * Generate a monotonic version for optimistic lock.
 * NOTE: currently we use the nano time, which is usually precise enough for the schema-change and version update
 * operations. Previously we use the millisecond time, which is not safe enough.
 * TODO: refactor related code to here
 */
public class OptimisticVersion {

    /**
     * Generate a version
     */
    public static long generate() {
        return System.nanoTime();
    }

    /**
     * Validate the candidate version
     */
    public static boolean validateTableUpdate(OlapTable olapTable, long candidateVersion) {
        long schemaUpdate = olapTable.lastSchemaUpdateTime.get();
        long dataUpdateStart = olapTable.lastVersionUpdateStartTime.get();
        long dataUpdateEnd = olapTable.lastVersionUpdateEndTime.get();

        return (schemaUpdate < candidateVersion) &&
                (dataUpdateEnd >= dataUpdateStart && dataUpdateEnd < candidateVersion);
    }
}
