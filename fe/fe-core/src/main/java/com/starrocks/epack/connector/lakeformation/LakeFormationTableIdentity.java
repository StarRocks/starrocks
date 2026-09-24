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

import static java.util.Objects.requireNonNull;

/**
 * Table identity for LF authorization. The AWS catalog id and region are part of it: the same
 * (db, table) pair means different objects in different accounts or regions.
 */
public record LakeFormationTableIdentity(
        String catalogName,
        String awsCatalogId,
        String region,
        String dbName,
        String tableName) {

    public LakeFormationTableIdentity {
        requireNonNull(catalogName, "catalogName is null");
        // Deliberately nullable: the catalog property aws.glue.catalog_id is optional, and absent means
        // the caller's own AWS account - which is what the Glue request itself sends when it is null.
        requireNonNull(region, "region is null");
        requireNonNull(dbName, "dbName is null");
        requireNonNull(tableName, "tableName is null");
    }

    @Override
    public String toString() {
        return catalogName + "." + dbName + "." + tableName + " (catalogId=" + describeCatalogId(awsCatalogId)
                + ", region=" + region + ")";
    }

    /** How messages name a catalog id; absent means the caller's own account. */
    static String describeCatalogId(String awsCatalogId) {
        return awsCatalogId == null ? "the caller's own AWS account" : awsCatalogId;
    }
}
