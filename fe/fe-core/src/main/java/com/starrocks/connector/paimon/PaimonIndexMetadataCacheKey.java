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

package com.starrocks.connector.paimon;

import java.util.Objects;

/** Identifies immutable Paimon index metadata at one table snapshot. */
final class PaimonIndexMetadataCacheKey {
    private final String catalogName;
    private final String databaseName;
    private final String tableName;
    private final String tableLocation;
    private final String branchName;
    private final long snapshotId;
    private final long snapshotTimeMillis;
    private final String indexManifest;

    PaimonIndexMetadataCacheKey(
            String catalogName, String databaseName, String tableName, String tableLocation,
            String branchName, long snapshotId, long snapshotTimeMillis, String indexManifest) {
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableLocation = tableLocation;
        this.branchName = branchName;
        this.snapshotId = snapshotId;
        this.snapshotTimeMillis = snapshotTimeMillis;
        this.indexManifest = indexManifest;
    }

    boolean belongsTo(String catalogName, String databaseName, String tableName) {
        return Objects.equals(this.catalogName, catalogName)
                && Objects.equals(this.databaseName, databaseName)
                && Objects.equals(this.tableName, tableName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PaimonIndexMetadataCacheKey)) {
            return false;
        }
        PaimonIndexMetadataCacheKey that = (PaimonIndexMetadataCacheKey) o;
        return snapshotId == that.snapshotId && snapshotTimeMillis == that.snapshotTimeMillis
                && Objects.equals(catalogName, that.catalogName)
                && Objects.equals(databaseName, that.databaseName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(tableLocation, that.tableLocation)
                && Objects.equals(branchName, that.branchName)
                && Objects.equals(indexManifest, that.indexManifest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, databaseName, tableName, tableLocation, branchName, snapshotId,
                snapshotTimeMillis, indexManifest);
    }
}
