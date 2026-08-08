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

package com.starrocks.connector.iceberg;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

// Mainly used for table with iceberg equality delete files.
// IcebergEqualityDeleteRewriteRule will split one table scan node to multiple scan nodes.
// IcebergMORParams is used to mark the type of split iceberg scan node.
public class IcebergMORParams {
    public enum ScanTaskType {
        // eligible iceberg scan task: FileScanTask = DataFile.
        // only scan data file in the iceberg file scan task
        DATA_FILE_WITHOUT_EQ_DELETE,

        // eligible iceberg scan task: FileScanTask = DataFile + Equality Delete Files + Optional(Position Delete Files)
        // scan data file and position delete files in the iceberg file scan task
        DATA_FILE_WITH_EQ_DELETE,

        // eligible iceberg scan task: FileScanTask = DataFile + Equality Delete Files + Optional(Position Delete Files)
        // only scan equality delete files in the iceberg file scan task
        EQ_DELETE,

        // default value. The table does not have any iceberg identifier columns. scan won't be overwritten.
        EMPTY
    }

    // Scopes an EQ_DELETE scan to the equality-delete files of a single partition-scoping kind,
    // mirroring Iceberg's DeleteFileIndex routing: GLOBAL = files written under an unpartitioned
    // spec (apply to every data file); PARTITIONED = files written under a partitioned spec (apply
    // only to data files of the same (specId, partition)). Only set for EQ_DELETE params; null
    // otherwise. Every EQ_DELETE leg carries a concrete scope - there is no catch-all - so the
    // remote-source trigger can key queues by an exact (equalityIds, scope) computed per delete file.
    public enum EqDeleteScope {
        GLOBAL,
        PARTITIONED
    }

    public static IcebergMORParams EMPTY = new IcebergMORParams(ScanTaskType.EMPTY, List.of());

    public static IcebergMORParams DATA_FILE_WITHOUT_EQ_DELETE = new IcebergMORParams(
            ScanTaskType.DATA_FILE_WITHOUT_EQ_DELETE, List.of());

    public static IcebergMORParams DATA_FILE_WITH_EQ_DELETE = new IcebergMORParams(
            ScanTaskType.DATA_FILE_WITH_EQ_DELETE, List.of());

    private final ScanTaskType scanTaskType;
    private final List<Integer> equalityIds;
    // Only meaningful for EQ_DELETE; null for all other scan task types.
    private final EqDeleteScope eqDeleteScope;

    public IcebergMORParams(ScanTaskType scanTaskType, List<Integer> equalityIds) {
        this(scanTaskType, equalityIds, null);
    }

    public IcebergMORParams(ScanTaskType scanTaskType, List<Integer> equalityIds, EqDeleteScope eqDeleteScope) {
        this.scanTaskType = scanTaskType;
        this.equalityIds = equalityIds;
        this.eqDeleteScope = eqDeleteScope;
    }

    public static IcebergMORParams of(ScanTaskType scanTaskType, List<Integer> equalityIds) {
        return new IcebergMORParams(scanTaskType, equalityIds);
    }

    public static IcebergMORParams ofEqDelete(List<Integer> equalityIds, EqDeleteScope eqDeleteScope) {
        return new IcebergMORParams(ScanTaskType.EQ_DELETE, equalityIds, eqDeleteScope);
    }

    public ScanTaskType getScanTaskType() {
        return scanTaskType;
    }

    public List<Integer> getEqualityIds() {
        return equalityIds;
    }

    public EqDeleteScope getEqDeleteScope() {
        return eqDeleteScope;
    }

    // The partition-scoping kind of an equality-delete file, per Iceberg's DeleteFileIndex routing:
    // files under an unpartitioned spec are GLOBAL (apply to all data files), otherwise PARTITIONED
    // (apply only within their own (specId, partition)).
    public static EqDeleteScope scopeOf(DeleteFile file, Map<Integer, PartitionSpec> specs) {
        return specs.get(file.specId()).isUnpartitioned() ? EqDeleteScope.GLOBAL : EqDeleteScope.PARTITIONED;
    }

    // Whether this EQ_DELETE leg should read the given equality-delete file: same equality-id set and
    // same partition scope. Used both when the remote-source trigger keys queues and when the
    // scan-range builder emits per-file ranges, so the two stay consistent.
    public boolean matchesEqDeleteFile(DeleteFile file, Map<Integer, PartitionSpec> specs) {
        return scanTaskType == ScanTaskType.EQ_DELETE
                && Objects.equals(equalityIds, file.equalityFieldIds())
                && eqDeleteScope == scopeOf(file, specs);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", IcebergMORParams.class.getSimpleName() + "[", "]")
                .add("scanTaskType=" + scanTaskType)
                .add("equalityIds=" + equalityIds)
                .add("eqDeleteScope=" + eqDeleteScope)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IcebergMORParams that = (IcebergMORParams) o;

        return scanTaskType == that.scanTaskType && Objects.equals(equalityIds, that.equalityIds) &&
                eqDeleteScope == that.eqDeleteScope;
    }

    @Override
    public int hashCode() {
        return Objects.hash(scanTaskType, equalityIds, eqDeleteScope);
    }
}
