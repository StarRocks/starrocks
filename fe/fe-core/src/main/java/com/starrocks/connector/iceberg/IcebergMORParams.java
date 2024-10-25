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

import java.util.List;
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

    public static IcebergMORParams EMPTY = new IcebergMORParams(ScanTaskType.EMPTY, List.of());

    public static IcebergMORParams DATA_FILE_WITHOUT_EQ_DELETE = new IcebergMORParams(
            ScanTaskType.DATA_FILE_WITHOUT_EQ_DELETE, List.of());

    public static IcebergMORParams DATA_FILE_WITH_EQ_DELETE = new IcebergMORParams(
            ScanTaskType.DATA_FILE_WITH_EQ_DELETE, List.of());

    private final ScanTaskType scanTaskType;
    private final List<Integer> equalityIds;

    public IcebergMORParams(ScanTaskType scanTaskType, List<Integer> equalityIds) {
        this.scanTaskType = scanTaskType;
        this.equalityIds = equalityIds;
    }

    public static IcebergMORParams of(ScanTaskType scanTaskType, List<Integer> equalityIds) {
        return new IcebergMORParams(scanTaskType, equalityIds);
    }

    public ScanTaskType getScanTaskType() {
        return scanTaskType;
    }

    public List<Integer> getEqualityIds() {
        return equalityIds;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", IcebergMORParams.class.getSimpleName() + "[", "]")
                .add("scanTaskType=" + scanTaskType)
                .add("equalityIds=" + equalityIds)
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

        return scanTaskType == that.scanTaskType && Objects.equals(equalityIds, that.equalityIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scanTaskType, equalityIds);
    }
}
