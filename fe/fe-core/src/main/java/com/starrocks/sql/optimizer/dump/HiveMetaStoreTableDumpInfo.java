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


package com.starrocks.sql.optimizer.dump;

import java.util.List;

public interface HiveMetaStoreTableDumpInfo {
    default String getType() {
        return "";
    }

    default void setPartitionNames(List<String> partitionNames) {
    }

    default List<String> getPartitionNames() {
        return null;
    }

    default void setPartColumnNames(List<String> partColumnNames) {
    }

    default List<String> getPartColumnNames() {
        return null;
    }

    default void setDataColumnNames(List<String> dataColumnNames) {
    }

    default List<String> getDataColumnNames() {
        return null;
    }

    default double getScanRowCount() {
        return 1;
    }

    default void setScanRowCount(double rowCount) {
    }
}
