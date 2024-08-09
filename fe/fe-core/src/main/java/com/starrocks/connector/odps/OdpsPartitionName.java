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

package com.starrocks.connector.odps;

import java.util.Objects;

public class OdpsPartitionName {
    public static final String ALL_PARTITION = "";
    private final OdpsTableName odpsTableName;
    private final String partitionName;

    public OdpsPartitionName(OdpsTableName odpsTableName, String partitionName) {
        this.odpsTableName = odpsTableName;
        this.partitionName = partitionName;
    }

    public static OdpsPartitionName of(OdpsTableName odpsTableName, String partitionName) {
        return new OdpsPartitionName(odpsTableName, partitionName);
    }

    public OdpsTableName getOdpsTableName() {
        return odpsTableName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OdpsPartitionName that = (OdpsPartitionName) o;
        return Objects.equals(odpsTableName, that.odpsTableName) &&
                Objects.equals(partitionName, that.partitionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(odpsTableName, partitionName);
    }
}
