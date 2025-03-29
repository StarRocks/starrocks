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

import com.starrocks.connector.PredicateSearchKey;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Objects;

public class IcebergRemoteFileInfoSourceKey extends PredicateSearchKey {
    private final long morId;
    private final IcebergMORParams icebergMORParams;

    private IcebergRemoteFileInfoSourceKey(String databaseName,
                                           String tableName,
                                           long snapshotId,
                                           ScalarOperator predicate,
                                           long morId,
                                           IcebergMORParams icebergMORParams) {
        super(databaseName, tableName, snapshotId, predicate);
        this.morId = morId;
        this.icebergMORParams = icebergMORParams;
    }

    public static IcebergRemoteFileInfoSourceKey of(String databaseName,
                                                    String tableName,
                                                    long snapshotId,
                                                    ScalarOperator predicate,
                                                    long morId,
                                                    IcebergMORParams icebergMORParams) {
        predicate = predicate == null ? ConstantOperator.TRUE : predicate;
        return new IcebergRemoteFileInfoSourceKey(databaseName, tableName, snapshotId, predicate, morId, icebergMORParams);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        IcebergRemoteFileInfoSourceKey that = (IcebergRemoteFileInfoSourceKey) o;
        return morId == that.morId && Objects.equals(icebergMORParams, that.icebergMORParams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), morId, icebergMORParams);
    }
}
