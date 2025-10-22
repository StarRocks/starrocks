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

<<<<<<< HEAD
=======
import com.starrocks.connector.GetRemoteFilesParams;
>>>>>>> a5ff91dfe2 ([BugFix] fix cache key of iceberg split tasks (#64272))
import com.starrocks.connector.PredicateSearchKey;

import java.util.Objects;

public class IcebergRemoteFileInfoSourceKey extends PredicateSearchKey {
    private final long morId;
    private final IcebergMORParams icebergMORParams;

    private IcebergRemoteFileInfoSourceKey(String databaseName,
                                           String tableName,
<<<<<<< HEAD
                                           long snapshotId,
                                           ScalarOperator predicate,
                                           long morId,
                                           IcebergMORParams icebergMORParams) {
        super(databaseName, tableName, snapshotId, predicate);
=======
                                           GetRemoteFilesParams params,
                                           long morId,
                                           IcebergMORParams icebergMORParams) {
        super(databaseName, tableName, params);
>>>>>>> a5ff91dfe2 ([BugFix] fix cache key of iceberg split tasks (#64272))
        this.morId = morId;
        this.icebergMORParams = icebergMORParams;
    }

    public static IcebergRemoteFileInfoSourceKey of(String databaseName,
                                                    String tableName,
<<<<<<< HEAD
                                                    long snapshotId,
                                                    ScalarOperator predicate,
                                                    long morId,
                                                    IcebergMORParams icebergMORParams) {
        predicate = predicate == null ? ConstantOperator.TRUE : predicate;
        return new IcebergRemoteFileInfoSourceKey(databaseName, tableName, snapshotId, predicate, morId, icebergMORParams);
=======
                                                    GetRemoteFilesParams params,
                                                    long morId,
                                                    IcebergMORParams icebergMORParams) {
        return new IcebergRemoteFileInfoSourceKey(databaseName, tableName, params, morId, icebergMORParams);
>>>>>>> a5ff91dfe2 ([BugFix] fix cache key of iceberg split tasks (#64272))
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
