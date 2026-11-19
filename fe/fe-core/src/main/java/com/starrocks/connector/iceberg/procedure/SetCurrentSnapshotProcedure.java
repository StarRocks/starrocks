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

package com.starrocks.connector.iceberg.procedure;

import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.IntegerType;

import java.util.List;
import java.util.Map;

/**
 * Sets the table's current snapshot to the given snapshot. Unlike rollback_to_snapshot, the target does not
 * have to be an ancestor of the current snapshot, so it can also move the table forward again after a rollback.
 */
public class SetCurrentSnapshotProcedure extends IcebergTableProcedure {
    private static final String PROCEDURE_NAME = "set_current_snapshot";

    public static final String SNAPSHOT_ID = "snapshot_id";

    private static final SetCurrentSnapshotProcedure INSTANCE = new SetCurrentSnapshotProcedure();

    public static SetCurrentSnapshotProcedure getInstance() {
        return INSTANCE;
    }

    private SetCurrentSnapshotProcedure() {
        super(
                PROCEDURE_NAME,
                List.of(
                        new NamedArgument(SNAPSHOT_ID, IntegerType.BIGINT, true)
                ),
                IcebergTableOperation.SET_CURRENT_SNAPSHOT
        );
    }

    @Override
    public ShowResultSet execute(IcebergTableProcedureContext context, Map<String, ConstantOperator> args) {
        if (args.size() != 1 || !args.containsKey(SNAPSHOT_ID)) {
            throw new StarRocksConnectorException("invalid args. set current snapshot must contain `%s`", SNAPSHOT_ID);
        }

        long snapshotId = args.get(SNAPSHOT_ID).castTo(IntegerType.BIGINT)
                .map(ConstantOperator::getBigint)
                .orElseThrow(() ->
                        new StarRocksConnectorException("invalid argument type for %s, expected BIGINT", SNAPSHOT_ID));

        context.transaction().manageSnapshots().setCurrentSnapshot(snapshotId).commit();
        return null;
    }
}
