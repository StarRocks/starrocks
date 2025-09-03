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

import com.starrocks.catalog.Type;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.util.List;
import java.util.Map;

public class CherryPickSnapshotProcedure extends IcebergTableProcedure {
    private static final String PROCEDURE_NAME = "cherrypick_snapshot";

    private static final String SNAPSHOT_ID = "snapshot_id";

    private static final CherryPickSnapshotProcedure INSTANCE = new CherryPickSnapshotProcedure();

    public static CherryPickSnapshotProcedure getInstance() {
        return INSTANCE;
    }

    private CherryPickSnapshotProcedure() {
        super(
                PROCEDURE_NAME,
                List.of(
                        new NamedArgument(SNAPSHOT_ID, Type.BIGINT, true)
                ),
                IcebergTableOperation.CHERRYPICK_SNAPSHOT
        );
    }

    @Override
    public void execute(IcebergTableProcedureContext context, Map<String, ConstantOperator> args) {
        if (args.size() != 1) {
            throw new StarRocksConnectorException("invalid args. cherrypick snapshot must contain `snapshot id`");
        }
        if (!args.containsKey(SNAPSHOT_ID)) {
            throw new StarRocksConnectorException("missing required argument: %s", SNAPSHOT_ID);
        }
        ConstantOperator snapshotIdArg = args.get(SNAPSHOT_ID);
        long snapshotId = snapshotIdArg.castTo(Type.BIGINT)
                .map(ConstantOperator::getBigint)
                .orElseThrow(() -> new StarRocksConnectorException("invalid argument type for %s, expected BIGINT", SNAPSHOT_ID));

        context.transaction().manageSnapshots().cherrypick(snapshotId).commit();
    }
}