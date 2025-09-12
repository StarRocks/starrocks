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
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.apache.iceberg.ExpireSnapshots;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class ExpireSnapshotsProcedure extends IcebergTableProcedure {
    private static final String PROCEDURE_NAME = "expire_snapshots";

    private static final String OLDER_THAN = "older_than";

    private static final ExpireSnapshotsProcedure INSTANCE = new ExpireSnapshotsProcedure();

    public static ExpireSnapshotsProcedure getInstance() {
        return INSTANCE;
    }

    private ExpireSnapshotsProcedure() {
        super(
                PROCEDURE_NAME,
                List.of(
                        new NamedArgument(OLDER_THAN, Type.DATETIME, false)
                ),
                IcebergTableOperation.EXPIRE_SNAPSHOTS
        );
    }

    @Override
    public void execute(IcebergTableProcedureContext context, Map<String, ConstantOperator> args) {
        if (args.size() > 1) {
            throw new StarRocksConnectorException("invalid args. only support `older_than` in the expire snapshot operation");
        }

        long olderThanMillis;
        ConstantOperator olderThanArg = args.get(OLDER_THAN);
        if (olderThanArg == null) {
            olderThanMillis = -1L;
        } else {
            LocalDateTime time = olderThanArg.castTo(Type.DATETIME).map(ConstantOperator::getDatetime)
                    .orElseThrow(() ->
                            new StarRocksConnectorException("invalid argument type for %s, expected DATETIME", OLDER_THAN));
            olderThanMillis = Duration.ofSeconds(time.atZone(TimeUtils.getTimeZone().toZoneId()).toEpochSecond()).toMillis();
        }

        ExpireSnapshots expireSnapshots = context.transaction().expireSnapshots();
        if (olderThanMillis != -1) {
            expireSnapshots = expireSnapshots.expireOlderThan(olderThanMillis);
        }
        expireSnapshots.commit();
    }
}