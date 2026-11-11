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

import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.DateType;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Rolls the table's current snapshot back to the latest snapshot committed strictly before the given
 * timestamp. The timestamp is interpreted in the session time zone, like the other procedures that take
 * a DATETIME argument (e.g. expire_snapshots.older_than).
 */
public class RollbackToTimestampProcedure extends IcebergTableProcedure {
    private static final String PROCEDURE_NAME = "rollback_to_timestamp";

    public static final String TIMESTAMP = "timestamp";

    private static final RollbackToTimestampProcedure INSTANCE = new RollbackToTimestampProcedure();

    public static RollbackToTimestampProcedure getInstance() {
        return INSTANCE;
    }

    private RollbackToTimestampProcedure() {
        super(
                PROCEDURE_NAME,
                List.of(
                        new NamedArgument(TIMESTAMP, DateType.DATETIME, true)
                ),
                IcebergTableOperation.ROLLBACK_TO_TIMESTAMP
        );
    }

    @Override
    public ShowResultSet execute(IcebergTableProcedureContext context, Map<String, ConstantOperator> args) {
        if (args.size() != 1 || !args.containsKey(TIMESTAMP)) {
            throw new StarRocksConnectorException("invalid args. rollback to timestamp must contain `%s`", TIMESTAMP);
        }

        LocalDateTime time = args.get(TIMESTAMP).castTo(DateType.DATETIME)
                .map(ConstantOperator::getDatetime)
                .orElseThrow(() ->
                        new StarRocksConnectorException("invalid argument type for %s, expected DATETIME", TIMESTAMP));
        long timestampMillis = time.atZone(TimeUtils.getTimeZone().toZoneId()).toInstant().toEpochMilli();

        context.transaction().manageSnapshots().rollbackToTime(timestampMillis).commit();
        return null;
    }
}
