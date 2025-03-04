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

package com.starrocks.sql.optimizer.operator.pattern;

import com.google.common.collect.ImmutableSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.Set;

public class MultiOpPattern extends Pattern {
    public static final ImmutableSet<OperatorType> ALL_SCAN_TYPES = ImmutableSet.<OperatorType>builder()
            .add(OperatorType.LOGICAL_OLAP_SCAN)
            .add(OperatorType.LOGICAL_HIVE_SCAN)
            .add(OperatorType.LOGICAL_ICEBERG_SCAN)
            .add(OperatorType.LOGICAL_HUDI_SCAN)
            .add(OperatorType.LOGICAL_FILE_SCAN)
            .add(OperatorType.LOGICAL_SCHEMA_SCAN)
            .add(OperatorType.LOGICAL_MYSQL_SCAN)
            .add(OperatorType.LOGICAL_ES_SCAN)
            .add(OperatorType.LOGICAL_META_SCAN)
            .add(OperatorType.LOGICAL_JDBC_SCAN)
            .add(OperatorType.LOGICAL_BINLOG_SCAN)
            .add(OperatorType.LOGICAL_VIEW_SCAN)
            .add(OperatorType.LOGICAL_PAIMON_SCAN)
            .add(OperatorType.LOGICAL_ODPS_SCAN)
            .add(OperatorType.PATTERN_SCAN)
            .build();

    private final Set<OperatorType> ops;
    protected MultiOpPattern(Set<OperatorType> ops) {
        super();
        this.ops = ops;
    }

    @Override
    protected boolean matchWithoutChild(OperatorType op) {
        return ops.contains(op);
    }

    public static Pattern ofAllScan() {
        return of(ALL_SCAN_TYPES);
    }

    public static Pattern of(OperatorType... types) {
        return new MultiOpPattern(Set.of(types));
    }

    public static Pattern of(Set<OperatorType> types) {
        return new MultiOpPattern(types);
    }
}
