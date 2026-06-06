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

package com.starrocks.connector;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.type.DateType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Default resolver for Hive, Hudi, Paimon, and JDBC tables.
 * Extracts partition values directly from the partition name string.
 */
public class DefaultPartitionKeyResolver implements ExternalPartitionKeyResolver {

    public static final DefaultPartitionKeyResolver INSTANCE = new DefaultPartitionKeyResolver();

    private static final String MYSQL_PARTITION_MAXVALUE = "MAXVALUE";

    private DefaultPartitionKeyResolver() {
    }

    @Override
    public PartitionKeyResolutionResult resolve(ExternalPartitionMappingContext mappingContext,
                                                String basePartitionName)
            throws AnalysisException {
        if (mappingContext.getBaseTable().isJDBCTable()) {
            return resolveJdbc(mappingContext, basePartitionName);
        }
        return resolveDirect(mappingContext, basePartitionName);
    }

    private PartitionKeyResolutionResult resolveDirect(ExternalPartitionMappingContext mappingContext,
                                                       String basePartitionName)
            throws AnalysisException {
        List<String> basePartitionValues = PartitionUtil.toPartitionValues(basePartitionName);
        List<Integer> mvRefBasePartitionColumnIndexes = mappingContext.getMvRefBasePartitionColumnIndexes();

        PartitionKey mvPartitionKey = PartitionUtil.createPartitionKey(
                mvRefBasePartitionColumnIndexes.stream().map(basePartitionValues::get).collect(Collectors.toList()),
                mvRefBasePartitionColumnIndexes.stream()
                        .map(mappingContext.getBaseTablePartitionColumns()::get)
                        .collect(Collectors.toList()),
                mappingContext.getBaseTable());

        return PartitionKeyResolutionResult.of(mvPartitionKey, PartitionKeyResolutionPath.DIRECT_NAME_VALUE);
    }

    private PartitionKeyResolutionResult resolveJdbc(ExternalPartitionMappingContext mappingContext,
                                                     String basePartitionName)
            throws AnalysisException {
        List<Integer> mvRefBasePartitionColumnIndexes = mappingContext.getMvRefBasePartitionColumnIndexes();
        List<Column> baseTablePartitionColumns = mappingContext.getBaseTablePartitionColumns();

        PartitionKey mvPartitionKey = PartitionUtil.createPartitionKey(
                mvRefBasePartitionColumnIndexes.stream()
                        .map(index -> normalizeJdbcPartitionValue(
                                baseTablePartitionColumns.get(index), basePartitionName))
                        .collect(Collectors.toList()),
                mvRefBasePartitionColumnIndexes.stream()
                        .map(baseTablePartitionColumns::get)
                        .collect(Collectors.toList()),
                mappingContext.getBaseTable());

        return PartitionKeyResolutionResult.of(mvPartitionKey, PartitionKeyResolutionPath.JDBC_NAME_VALUE);
    }

    private static String normalizeJdbcPartitionValue(Column baseTablePartitionColumn, String basePartitionName) {
        if (basePartitionName.equalsIgnoreCase(MYSQL_PARTITION_MAXVALUE)) {
            if (baseTablePartitionColumn.getPrimitiveType().isIntegerType()) {
                return IntLiteral.createMaxValue(baseTablePartitionColumn.getType()).getStringValue();
            } else {
                return DateLiteral.createMaxValue(DateType.DATE).getStringValue();
            }
        }
        return basePartitionName;
    }
}
