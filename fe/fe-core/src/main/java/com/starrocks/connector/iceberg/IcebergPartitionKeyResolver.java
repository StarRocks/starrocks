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

import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.ExternalPartitionKeyResolver;
import com.starrocks.connector.ExternalPartitionMappingContext;
import com.starrocks.connector.PartitionKeyResolutionPath;
import com.starrocks.connector.PartitionKeyResolutionResult;
import com.starrocks.connector.PartitionUtil;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Resolves Iceberg base partitions to MV partition keys.
 * <p>
 * Phase 1: only handles current-spec direct mapping (same logic as default path,
 * but routed here so future phases have a natural attachment point).
 * <p>
 * Future phases will add resolution methods in this order:
 * <ol>
 *   <li>{@code resolveByCurrentSpec} (current phase)</li>
 *   <li>{@code resolveByHistoricalSpec}</li>
 *   <li>{@code resolveByPartitionExprFallback}</li>
 *   <li>{@code resolveBySyntheticTransform}</li>
 * </ol>
 */
public class IcebergPartitionKeyResolver implements ExternalPartitionKeyResolver {

    public static final IcebergPartitionKeyResolver INSTANCE = new IcebergPartitionKeyResolver();

    private IcebergPartitionKeyResolver() {
    }

    @Override
    public PartitionKeyResolutionResult resolve(ExternalPartitionMappingContext mappingContext,
                                                String basePartitionName)
            throws AnalysisException {
        return resolveByCurrentSpec(mappingContext, basePartitionName);
    }

    private PartitionKeyResolutionResult resolveByCurrentSpec(ExternalPartitionMappingContext mappingContext,
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

        return PartitionKeyResolutionResult.of(mvPartitionKey, PartitionKeyResolutionPath.ICEBERG_CURRENT_SPEC);
    }

    // Phase 2: resolveByHistoricalSpec
    // Phase 3: resolveByPartitionExprFallback
    // Phase 4: resolveBySyntheticTransform
}
