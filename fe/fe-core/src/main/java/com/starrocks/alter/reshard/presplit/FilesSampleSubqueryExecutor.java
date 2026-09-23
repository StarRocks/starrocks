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

package com.starrocks.alter.reshard.presplit;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.catalog.Column;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Shared scaffolding for data-tier sample sub-query executors that translate the
 * load's source into a {@code SELECT <sort_key> FROM FILES(...)} sub-query.
 * Subclasses provide a {@link Source} (FILES property map plus the load's
 * total input byte total and {@link ComputeResource}); everything else —
 * sampling-rate math, SQL synthesis, BE invocation, JSON row decode — is
 * inherited from {@link AbstractSqlSampleSubqueryExecutor}.
 *
 * <p>The sub-query is submitted through {@link com.starrocks.qe.SimpleExecutor#executeDQL}
 * with a {@link com.starrocks.qe.ConnectContext} pinned to the load's compute resource, so
 * cluster resources, audit logging, and credential redaction are inherited
 * rather than reimplemented. Result rows arrive as JSON
 * {@code {"data":[...]}} envelopes (HTTP_PROTOCAL sink); each row's projected
 * columns are fed to {@link com.starrocks.catalog.Variant#of} using their
 * respective declared types.
 */
abstract class FilesSampleSubqueryExecutor extends AbstractSqlSampleSubqueryExecutor {

    /**
     * Subclass-supplied FILES sub-query inputs.
     *
     * <p>{@code wherePredicateSqlOrNull} is copied verbatim into the sampling sub-query, and
     * {@code targetToSourceColumnNames} re-points each projected target column at the FILES column
     * that backs it. An EMPTY map means the file's columns already carry the target's names, which
     * is what the three-argument constructor below asserts for its callers.
     * {@code targetToConstantPartitionSql} carries the partition columns the load feeds with a
     * literal rather than a FILES column.
     */
    protected record Source(
            Map<String, String> filesProperties, long totalFileBytes, ComputeResource computeResource,
            String wherePredicateSqlOrNull, Map<String, String> targetToSourceColumnNames,
            Map<String, String> targetToConstantPartitionSql) {
        public Source {
            Objects.requireNonNull(filesProperties, "filesProperties");
            Objects.requireNonNull(computeResource, "computeResource");
            Objects.requireNonNull(targetToSourceColumnNames, "targetToSourceColumnNames");
            Objects.requireNonNull(targetToConstantPartitionSql, "targetToConstantPartitionSql");
            if (totalFileBytes < 0) {
                throw new IllegalArgumentException("totalFileBytes must be non-negative, was " + totalFileBytes);
            }
        }

        /** No predicate, and the file columns carry the target's own names. */
        protected Source(
                Map<String, String> filesProperties, long totalFileBytes, ComputeResource computeResource) {
            this(filesProperties, totalFileBytes, computeResource, null, Map.of(), Map.of());
        }
    }

    FilesSampleSubqueryExecutor(String errorPrefix) {
        super(errorPrefix, "TabletPreSplitDataTierFilesSubquery");
    }

    @VisibleForTesting
    FilesSampleSubqueryExecutor(String errorPrefix, SampleQueryRunner sampleQueryRunner) {
        super(errorPrefix, sampleQueryRunner);
    }

    /**
     * Translate the load's scan context into FILES-call inputs the shared
     * orchestration can execute. Implementations throw
     * {@link StarRocksException} for any source shape the FILES sub-query
     * cannot honor.
     */
    protected abstract Source resolveSource(SampleRequest request) throws StarRocksException;

    @Override
    protected final SampleSpec resolveSampleSpec(SampleRequest request) throws StarRocksException {
        Source source = resolveSource(request);
        String fromClauseSql = "FILES(" + buildPropertiesClause(source.filesProperties()) + ")";
        List<Column> sortKeyColumns = request.getSortKey();
        List<Column> partitionSourceColumns = request.getPartitionSourceColumns();
        Map<String, String> targetToSource = source.targetToSourceColumnNames();
        return new SampleSpec(fromClauseSql, source.wherePredicateSqlOrNull(),
                source.totalFileBytes(), source.computeResource(),
                filesProjectionIdents(sortKeyColumns, targetToSource),
                filesPartitionProjections(partitionSourceColumns, targetToSource,
                        source.targetToConstantPartitionSql()),
                sortKeyColumns, partitionSourceColumns);
    }

    private static List<String> columnIdentsOf(List<Column> columns) {
        return columns.stream()
                .map(column -> SqlUtils.getIdentSql(column.getName()))
                .collect(Collectors.toList());
    }

    /**
     * Like {@link #filesProjectionIdents}, except that a partition column the load feeds with a
     * literal is projected as that literal cast to the column type.
     */
    private static List<String> filesPartitionProjections(
            List<Column> partitionColumns, Map<String, String> targetToSourceColumnNames,
            Map<String, String> targetToConstantPartitionSql) throws StarRocksException {
        if (targetToConstantPartitionSql.isEmpty()) {
            return filesProjectionIdents(partitionColumns, targetToSourceColumnNames);
        }
        List<String> projections = InsertSelectSourceColumns.partitionProjections(
                partitionColumns, targetToSourceColumnNames, targetToConstantPartitionSql);
        if (projections == null) {
            throw new StarRocksException("a partition column has neither a FILES column nor a constant");
        }
        return projections;
    }

    /**
     * Projects each target column by the FILES column that backs it, or by its own name when the
     * mapping is empty (the projection is name-identity). Throws -&gt; the sample fails -&gt; the
     * load proceeds without pre-split, rather than letting a boundary be computed from the wrong
     * FILES column; the admitting gate in {@code FilesPreSplitSource#prepare} already proved every
     * projected column is mapped, so this remains a fail-safe.
     */
    static List<String> filesProjectionIdents(List<Column> columns, Map<String, String> targetToSourceColumnNames)
            throws StarRocksException {
        if (targetToSourceColumnNames.isEmpty()) {
            return columnIdentsOf(columns);
        }
        List<String> sourceNames = InsertSelectSourceColumns.lookup(columns, targetToSourceColumnNames);
        if (sourceNames == null) {
            throw new StarRocksException("a projected column has no FILES column mapping");
        }
        return identsOf(sourceNames);
    }

    /**
     * Builds the {@code FILES(...)} properties clause from the supplied map.
     * String literal escaping covers both {@code "} and {@code \} so a
     * crafted property value cannot break out of the double-quoted form and
     * inject SQL into the internal-context sub-query.
     */
    static String buildPropertiesClause(Map<String, String> filesProperties) {
        return filesProperties.entrySet().stream()
                .map(property -> '"' + escapeDoubleQuoted(property.getKey()) + "\" = \""
                        + escapeDoubleQuoted(property.getValue()) + '"')
                .collect(Collectors.joining(", "));
    }

    /**
     * Escapes both backslash and double-quote inside a double-quoted SQL
     * string literal. Backslash MUST be escaped first to avoid double-
     * escaping the slashes inserted by the quote escape. Short-circuits
     * the common case (paths, simple credentials) to avoid two full
     * {@code String.replace} copies of multi-MB property values.
     */
    private static String escapeDoubleQuoted(String value) {
        if (value.indexOf('\\') < 0 && value.indexOf('"') < 0) {
            return value;
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
