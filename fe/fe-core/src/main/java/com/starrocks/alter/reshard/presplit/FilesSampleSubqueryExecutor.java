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

    /** Subclass-supplied FILES sub-query inputs. */
    protected record Source(
            Map<String, String> filesProperties, long totalFileBytes, ComputeResource computeResource) {
        public Source {
            Objects.requireNonNull(filesProperties, "filesProperties");
            Objects.requireNonNull(computeResource, "computeResource");
            if (totalFileBytes < 0) {
                throw new IllegalArgumentException("totalFileBytes must be non-negative, was " + totalFileBytes);
            }
        }
    }

    FilesSampleSubqueryExecutor(String errorPrefix) {
        super(errorPrefix);
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
        return new SampleSpec(fromClauseSql, /*whereClauseSqlOrNull=*/ null,
                source.totalFileBytes(), source.computeResource(),
                identsOf(sortKeyColumns), identsOf(partitionSourceColumns),
                sortKeyColumns, partitionSourceColumns);
    }

    private static List<String> identsOf(List<Column> columns) {
        return columns.stream()
                .map(column -> SqlUtils.getIdentSql(column.getName()))
                .collect(Collectors.toList());
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
