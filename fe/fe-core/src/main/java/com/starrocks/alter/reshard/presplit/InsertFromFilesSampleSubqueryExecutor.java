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
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.StarRocksException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Production data-tier {@link SampleSubqueryExecutor} for the INSERT-from-FILES
 * path. Re-issues the load's original {@code FILES(...)} properties via
 * {@link FilesSampleSubqueryExecutor}'s shared scaffolding so the BE scan covers
 * the same files the load will scan — except that past
 * {@code tablet_pre_split_data_tier_scan_byte_limit} the {@code path} property is
 * replaced by the explicit subset of the load's files {@link DataTierFileSubset}
 * chooses — carrying over the statement's WHERE predicate and projecting every
 * key column through the scan context's target-&gt;FILES column mapping so a
 * renamed or reordered projection is sampled from the column the load actually
 * writes.
 */
final class InsertFromFilesSampleSubqueryExecutor extends FilesSampleSubqueryExecutor {

    private static final String ERROR_PREFIX = "INSERT-from-FILES data tier ";

    InsertFromFilesSampleSubqueryExecutor() {
        super(ERROR_PREFIX);
    }

    @VisibleForTesting
    InsertFromFilesSampleSubqueryExecutor(SampleQueryRunner sampleQueryRunner) {
        super(ERROR_PREFIX, sampleQueryRunner);
    }

    @Override
    protected Source resolveSource(SampleRequest request) throws StarRocksException {
        ScanContext scanContext = request.getScanContext();
        if (!(scanContext instanceof InsertFromFilesScanContext insertFromFilesContext)) {
            throw new StarRocksException(ERROR_PREFIX + "received a "
                    + scanContext.getClass().getSimpleName() + " — wire only the INSERT-from-FILES load kind here");
        }
        TableFunctionTable sourceTable = insertFromFilesContext.sourceTable();
        PathPartitionValues pathPartitions = pathPartitionValues(sourceTable, insertFromFilesContext, request);
        DataTierFileSubset files = DataTierFileSubset.choose(sourceTable.loadFileList(), pathPartitions,
                DataTierFileSubset.partitionFromFileData(request.getPartitionSourceColumns(), pathPartitions,
                        insertFromFilesContext.targetToConstantSql().keySet()),
                /*requireExactFilesPaths=*/ true);
        files.report(ERROR_PREFIX);
        Map<String, String> filesProperties = files.isSubset()
                ? withPath(sourceTable.getProperties(), String.join(",", files.paths()))
                : sourceTable.getProperties();
        return new Source(
                filesProperties,
                files.totalBytes(),
                insertFromFilesContext.computeResource(),
                insertFromFilesContext.wherePredicateSql(),
                insertFromFilesContext.targetToSourceColumnNames(),
                insertFromFilesContext.targetToConstantSql(),
                files.scannedBytes(),
                files.partitionSourceBytes());
    }

    /**
     * A partition source is read from the path when the FILES column that feeds it is one of the
     * {@code columns_from_path} columns. An empty mapping means the projection is name-identity; a
     * partition source fed by a literal has no FILES column at all, so it is never read from the path.
     */
    private static PathPartitionValues pathPartitionValues(
            TableFunctionTable sourceTable, InsertFromFilesScanContext context, SampleRequest request) {
        List<Column> partitionSources = request.getPartitionSourceColumns();
        if (context.targetToSourceColumnNames().isEmpty() && context.targetToConstantSql().isEmpty()) {
            return PathPartitionValues.of(sourceTable.getColumnsFromPath(), partitionSources);
        }
        List<String> sourceNames = InsertSelectSourceColumns.lookup(partitionSources, context.targetToSourceColumnNames());
        return sourceNames == null ? null
                : PathPartitionValues.of(sourceTable.getColumnsFromPath(), partitionSources, sourceNames);
    }

    /**
     * The statement's FILES properties with {@code path} replaced. The parser's map is case-insensitive
     * and shared with the rest of the INSERT's analysis, so this writes into a case-insensitive copy: a
     * {@code path} key spelled in any case is replaced rather than duplicated, the key order (and so the
     * rest of the SQL) is unchanged, and the statement's own map is never touched.
     */
    static Map<String, String> withPath(Map<String, String> properties, String commaSeparatedPaths) {
        Map<String, String> copy = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        copy.putAll(properties);
        copy.put(TableFunctionTable.PROPERTY_PATH, commaSeparatedPaths);
        return copy;
    }

    /**
     * Projects each rollup's sort key through the same target-&gt;FILES mapping the base sort key
     * uses, rather than by the target's own column names as the default does: a projection that
     * renames or reorders columns leaves a rollup key under a different name in the file too.
     */
    @Override
    protected List<String> secondaryProjectionIdents(SampleRequest request) throws StarRocksException {
        ScanContext scanContext = request.getScanContext();
        if (!(scanContext instanceof InsertFromFilesScanContext insertFromFilesContext)) {
            throw new StarRocksException(ERROR_PREFIX + "received a "
                    + scanContext.getClass().getSimpleName() + " — wire only the INSERT-from-FILES load kind here");
        }
        List<String> idents = new ArrayList<>();
        for (SecondaryIndexSpec spec : request.getSecondaryIndexSortKeys()) {
            idents.addAll(filesProjections(spec.sortKey(), insertFromFilesContext.targetToSourceColumnNames(),
                    insertFromFilesContext.targetToConstantSql()));
        }
        return idents;
    }
}
