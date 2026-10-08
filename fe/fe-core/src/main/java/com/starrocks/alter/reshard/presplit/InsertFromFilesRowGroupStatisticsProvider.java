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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.StarRocksException;
import com.starrocks.thrift.TBrokerFileStatus;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Production meta-tier {@link RowGroupStatisticsProvider} for the INSERT-from-FILES
 * load path. Enumerates the {@link TableFunctionTable}'s already-resolved file
 * list, opens each file's footer via the {@link MetaTierFormat} reader for the
 * table's format (Parquet or ORC), and concatenates per-stripe/row-group
 * statistics projected onto the request's sort key. Shared Hadoop-side wiring
 * (configuration build, broker → Hadoop file-status conversion, concurrent
 * footer reads) lives in {@link PreSplitHadoopAccess}.
 *
 * <p>Two shapes the data tier serves are declined here so the pipeline falls back to it: a WHERE
 * clause, because a footer describes every row in the file and no footer statistic can be narrowed
 * to the rows a predicate keeps; and a sort-key column no file column backs -- one the projection
 * leaves unmapped, or one the SELECT feeds with a literal, which lives in no footer.
 */
final class InsertFromFilesRowGroupStatisticsProvider implements RowGroupStatisticsProvider {

    @Override
    public List<RowGroupStatistics> fetch(SampleRequest request) throws StarRocksException {
        InsertFromFilesScanContext context = requireInsertFromFilesContext(request);
        if (context.wherePredicateSql() != null) {
            throw new MetaTierUnavailableException(
                    "the INSERT carries a WHERE clause; row-group statistics describe every row in the "
                            + "file, not the rows the predicate keeps, so footer-derived boundaries would "
                            + "describe a different row set than the load writes");
        }
        TableFunctionTable sourceTable = context.sourceTable();
        // FILES() reports one format for the whole table, so resolve the reader once.
        MetaTierFormat format = MetaTierFormat.fromTableFunctionFormat(sourceTable.getFormat());
        List<Column> sortKeyColumns =
                sourceNamedSortKey(request.getSortKey(), context.targetToSourceColumnNames());

        Configuration hadoopConfig = PreSplitHadoopAccess.buildHadoopConfiguration(sourceTable.getProperties());

        // Read every non-directory file's footer. The pipeline picks K (tablet
        // count) from total file bytes, and ParquetMetadataSampler computes
        // K-1 row-quantile cuts from the full per-stripe stats list — a
        // partial enumeration would bias the cuts toward the prefix.
        List<PreSplitHadoopAccess.FooterRead> footerReads = new ArrayList<>();
        for (TBrokerFileStatus brokerFileStatus : sourceTable.loadFileList()) {
            if (!brokerFileStatus.isDir) {
                footerReads.add(new PreSplitHadoopAccess.FooterRead(
                        format, PreSplitHadoopAccess.toHadoopFileStatus(brokerFileStatus)));
            }
        }
        return PreSplitHadoopAccess.readFooters(footerReads, hadoopConfig, sortKeyColumns, context.loadTimeZone());
    }

    /**
     * Re-labels each target sort-key column with the FILES column that backs it, keeping the TARGET
     * type. The footer readers locate a field by {@link Column#getName()} and then type-check and
     * decode its min/max against {@link Column#getType()} — exactly the pairing the data tier makes
     * when it projects the source column and decodes it into the destination schema type. An empty
     * mapping means the projection is name-identity and the request's own columns already name the
     * right fields.
     */
    private static List<Column> sourceNamedSortKey(
            List<Column> sortKeyColumns, Map<String, String> targetToSourceColumnNames)
            throws MetaTierUnavailableException {
        if (targetToSourceColumnNames.isEmpty()) {
            return sortKeyColumns;
        }
        List<String> sourceNames = InsertSelectSourceColumns.lookup(sortKeyColumns, targetToSourceColumnNames);
        if (sourceNames == null) {
            throw new MetaTierUnavailableException(
                    "a sort-key column has no FILES column behind it (unmapped, or fed by a literal), "
                            + "so no footer statistic describes it");
        }
        List<Column> renamed = new ArrayList<>(sortKeyColumns.size());
        for (int i = 0; i < sortKeyColumns.size(); i++) {
            Column targetColumn = sortKeyColumns.get(i);
            String sourceName = sourceNames.get(i);
            renamed.add(sourceName.equalsIgnoreCase(targetColumn.getName())
                    ? targetColumn
                    : new Column(sourceName, targetColumn.getType(), targetColumn.isAllowNull()));
        }
        return renamed;
    }

    private static InsertFromFilesScanContext requireInsertFromFilesContext(SampleRequest request)
            throws MetaTierUnavailableException {
        ScanContext scanContext = request.getScanContext();
        if (!(scanContext instanceof InsertFromFilesScanContext insertFromFilesContext)) {
            throw new MetaTierUnavailableException(
                    "InsertFromFilesRowGroupStatisticsProvider received a " + scanContext.getClass().getSimpleName()
                            + " — wire only the INSERT-from-FILES load kind here");
        }
        return insertFromFilesContext;
    }
}
