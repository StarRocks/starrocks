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
import org.apache.hadoop.fs.FileStatus;

import java.util.ArrayList;
import java.util.List;

/**
 * Production meta-tier {@link RowGroupStatisticsProvider} for the INSERT-from-FILES
 * load path. Enumerates the {@link TableFunctionTable}'s already-resolved file
 * list, opens each Parquet file's footer via
 * {@link ParquetRowGroupStatisticsReader}, and concatenates per-row-group
 * statistics projected onto the request's sort key. Shared Hadoop-side wiring
 * (configuration build, broker → Hadoop file-status conversion) lives in
 * {@link PreSplitHadoopAccess}.
 */
final class InsertFromFilesRowGroupStatisticsProvider implements RowGroupStatisticsProvider {

    @Override
    public List<RowGroupStatistics> fetch(SampleRequest request) throws StarRocksException {
        InsertFromFilesScanContext context = requireInsertFromFilesContext(request);
        TableFunctionTable sourceTable = context.sourceTable();
        rejectNonParquetFormat(sourceTable);
        // ParquetMetadataSampler.rejectCompositeSortKey runs upstream in tryPlan
        // before this provider is invoked, so a single-element sort key is the
        // contract by the time we get here.
        Column sortKeyColumn = request.getSortKey().get(0);

        Configuration hadoopConfig = PreSplitHadoopAccess.buildHadoopConfiguration(sourceTable.getProperties());

        // Read every non-directory file's footer. The pipeline picks K (tablet
        // count) from total file bytes, and ParquetMetadataSampler computes
        // K-1 row-quantile cuts from the full per-row-group stats list — a
        // partial enumeration would bias the cuts toward the prefix.
        List<RowGroupStatistics> aggregated = new ArrayList<>();
        for (TBrokerFileStatus brokerFileStatus : sourceTable.loadFileList()) {
            if (brokerFileStatus.isDir) {
                continue;
            }
            FileStatus hadoopFileStatus = PreSplitHadoopAccess.toHadoopFileStatus(brokerFileStatus);
            aggregated.addAll(
                    ParquetRowGroupStatisticsReader.read(hadoopFileStatus, hadoopConfig, sortKeyColumn));
        }
        return aggregated;
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

    private static void rejectNonParquetFormat(TableFunctionTable sourceTable) throws MetaTierUnavailableException {
        String format = sourceTable.getFormat();
        if (format == null || !"parquet".equalsIgnoreCase(format)) {
            throw new MetaTierUnavailableException(
                    "meta tier supports Parquet sources only; FILES() reported format \"" + format + "\"");
        }
    }
}
