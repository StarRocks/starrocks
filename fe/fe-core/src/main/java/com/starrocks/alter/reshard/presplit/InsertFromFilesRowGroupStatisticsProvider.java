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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.thrift.TBrokerFileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Production meta-tier {@link RowGroupStatisticsProvider} for the INSERT-from-FILES
 * load path. Enumerates the {@link TableFunctionTable}'s already-resolved file
 * list, opens each file's footer via the {@link MetaTierFormat} reader for the
 * table's format (Parquet or ORC), and concatenates per-stripe/row-group
 * statistics projected onto the request's sort key. Shared Hadoop-side wiring
 * (configuration build, broker → Hadoop file-status conversion) lives in
 * {@link PreSplitHadoopAccess}.
 */
final class InsertFromFilesRowGroupStatisticsProvider implements RowGroupStatisticsProvider {

    @Override
    public List<RowGroupStatistics> fetch(SampleRequest request) throws StarRocksException {
        InsertFromFilesScanContext context = requireInsertFromFilesContext(request);
        TableFunctionTable sourceTable = context.sourceTable();
        // FILES() reports one format for the whole table, so resolve the reader once.
        MetaTierFormat format = MetaTierFormat.fromTableFunctionFormat(sourceTable.getFormat());
        List<Column> sortKeyColumns = request.getSortKey();

        Configuration hadoopConfig = PreSplitHadoopAccess.buildHadoopConfiguration(sourceTable.getProperties());

        // Read every non-directory file's footer. The pipeline picks K (tablet
        // count) from total file bytes, and ParquetMetadataSampler computes
        // K-1 row-quantile cuts from the full per-stripe stats list — a
        // partial enumeration would bias the cuts toward the prefix.
        List<FileStatus> files = new ArrayList<>();
        for (TBrokerFileStatus brokerFileStatus : sourceTable.loadFileList()) {
            if (!brokerFileStatus.isDir) {
                files.add(PreSplitHadoopAccess.toHadoopFileStatus(brokerFileStatus));
            }
        }
        String loadTimeZone = context.loadTimeZone();
        // Footer reads are independent per file and the sampler sorts the aggregated stats, so
        // reading them concurrently only cuts wall time — each footer is a remote round-trip, and a
        // serial pass over a many-file FILES() source otherwise dominates the pre-split hook.
        int parallelism = Math.max(1,
                Math.min(Config.tablet_pre_split_meta_tier_footer_read_parallelism, files.size()));
        if (parallelism == 1) {
            List<RowGroupStatistics> aggregated = new ArrayList<>();
            for (FileStatus file : files) {
                aggregated.addAll(format.read(file, hadoopConfig, sortKeyColumns, loadTimeZone));
            }
            return aggregated;
        }
        ExecutorService footerReadPool = Executors.newFixedThreadPool(parallelism,
                new ThreadFactoryBuilder().setNameFormat("presplit-footer-reader-%d").setDaemon(true).build());
        try {
            List<Future<List<RowGroupStatistics>>> futures = new ArrayList<>(files.size());
            for (FileStatus file : files) {
                futures.add(footerReadPool.submit(
                        () -> format.read(file, hadoopConfig, sortKeyColumns, loadTimeZone)));
            }
            List<RowGroupStatistics> aggregated = new ArrayList<>();
            for (Future<List<RowGroupStatistics>> future : futures) {
                aggregated.addAll(joinFooterRead(future));
            }
            return aggregated;
        } finally {
            footerReadPool.shutdownNow();
        }
    }

    /**
     * Awaits one footer-read task. A per-file {@link StarRocksException} (e.g. a
     * {@link MetaTierUnavailableException} for truncated / unmappable stats) is rethrown unchanged
     * so the pipeline falls back to the data tier exactly as the serial reader would; any other
     * failure is wrapped as a checked {@link StarRocksException}.
     */
    private static List<RowGroupStatistics> joinFooterRead(Future<List<RowGroupStatistics>> future)
            throws StarRocksException {
        try {
            return future.get();
        } catch (ExecutionException executionFailure) {
            Throwable cause = executionFailure.getCause();
            if (cause instanceof StarRocksException starRocksException) {
                throw starRocksException;
            }
            throw new StarRocksException("Parquet/ORC footer read failed during pre-split sampling: "
                    + (cause == null ? executionFailure.getMessage() : cause.getMessage()), cause);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new StarRocksException("Interrupted while reading footers for pre-split sampling", interrupted);
        }
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
