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
import com.starrocks.common.StarRocksException;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.Load;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TFileFormatType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.util.ArrayList;
import java.util.List;

/**
 * Production Tier 1 {@link RowGroupStatisticsProvider} for the Broker Load
 * path. Mirrors {@link InsertFromFilesRowGroupStatisticsProvider} but
 * consumes the file-status snapshot the load already resolved (carried on
 * {@link BrokerLoadScanContext#fileStatusesPerGroup()}); re-globbing here
 * would race with the load's own enumeration and risk planning quantile
 * cuts from a different file set than the one actually loaded.
 *
 * <p>Format detection follows Broker Load's own rule
 * ({@link Load#getFormatType}): the file group's declared
 * {@code FileFormat} takes precedence; when absent we fall back to the
 * file-extension inference the load uses at scan time. Any non-Parquet
 * file makes the provider throw {@link Tier1UnavailableException} so the
 * pipeline falls back to Tier 2.
 *
 * <p>Only direct HDFS-style loads (no broker) are supported today. When
 * {@code brokerDesc.hasBroker()} is true the real load reads through the
 * broker, whose filesystem and auth semantics may differ from the FE-local
 * Hadoop {@code FileSystem} this provider uses to read footers — we fall
 * back to Tier 2 rather than risk a credentialed-via-broker source that
 * FE-local access cannot reach. Routing footer reads through a
 * broker-backed seekable input is a deliberate follow-up.
 *
 * <p>Hadoop-side wiring (configuration build, broker → Hadoop file-status
 * conversion) is shared with the INSERT-from-FILES provider via
 * {@link PreSplitHadoopAccess}.
 */
final class BrokerLoadRowGroupStatisticsProvider implements RowGroupStatisticsProvider {

    @Override
    public List<RowGroupStatistics> fetch(SampleRequest request) throws StarRocksException {
        BrokerLoadScanContext context = requireBrokerLoadContext(request);
        BrokerDesc brokerDesc = requireBrokerDesc(context);
        rejectIfBrokerBacked(brokerDesc);
        List<BrokerFileGroup> fileGroups = context.fileGroups();
        List<List<TBrokerFileStatus>> fileStatusesPerGroup = context.fileStatusesPerGroup();
        // ParquetMetadataSampler.rejectCompositeSortKey runs upstream in tryPlan
        // before this provider is invoked, so a single-element sort key is the
        // contract by the time we get here.
        Column sortKeyColumn = request.getSortKey().get(0);

        Configuration hadoopConfig = PreSplitHadoopAccess.buildHadoopConfiguration(brokerDesc.getProperties());

        // Read every non-directory file's footer across all file groups. The
        // pipeline picks K from total file bytes, so partial enumeration would
        // bias the planner's quantile cuts.
        List<RowGroupStatistics> aggregated = new ArrayList<>();
        for (int groupIndex = 0; groupIndex < fileGroups.size(); groupIndex++) {
            BrokerFileGroup fileGroup = fileGroups.get(groupIndex);
            String declaredFormat = fileGroup.getFileFormat();
            for (TBrokerFileStatus brokerFileStatus : fileStatusesPerGroup.get(groupIndex)) {
                if (brokerFileStatus.isDir) {
                    continue;
                }
                rejectIfNotParquet(declaredFormat, brokerFileStatus.path);
                FileStatus hadoopFileStatus = PreSplitHadoopAccess.toHadoopFileStatus(brokerFileStatus);
                aggregated.addAll(
                        ParquetRowGroupStatisticsReader.read(hadoopFileStatus, hadoopConfig, sortKeyColumn));
            }
        }
        return aggregated;
    }

    private static BrokerLoadScanContext requireBrokerLoadContext(SampleRequest request)
            throws Tier1UnavailableException {
        ScanContext scanContext = request.getScanContext();
        if (!(scanContext instanceof BrokerLoadScanContext brokerLoadContext)) {
            throw new Tier1UnavailableException(
                    "BrokerLoadRowGroupStatisticsProvider received a " + scanContext.getClass().getSimpleName()
                            + " — wire only the Broker Load load kind here");
        }
        return brokerLoadContext;
    }

    private static void rejectIfBrokerBacked(BrokerDesc brokerDesc) throws Tier1UnavailableException {
        if (brokerDesc.hasBroker()) {
            throw new Tier1UnavailableException(
                    "Broker-backed Broker Load is not yet supported by Tier 1 — FE-local Hadoop access "
                            + "would not honor the broker's filesystem/auth; falling back to Tier 2");
        }
    }

    private static BrokerDesc requireBrokerDesc(BrokerLoadScanContext context) throws Tier1UnavailableException {
        BrokerDesc brokerDesc = context.brokerDesc();
        if (brokerDesc == null) {
            // Broker Load always has a BrokerDesc in practice (BrokerLoadJob's
            // construction requires it); we surface this as a Tier-1 fallback
            // rather than NPE if a future caller violates the invariant.
            throw new Tier1UnavailableException(
                    "Broker Load pre-split sample request is missing BrokerDesc");
        }
        return brokerDesc;
    }

    private static void rejectIfNotParquet(String declaredFormat, String filePath) throws Tier1UnavailableException {
        // Load.getFormatType matches Broker Load's own scan-time decision:
        // declared FileFormat wins; otherwise the file extension is consulted.
        // Both `*.parquet`-without-FORMAT-AS-PARQUET and explicit-Parquet groups
        // route through Tier 1 here.
        TFileFormatType formatType = Load.getFormatType(declaredFormat, filePath);
        if (formatType != TFileFormatType.FORMAT_PARQUET) {
            throw new Tier1UnavailableException(String.format(
                    "Tier 1 supports Parquet sources only; Broker Load file \"%s\" resolved to format %s",
                    filePath, formatType));
        }
    }
}
