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
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.thrift.TBrokerFileStatus;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Production Tier 2 {@link SampleSubqueryExecutor} for the Broker Load path.
 * Translates the load's resolved {@link BrokerLoadScanContext} into a FILES
 * property map and delegates the SQL synthesis, BE invocation, and JSON
 * decode to {@link FilesSampleSubqueryExecutor}.
 *
 * <p>The sub-query reads exactly the file-status snapshot the load's pending
 * task resolved — re-globbing here would race with the load's own
 * enumeration and risk planning quantile cuts from a different file set
 * than the one actually loaded.
 *
 * <p>Sources the FILES table function or this sampler cannot honor are
 * rejected with {@link StarRocksException}; the coordinator maps that to
 * {@link SkipReason#SAMPLE_FAILED} and the load proceeds without pre-split.
 * Rejected shapes include broker-backed loads, missing/disagreeing file-
 * group formats, non-Parquet/ORC sources, paths containing FILES's
 * {@code ,} list separator, empty file lists, and non-identity file
 * groups. The identity check matters because Broker Load supports per-group
 * {@code WHERE} filters, negative-load rewrites, explicit column lists,
 * {@code SET} clauses, and {@code COLUMNS FROM PATH} — each of which makes
 * the sampled distribution diverge from the actually-loaded distribution.
 * (Example: {@code SET sort_key = upper(file_x)} would have the sampler
 * read the file's raw {@code sort_key} column instead of the mapped value
 * the load inserts.)
 */
final class BrokerLoadSampleSubqueryExecutor extends FilesSampleSubqueryExecutor {

    private static final String ERROR_PREFIX = "Broker Load Tier 2 ";

    BrokerLoadSampleSubqueryExecutor() {
        super(ERROR_PREFIX);
    }

    @VisibleForTesting
    BrokerLoadSampleSubqueryExecutor(SampleQueryRunner sampleQueryRunner) {
        super(ERROR_PREFIX, sampleQueryRunner);
    }

    @Override
    protected Source resolveSource(SampleRequest request) throws StarRocksException {
        BrokerLoadScanContext context = requireBrokerLoadContext(request);
        BrokerDesc brokerDesc = requireBrokerDesc(context);
        rejectIfBrokerBacked(brokerDesc);
        rejectNonIdentityFileGroups(context.fileGroups());

        String format = resolveSharedFormat(context.fileGroups());
        ResolvedFiles resolved = collectResolvedFiles(context.fileStatusesPerGroup());

        Map<String, String> filesProperties = buildFilesProperties(
                brokerDesc, String.join(",", resolved.paths()), format);
        return new Source(filesProperties, resolved.totalBytes(), context.computeResource());
    }

    private static BrokerLoadScanContext requireBrokerLoadContext(SampleRequest request) throws StarRocksException {
        ScanContext scanContext = request.getScanContext();
        if (!(scanContext instanceof BrokerLoadScanContext brokerLoadContext)) {
            throw new StarRocksException(ERROR_PREFIX + "received a "
                    + scanContext.getClass().getSimpleName() + " — wire only the Broker Load load kind here");
        }
        return brokerLoadContext;
    }

    private static BrokerDesc requireBrokerDesc(BrokerLoadScanContext context) throws StarRocksException {
        BrokerDesc brokerDesc = context.brokerDesc();
        if (brokerDesc == null) {
            // BrokerLoadJob's construction requires a BrokerDesc; surface as a
            // clean Tier 2 failure rather than NPE if a future caller violates
            // that invariant.
            throw new StarRocksException(ERROR_PREFIX + "scan context is missing BrokerDesc");
        }
        return brokerDesc;
    }

    private static void rejectIfBrokerBacked(BrokerDesc brokerDesc) throws StarRocksException {
        if (brokerDesc.hasBroker()) {
            throw new StarRocksException(ERROR_PREFIX
                    + "broker-backed sources are not supported — FILES uses FE-local Hadoop "
                    + "access which does not honor the broker's filesystem/auth");
        }
    }

    private static void rejectNonIdentityFileGroups(List<BrokerFileGroup> fileGroups) throws StarRocksException {
        for (BrokerFileGroup fileGroup : fileGroups) {
            if (fileGroup.getWhereExpr() != null) {
                throw new StarRocksException(ERROR_PREFIX
                        + "WHERE filter on file group is not supported "
                        + "(sampler would observe rows the load will exclude)");
            }
            if (fileGroup.isNegative()) {
                throw new StarRocksException(ERROR_PREFIX
                        + "negative-load file groups are not supported");
            }
            List<String> columnsFromPath = fileGroup.getColumnsFromPath();
            if (columnsFromPath != null && !columnsFromPath.isEmpty()) {
                throw new StarRocksException(ERROR_PREFIX
                        + "columns_from_path remapping is not supported");
            }
            List<ImportColumnDesc> columnExpressions = fileGroup.getColumnExprList();
            if (columnExpressions != null && !columnExpressions.isEmpty()) {
                throw new StarRocksException(ERROR_PREFIX
                        + "explicit column list or SET clauses on file group are not supported");
            }
            Map<String, Pair<String, List<String>>> hadoopFunctions = fileGroup.getColumnToHadoopFunction();
            if (hadoopFunctions != null && !hadoopFunctions.isEmpty()) {
                throw new StarRocksException(ERROR_PREFIX
                        + "legacy hadoop column functions on file group are not supported");
            }
        }
    }

    /**
     * A single FILES call takes one {@code format} property, so every file
     * group must declare the same non-null format. CSV and JSON are rejected
     * in this commit — translating CSV byte-valued options onto FILES
     * {@code csv.*} string properties is a follow-up.
     */
    private static String resolveSharedFormat(List<BrokerFileGroup> fileGroups) throws StarRocksException {
        if (fileGroups.isEmpty()) {
            throw new StarRocksException(ERROR_PREFIX + "no file groups in scan context");
        }
        String sharedFormat = null;
        for (BrokerFileGroup fileGroup : fileGroups) {
            String declaredFormat = fileGroup.getFileFormat();
            if (declaredFormat == null) {
                throw new StarRocksException(ERROR_PREFIX
                        + "file group has no declared format (extension inference is not supported by Tier 2)");
            }
            String normalizedFormat = declaredFormat.toLowerCase(Locale.ROOT);
            if (sharedFormat == null) {
                sharedFormat = normalizedFormat;
            } else if (!sharedFormat.equals(normalizedFormat)) {
                throw new StarRocksException(ERROR_PREFIX + "file groups disagree on format: \""
                        + sharedFormat + "\" vs \"" + normalizedFormat + "\"");
            }
        }
        if (!"parquet".equals(sharedFormat) && !"orc".equals(sharedFormat)) {
            throw new StarRocksException(ERROR_PREFIX
                    + "format \"" + sharedFormat + "\" is not yet supported (Parquet and ORC only)");
        }
        return sharedFormat;
    }

    /**
     * Walks the load's resolved file-status snapshot, dropping directory
     * entries, summing file byte totals, and returning the per-file paths
     * for the FILES {@code path} property. A path containing {@code ,} is
     * rejected because FILES has no escape syntax for its path-list
     * separator.
     */
    private static ResolvedFiles collectResolvedFiles(
            List<List<TBrokerFileStatus>> fileStatusesPerGroup) throws StarRocksException {
        List<String> paths = new ArrayList<>();
        long totalBytes = 0L;
        for (List<TBrokerFileStatus> filesInGroup : fileStatusesPerGroup) {
            for (TBrokerFileStatus fileStatus : filesInGroup) {
                if (fileStatus.isDir) {
                    continue;
                }
                if (fileStatus.path.indexOf(',') >= 0) {
                    throw new StarRocksException(ERROR_PREFIX
                            + "file path contains \",\" which FILES treats as a path-list separator: "
                            + fileStatus.path);
                }
                paths.add(fileStatus.path);
                totalBytes += fileStatus.size;
            }
        }
        if (paths.isEmpty()) {
            throw new StarRocksException(ERROR_PREFIX + "no files to sample (all entries were directories or empty)");
        }
        return new ResolvedFiles(paths, totalBytes);
    }

    /**
     * Broker properties (e.g. {@code fs.s3a.access.key}) pass through verbatim
     * — FILES and Broker Load share the same Hadoop {@code FileSystem}
     * configuration surface. {@code path} and {@code format} are appended
     * last so a misconfigured BrokerDesc cannot silently override them.
     */
    private static Map<String, String> buildFilesProperties(
            BrokerDesc brokerDesc, String commaSeparatedPaths, String format) {
        Map<String, String> filesProperties = new LinkedHashMap<>(brokerDesc.getProperties());
        filesProperties.put(TableFunctionTable.PROPERTY_PATH, commaSeparatedPaths);
        filesProperties.put(TableFunctionTable.PROPERTY_FORMAT, format);
        return filesProperties;
    }

    private record ResolvedFiles(List<String> paths, long totalBytes) {
    }
}
