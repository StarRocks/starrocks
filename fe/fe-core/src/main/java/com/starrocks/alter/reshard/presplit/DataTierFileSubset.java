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

import com.google.common.base.CharMatcher;
import com.starrocks.catalog.Column;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.thrift.TBrokerFileStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The input files a Broker Load or INSERT-from-FILES data-tier sample scans.
 *
 * <p>Reading every input file makes the sample's cost grow with the load, and a large enough load
 * overruns the pre-submit budget and loses its pre-split. Past
 * {@link Config#tablet_pre_split_data_tier_scan_byte_limit} the sample scans the subset
 * {@link SampleFileSelector} picks -- stratified by path partition value when every partition source is
 * read from the file path -- while {@link #totalBytes}, which sizes the split, stays the whole input.
 * When a partition source is read from the file data instead, a subset could miss whole partitions, so
 * every file is still scanned. Otherwise every file is scanned exactly as before.
 */
final class DataTierFileSubset {

    private static final Logger LOG = LogManager.getLogger(DataTierFileSubset.class);

    /** How the scanned files were chosen. {@link #label} is the metric label. */
    enum Mode {
        SUBSET("subset", "a subset of the files"),
        DISABLED("disabled", "every file: the scan limit is disabled"),
        UNDER_LIMIT("under_limit", "every file: the input is within the scan limit"),
        ALL_SELECTED("all_selected", "every file: the selection covers them all"),
        PARTITION_FROM_FILE_DATA("partition_from_file_data",
                "every file: a partition column is read from the file data, or path and literal partition "
                        + "columns are mixed, so a subset could miss partitions"),
        PATH_NOT_EXPRESSIBLE("path_not_expressible",
                "every file: a selected path cannot be passed to FILES as an exact path");

        private final String label;
        private final String description;

        Mode(String label, String description) {
            this.label = label;
            this.description = description;
        }
    }

    private final Mode mode;
    private final List<String> paths;
    private final long scannedBytes;
    private final long totalBytes;
    private final int totalFiles;
    private final List<Estimates.PartitionSourceBytes> partitionSourceBytes;
    private final String detail;

    private DataTierFileSubset(Mode mode, List<String> paths, long scannedBytes, long totalBytes, int totalFiles,
                               List<Estimates.PartitionSourceBytes> partitionSourceBytes, String detail) {
        this.mode = mode;
        this.paths = List.copyOf(paths);
        this.scannedBytes = scannedBytes;
        this.totalBytes = totalBytes;
        this.totalFiles = totalFiles;
        this.partitionSourceBytes = List.copyOf(partitionSourceBytes);
        this.detail = detail;
    }

    /**
     * Chooses the files to scan out of a load's listed files. {@code pathPartitions} is non-null when every
     * partition source is read from the path; it stratifies a subset and yields the exact per-partition
     * bytes. {@code partitionFromFileData} is true when the files' partitions cannot be told from their
     * paths or from literals alone -- a partition source is read from the file data, or path and literal
     * partition sources are mixed: the files a subset leaves out may then hold whole partitions, so every
     * file is scanned instead. {@code requireExactFilesPaths} is for a caller that must hand the subset to
     * FILES as explicit paths: when one of them cannot be expressed so, the result scans every file instead.
     *
     * @throws StarRocksException when the input exceeds the scan limit, every partition source is read from
     *         the path, and a file's path lacks one of their values; the load's own scan fails on that path too
     */
    static DataTierFileSubset choose(List<TBrokerFileStatus> fileStatuses, PathPartitionValues pathPartitions,
                                     boolean partitionFromFileData, boolean requireExactFilesPaths)
            throws StarRocksException {
        long byteLimit = Config.tablet_pre_split_data_tier_scan_byte_limit;
        int minFiles = Config.tablet_pre_split_data_tier_min_scan_files;
        int maxFiles = Config.tablet_pre_split_data_tier_max_scan_files;
        List<TBrokerFileStatus> files = new ArrayList<>(fileStatuses.size());
        long totalBytes = 0L;
        for (TBrokerFileStatus fileStatus : fileStatuses) {
            if (!fileStatus.isDir) {
                files.add(fileStatus);
                totalBytes += fileStatus.size;
            }
        }
        if (byteLimit <= 0L) {
            return everyFile(Mode.DISABLED, files, totalBytes, "");
        }
        if (totalBytes <= byteLimit) {
            return everyFile(Mode.UNDER_LIMIT, files, totalBytes, "");
        }
        if (partitionFromFileData) {
            return everyFile(Mode.PARTITION_FROM_FILE_DATA, files, totalBytes, "");
        }
        List<SampleFileSelector.Candidate> candidates = new ArrayList<>(files.size());
        int nonEmptyFiles = 0;
        for (TBrokerFileStatus file : files) {
            candidates.add(new SampleFileSelector.Candidate(file.path, file.size,
                    pathPartitions == null ? List.of() : rawValuesOf(pathPartitions, file.path)));
            if (file.size > 0L) {
                nonEmptyFiles++;
            }
        }
        List<SampleFileSelector.Candidate> selected = SampleFileSelector.select(candidates, byteLimit, minFiles, maxFiles);
        if (selected.size() == nonEmptyFiles) {
            return everyFile(Mode.ALL_SELECTED, files, totalBytes, "");
        }
        List<String> selectedPaths = new ArrayList<>(selected.size());
        long selectedBytes = 0L;
        for (SampleFileSelector.Candidate candidate : selected) {
            selectedPaths.add(candidate.path());
            selectedBytes += candidate.bytes();
        }
        if (requireExactFilesPaths) {
            for (String path : selectedPaths) {
                String problem = exactFilesPathProblem(path);
                if (problem != null) {
                    // The path itself stays out of the log: an object key can carry identifying data.
                    return everyFile(Mode.PATH_NOT_EXPRESSIBLE, files, totalBytes, ": it contains " + problem);
                }
            }
        }
        String detail = selectedBytes <= byteLimit ? "" : String.format(
                "; above the %d-byte scan limit: files are taken whole, and the file floor can add files up to %d "
                        + "bytes", byteLimit, SampleFileSelector.floorByteCeiling(byteLimit));
        return new DataTierFileSubset(Mode.SUBSET, selectedPaths, selectedBytes, totalBytes, files.size(),
                pathPartitions == null ? List.of() : bytesByPathPartition(candidates, pathPartitions), detail);
    }

    /**
     * {@link PathPartitionValues#rawValues}, with a failure reported without the path: the parser's message
     * names the file, and an object key can carry identifying data. The load's own scan reports the path.
     */
    private static List<String> rawValuesOf(PathPartitionValues pathPartitions, String path)
            throws StarRocksException {
        try {
            return pathPartitions.rawValues(path);
        } catch (StarRocksException missingValue) {
            throw new StarRocksException("Pre-split data tier: a file path lacks a value of the partition columns "
                    + "read from the path");
        }
    }

    private static DataTierFileSubset everyFile(Mode mode, List<TBrokerFileStatus> files, long totalBytes,
                                                String detail) {
        List<String> allPaths = new ArrayList<>(files.size());
        for (TBrokerFileStatus file : files) {
            allPaths.add(file.path);
        }
        return new DataTierFileSubset(mode, allPaths, totalBytes, totalBytes, allPaths.size(), List.of(), detail);
    }

    /**
     * The {@code partitionFromFileData} argument of {@link #choose}: whether the partition sources are not all
     * path columns ({@code pathPartitions} is null) and at least one of them is not fed by a literal, i.e. it
     * is read from the file data, or path and literal sources are mixed. A literal puts every row in one
     * partition, so literal-only partition sources are safe to subset. {@code literalFedColumnNames} holds the
     * lower-cased names of the partition sources the load feeds with a literal.
     */
    static boolean partitionFromFileData(List<Column> partitionSourceColumns, PathPartitionValues pathPartitions,
                                         Set<String> literalFedColumnNames) {
        if (pathPartitions != null) {
            return false;
        }
        for (Column partitionSource : partitionSourceColumns) {
            if (!literalFedColumnNames.contains(partitionSource.getName().toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    /**
     * The bytes of every file per path partition value. A value that does not convert to its column type
     * is left out: the load writes no row with it as that value, so no sampled row can need its size.
     */
    private static List<Estimates.PartitionSourceBytes> bytesByPathPartition(
            List<SampleFileSelector.Candidate> candidates, PathPartitionValues pathPartitions) {
        Map<List<String>, Long> bytesByValues = new LinkedHashMap<>();
        for (SampleFileSelector.Candidate candidate : candidates) {
            bytesByValues.merge(candidate.stratum(), candidate.bytes(), Long::sum);
        }
        List<Estimates.PartitionSourceBytes> partitionSourceBytes = new ArrayList<>(bytesByValues.size());
        int unconvertible = 0;
        for (Map.Entry<List<String>, Long> entry : bytesByValues.entrySet()) {
            try {
                partitionSourceBytes.add(new Estimates.PartitionSourceBytes(
                        pathPartitions.typed(entry.getKey()), entry.getValue()));
            } catch (RuntimeException notOfColumnType) {
                unconvertible++;
            }
        }
        if (unconvertible > 0) {
            // Counted, not printed: a partition value is table data.
            LOG.info("Pre-split data tier leaves {} of {} path partition values out of the per-partition sizes: "
                    + "they do not convert to their column type", unconvertible, bytesByValues.size());
        }
        return partitionSourceBytes;
    }

    /**
     * Whether FILES reads exactly {@code path}, and nothing else, when given it as one entry of its
     * {@code path} property. FILES splits that property on every {@code ,} (there is no escape), trims
     * each entry, and globs it: <code>* ? [ {</code> are wildcards, a backslash escapes, and a colon in
     * the path part makes the glob reject the path.
     */
    static boolean isExactFilesPath(String path) {
        return exactFilesPathProblem(path) == null;
    }

    /** What keeps FILES from reading exactly {@code path}, for the log, or {@code null} when nothing does. */
    private static String exactFilesPathProblem(String path) {
        // FILES trims each entry with Guava's Splitter.trimResults(), i.e. CharMatcher.whitespace(), which
        // also strips non-ASCII whitespace that String.trim() keeps.
        if (path.isEmpty() || !CharMatcher.whitespace().trimFrom(path).equals(path)) {
            return "surrounding whitespace";
        }
        if (path.indexOf(',') >= 0) {
            return "a ',' (the FILES path-list separator)";
        }
        for (char special : new char[] {'*', '?', '[', '{', '\\'}) {
            if (path.indexOf(special) >= 0) {
                return "a glob character";
            }
        }
        int pathStart = 0;
        int schemeEnd = path.indexOf(':');
        if (schemeEnd > 0 && path.startsWith("//", schemeEnd + 1)) {
            pathStart = path.indexOf('/', schemeEnd + 3);
            if (pathStart < 0) {
                return null;
            }
        } else if (schemeEnd > 0 && path.startsWith("/", schemeEnd + 1)) {
            pathStart = schemeEnd + 1;
        }
        return path.indexOf(':', pathStart) < 0 ? null : "a ':' in the path";
    }

    /** Logs the choice and records it in the metrics and the load profile. */
    void report(String loadDescription) {
        LOG.info("Pre-split {}scans {}/{} files, {}/{} bytes ({}{})", loadDescription, paths.size(), totalFiles,
                scannedBytes, totalBytes, mode.description, detail);
        PreSplitMetrics.recordDataTierFileSelection(mode.label, scannedBytes, totalBytes);
        PreSplitProfile.recordDataTierFileSelection(String.format("%s %d/%d files %d/%d bytes",
                mode.label, paths.size(), totalFiles, scannedBytes, totalBytes));
    }

    Mode mode() {
        return mode;
    }

    boolean isSubset() {
        return mode == Mode.SUBSET;
    }

    /** The paths to scan, in listing order: the subset, or every file. */
    List<String> paths() {
        return paths;
    }

    long scannedBytes() {
        return scannedBytes;
    }

    long totalBytes() {
        return totalBytes;
    }

    /** Exact bytes per path partition value; empty unless a subset is taken of path-partitioned files. */
    List<Estimates.PartitionSourceBytes> partitionSourceBytes() {
        return partitionSourceBytes;
    }
}
