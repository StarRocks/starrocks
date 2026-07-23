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

package com.starrocks.epack.connector.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.connector.iceberg.IcebergRemoteFileInfo;
import com.starrocks.planner.IcebergDeleteSink;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.sql.IcebergPlannerUtils;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TIcebergDataFile;
import com.starrocks.thrift.TIcebergPreviousDeleteFile;
import com.starrocks.thrift.TSinkCommitInfo;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.ContentFileUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Collectors;

/**
 * Planner- and commit-side support for writing Iceberg V3 deletion vectors (Puffin DV blobs
 * replacing Parquet position-delete files on format-version &gt;= 3 tables), including the
 * previous-delete merge round trip: shipping each scanned data file's pre-existing position
 * deletes to the BE sink and removing exactly the file-scoped originals it folded in.
 */
public final class IcebergDeletionVectorSupport {

    /** The thrift-level format tag of a Puffin deletion-vector entry ("puffin"). */
    public static final String PUFFIN_FORMAT = FileFormat.PUFFIN.name().toLowerCase(Locale.ROOT);

    private IcebergDeletionVectorSupport() {
    }

    /**
     * Returns true when the Iceberg table's format version is >= 3, i.e. DELETE must write a
     * Puffin deletion vector rather than a Parquet position-delete file.
     */
    public static boolean isV3(IcebergTable icebergTable) {
        return icebergTable.getFormatVersion() >= 3;
    }

    /**
     * Builds a required property that hash-distributes the plan output by the {@code _file}
     * (referenced data file) column. V3 DELETE shuffles by _file so all delete rows of one data
     * file converge on a single sink instance, yielding a single deletion vector per file
     * regardless of partitioning or partition evolution.
     */
    public static PhysicalPropertySet createFileHashShuffleProperty(List<ColumnRefOperator> outputColumns) {
        for (ColumnRefOperator outputColumn : outputColumns) {
            if (IcebergTable.FILE_PATH.equalsIgnoreCase(outputColumn.getName())) {
                Preconditions.checkArgument(outputColumn.getId() >= 0, "invalid _file column ref id");
                HashDistributionDesc distributionDesc = new HashDistributionDesc(
                        Lists.newArrayList(outputColumn.getId()), HashDistributionDesc.SourceType.SHUFFLE_AGG);
                DistributionProperty distributionProperty = DistributionProperty.createProperty(
                        DistributionSpec.createHashDistributionSpec(distributionDesc));
                return new PhysicalPropertySet(distributionProperty);
            }
        }
        throw new StarRocksPlannerException(
                "Cannot find " + IcebergTable.FILE_PATH + " column for deletion-vector shuffle",
                ErrorType.INTERNAL_ERROR);
    }

    /**
     * Collects each scanned data file's pre-existing position deletes and its DataFile from the
     * target scan node's materialized planning. Deduped by location: split/coalesce produces
     * multiple tasks per data file.
     */
    public static void collectPreviousDeletes(List<RemoteFileInfo> splits,
                                              Map<String, List<DeleteFile>> previousDeletesByRef,
                                              Map<String, DataFile> plannedDataFiles) {
        for (RemoteFileInfo remoteFileInfo : splits) {
            IcebergRemoteFileInfo icebergRemoteFileInfo = remoteFileInfo.cast();
            FileScanTask task = icebergRemoteFileInfo.getFileScanTask();
            String location = task.file().location();
            if (plannedDataFiles.putIfAbsent(location, task.file()) != null) {
                continue;
            }
            List<DeleteFile> positionDeletes = new ArrayList<>();
            for (DeleteFile deleteFile : task.deletes()) {
                if (deleteFile.content() == FileContent.POSITION_DELETES) {
                    positionDeletes.add(deleteFile);
                }
            }
            if (!positionDeletes.isEmpty()) {
                previousDeletesByRef.put(location, positionDeletes);
            }
        }
    }

    /**
     * Hands the DELETE target's materialized planning result (scanned data files and their
     * pre-existing deletes) to the deletion-vector sink.
     */
    public static void attachPreviousDeletes(ExecPlan execPlan, IcebergTable icebergTable,
                                             IcebergMetadata.IcebergSinkExtra sinkExtra,
                                             IcebergDeleteSink dataSink) {
        // Take the planning result from the DML target's scan node only (same resolution as
        // extractBaseSnapshotId): a same-table subquery — possibly time-traveled onto another
        // snapshot — must not contribute its associations.
        IcebergScanNode target = IcebergPlannerUtils.findScanNodeFor(execPlan, icebergTable);
        if (target == null) {
            return;
        }
        sinkExtra.addScannedDataFiles(new HashSet<>(target.getPlannedDataFiles().values()));
        dataSink.setPreviousDeleteFiles(buildPreviousDeleteFiles(target.getPreviousDeletesByRef(), sinkExtra));
    }

    /**
     * Converts the per-file previous deletes (Iceberg's DeleteFileIndex association,
     * sequence-aware and duplicate-DV guarded) into the sink's thrift form, indexing file-scoped
     * originals for the removeDeletes round trip. The association is predicate-independent, so
     * earlier deletes of other rows in a touched file cannot resurrect. A partition-scoped
     * delete becomes ONE entry carrying its associated-file list, never one entry per
     * association.
     */
    public static List<TIcebergPreviousDeleteFile> buildPreviousDeleteFiles(
            Map<String, List<DeleteFile>> previousDeletesByRef, IcebergMetadata.IcebergSinkExtra extra) {
        List<TIcebergPreviousDeleteFile> result = Lists.newArrayList();
        Map<String, TIcebergPreviousDeleteFile> partitionScopedByPath = new LinkedHashMap<>();
        for (Map.Entry<String, List<DeleteFile>> entry : previousDeletesByRef.entrySet()) {
            String referencedDataFile = entry.getKey();
            for (DeleteFile file : entry.getValue()) {
                if (ContentFileUtil.referencedDataFileLocation(file) == null) {
                    partitionScopedByPath.computeIfAbsent(file.location(), path -> {
                        TIcebergPreviousDeleteFile prev = new TIcebergPreviousDeleteFile();
                        prev.setPath(path);
                        prev.setFormat(file.format().name().toLowerCase(Locale.ROOT));
                        prev.setFile_scoped(false);
                        prev.setRecord_count(file.recordCount());
                        prev.setFile_size_in_bytes(file.fileSizeInBytes());
                        prev.setReferenced_data_files(Lists.newArrayList());
                        return prev;
                    }).addToReferenced_data_files(referencedDataFile);
                    continue;
                }
                boolean isDv = ContentFileUtil.isDV(file);
                TIcebergPreviousDeleteFile prev = new TIcebergPreviousDeleteFile();
                prev.setPath(file.location());
                prev.setFormat(file.format().name().toLowerCase(Locale.ROOT));
                prev.setFile_scoped(true);
                prev.setReferenced_data_files(Lists.newArrayList(referencedDataFile));
                if (isDv) {
                    prev.setContent_offset(file.contentOffset());
                    prev.setContent_size_in_bytes(file.contentSizeInBytes());
                }
                prev.setRecord_count(file.recordCount());
                // Lets the BE merge path skip get_file_size (unsupported on object storage).
                prev.setFile_size_in_bytes(file.fileSizeInBytes());
                // Only file-scoped deletes may ever be removed, so only they enter the
                // removable-set index.
                extra.putPreviousDeleteFile(uniqueDeleteFileKey(
                        file.location(), isDv ? file.contentOffset() : null), file);
                result.add(prev);
            }
        }
        result.addAll(partitionScopedByPath.values());
        return result;
    }

    /**
     * Unique delete-file key for removeDeletes: several DV blobs share one Puffin path and a
     * data file may accumulate several delete files, so DVs key by (path, content_offset) and
     * other deletes by path.
     */
    public static String uniqueDeleteFileKey(String path, Long contentOffset) {
        return contentOffset == null ? path : path + "@" + contentOffset;
    }

    /**
     * The file-scoped old deletes the BE folded into new deletion vectors; the delete commit
     * removes exactly these.
     */
    public static List<TIcebergPreviousDeleteFile> collectRewrittenDeleteFiles(List<TSinkCommitInfo> commitInfos) {
        return commitInfos.stream()
                .filter(TSinkCommitInfo::isSetRewritten_delete_files)
                .flatMap(ci -> ci.getRewritten_delete_files().stream())
                .collect(Collectors.toList());
    }

    /**
     * The scanned data files indexed by location — the source of each deletion-vector entry's
     * spec and partition (a file written under an older spec keeps that spec).
     */
    public static Map<String, DataFile> scannedDataFilesByLocation(Object extra) {
        Map<String, DataFile> scannedByLocation = new HashMap<>();
        if (extra instanceof IcebergMetadata.IcebergSinkExtra) {
            for (DataFile scanned : ((IcebergMetadata.IcebergSinkExtra) extra).getScannedDataFiles()) {
                scannedByLocation.put(scanned.location(), scanned);
            }
        }
        return scannedByLocation;
    }

    /**
     * Removes exactly the file-scoped old deletes the BE reports it folded into new deletion
     * vectors, resolved by unique key against the plan-time collection.
     */
    public static void removeRewrittenDeletes(RowDelta rowDelta,
                                              List<TIcebergPreviousDeleteFile> rewrittenDeleteFiles, Object extra) {
        for (TIcebergPreviousDeleteFile rewritten : rewrittenDeleteFiles) {
            String key = uniqueDeleteFileKey(rewritten.getPath(),
                    PUFFIN_FORMAT.equalsIgnoreCase(rewritten.getFormat()) && rewritten.isSetContent_offset()
                            ? rewritten.getContent_offset() : null);
            DeleteFile original = extra instanceof IcebergMetadata.IcebergSinkExtra
                    ? ((IcebergMetadata.IcebergSinkExtra) extra).getPreviousDeleteFile(key) : null;
            if (original == null) {
                throw new StarRocksConnectorException(
                        "rewritten delete file was not part of the planned previous-delete set: " + key);
            }
            rowDelta.removeDeletes(original);
        }
    }

    /**
     * Rows deleted by the current statement, summed from the BE-reported added_delete_rows (a
     * merged deletion vector's record_count also counts historical deletes). Empty unless EVERY
     * entry carries the field, so a partial mix falls back to the snapshot summary.
     */
    public static OptionalLong currentStatementDeletedRows(List<TIcebergDataFile> dataFiles) {
        if (!dataFiles.isEmpty() && dataFiles.stream().allMatch(TIcebergDataFile::isSetAdded_delete_rows)) {
            return OptionalLong.of(dataFiles.stream().mapToLong(TIcebergDataFile::getAdded_delete_rows).sum());
        }
        return OptionalLong.empty();
    }

    /**
     * Builds the Iceberg {@link org.apache.iceberg.DeleteFile} for a BE-written deletion vector:
     * a Puffin blob rather than a Parquet position-delete file. DV blobs carry no Parquet column
     * stats (no withMetrics) but must carry the blob's location within the Puffin file (content
     * offset/size) and the referenced data file.
     */
    public static org.apache.iceberg.DeleteFile buildDeletionVectorFile(
            TIcebergDataFile dataFile, Table nativeTbl, Map<String, DataFile> scannedByLocation) {
        // A deletion vector must carry its referenced data file and blob coordinates. These
        // are primitive thrift getters (an unset field silently reads 0), so validate before
        // build() rather than committing metadata with a bogus offset/size that only fails on
        // read.
        if (!dataFile.isSetReferenced_data_file() || !dataFile.isSetContent_offset() ||
                !dataFile.isSetContent_size_in_bytes()) {
            throw new StarRocksConnectorException(
                    "Iceberg deletion vector is missing referenced_data_file/content_offset/content_size_in_bytes: "
                            + dataFile.getPath());
        }
        // The entry's spec and partition come from the referenced data file itself; the table's
        // current spec would mismatch under partition evolution.
        DataFile referenced = scannedByLocation.get(dataFile.getReferenced_data_file());
        if (referenced == null) {
            throw new StarRocksConnectorException(
                    "scanned data file not found for deletion vector referencing "
                            + dataFile.getReferenced_data_file());
        }
        PartitionSpec referencedSpec = nativeTbl.specs().get(referenced.specId());
        if (referencedSpec == null) {
            throw new StarRocksConnectorException(
                    "unknown partition spec " + referenced.specId() + " for data file " + referenced.location());
        }
        FileMetadata.Builder dvBuilder = FileMetadata.deleteFileBuilder(referencedSpec)
                .ofPositionDeletes()
                .withPath(dataFile.path)
                .withFormat(FileFormat.PUFFIN)
                .withFileSizeInBytes(dataFile.file_size_in_bytes)
                .withRecordCount(dataFile.record_count)
                .withReferencedDataFile(dataFile.getReferenced_data_file())
                .withContentOffset(dataFile.getContent_offset())
                .withContentSizeInBytes(dataFile.getContent_size_in_bytes())
                .withPartition(referencedSpec.isPartitioned() ? referenced.partition() : null);
        return dvBuilder.build();
    }

    /**
     * A DELETE writes Iceberg V3 deletion vectors when its output delete files are Puffin blobs that
     * reference a data file, as opposed to V2 Parquet/ORC position-delete files. The commit path uses
     * this to apply DV-specific commit-time conflict validation.
     */
    public static boolean isDeletionVectorDelete(List<TIcebergDataFile> dataFiles) {
        for (TIcebergDataFile dataFile : dataFiles) {
            if (PUFFIN_FORMAT.equalsIgnoreCase(dataFile.getFormat()) && dataFile.isSetReferenced_data_file()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Pre-commit uniqueness self-check for V3 deletion vectors: Iceberg 1.10 does not reject two
     * DVs for the same data file within one commit, so a shuffle bug would otherwise land invalid
     * metadata that only fails on read. Enforces a single DV per referenced data file.
     */
    public static void validateSingleDeletionVectorPerFile(List<TIcebergDataFile> dataFiles) {
        Map<String, Integer> dvCountByRef = new HashMap<>();
        for (TIcebergDataFile dataFile : dataFiles) {
            if (PUFFIN_FORMAT.equalsIgnoreCase(dataFile.getFormat()) && dataFile.isSetReferenced_data_file()) {
                int count = dvCountByRef.merge(dataFile.getReferenced_data_file(), 1, Integer::sum);
                if (count >= 2) {
                    throw new StarRocksConnectorException(
                            "Iceberg V3 DELETE produced multiple deletion vectors for the same data file "
                                    + dataFile.getReferenced_data_file()
                                    + "; expected exactly one (shuffle-by-_file invariant violated)");
                }
            }
        }
    }
}
