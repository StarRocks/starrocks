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
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.connector.iceberg.IcebergPartitionData;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.thrift.TIcebergDataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Planner- and commit-side support for writing Iceberg V3 deletion vectors (Puffin DV blobs
 * replacing Parquet position-delete files on format-version &gt;= 3 tables).
 */
public final class IcebergDeletionVectorSupport {

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
     * Happy-path guard for V3 DELETE. The deletion-vector write path does not yet merge a data
     * file's pre-existing deletes, nor express per-file partition under partition-spec evolution
     * (both are planned follow-ups), so reject those cases up front rather than silently dropping
     * deletes or writing mismatched partition metadata.
     */
    public static void assertV3DeleteSupported(IcebergTable icebergTable) {
        Table nativeTbl = icebergTable.getNativeTable();
        if (nativeTbl.specs().size() > 1) {
            throw new StarRocksPlannerException(
                    "Iceberg V3 DELETE is not yet supported on tables with evolved partition specs: "
                            + icebergTable.getName(), ErrorType.UNSUPPORTED);
        }
        Snapshot current = nativeTbl.currentSnapshot();
        if (current != null && hasExistingDeleteFiles(nativeTbl, current)) {
            throw new StarRocksPlannerException(
                    "Iceberg V3 DELETE is not yet supported on tables with pre-existing delete files "
                            + "(previous-delete merge lands in a follow-up): " + icebergTable.getName(),
                    ErrorType.UNSUPPORTED);
        }
    }

    private static boolean hasExistingDeleteFiles(Table nativeTbl, Snapshot current) {
        String total = current.summary() == null ? null
                : current.summary().get(SnapshotSummary.TOTAL_DELETE_FILES_PROP);
        if (total != null) {
            try {
                return Long.parseLong(total) > 0;
            } catch (NumberFormatException ignore) {
                // fall through to the manifest-based check
            }
        }
        return !current.deleteManifests(nativeTbl.io()).isEmpty();
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
     * Builds the Iceberg {@link org.apache.iceberg.DeleteFile} for a BE-written deletion vector:
     * a Puffin blob rather than a Parquet position-delete file. DV blobs carry no Parquet column
     * stats (no withMetrics) but must carry the blob's location within the Puffin file (content
     * offset/size) and the referenced data file.
     */
    public static org.apache.iceberg.DeleteFile buildDeletionVectorFile(
            TIcebergDataFile dataFile, PartitionSpec partitionSpec, Table nativeTbl) {
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
        if (partitionSpec.isPartitioned() && !dataFile.isSetPartition_path()) {
            throw new StarRocksConnectorException(
                    "Iceberg deletion vector for a partitioned table is missing partition_path: "
                            + dataFile.getPath());
        }
        FileMetadata.Builder dvBuilder = FileMetadata.deleteFileBuilder(partitionSpec)
                .ofPositionDeletes()
                .withPath(dataFile.path)
                .withFormat(FileFormat.PUFFIN)
                .withFileSizeInBytes(dataFile.file_size_in_bytes)
                .withRecordCount(dataFile.record_count)
                .withReferencedDataFile(dataFile.getReferenced_data_file())
                .withContentOffset(dataFile.getContent_offset())
                .withContentSizeInBytes(dataFile.getContent_size_in_bytes())
                .withPartition(partitionSpec.isPartitioned() ?
                        IcebergPartitionData.partitionDataFromPath(
                                IcebergMetadata.getIcebergRelativePartitionPath(
                                        IcebergUtil.tableDataLocation(nativeTbl),
                                        dataFile.partition_path),
                                dataFile.isSetPartition_null_fingerprint() ?
                                        dataFile.getPartition_null_fingerprint() :
                                        "0".repeat(partitionSpec.fields().size()),
                                partitionSpec) : null);
        return dvBuilder.build();
    }

    /**
     * A DELETE writes Iceberg V3 deletion vectors when its output delete files are Puffin blobs that
     * reference a data file, as opposed to V2 Parquet/ORC position-delete files. The commit path uses
     * this to apply DV-specific commit-time conflict validation.
     */
    public static boolean isDeletionVectorDelete(List<TIcebergDataFile> dataFiles) {
        for (TIcebergDataFile dataFile : dataFiles) {
            if ("puffin".equalsIgnoreCase(dataFile.getFormat()) && dataFile.isSetReferenced_data_file()) {
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
            if ("puffin".equalsIgnoreCase(dataFile.getFormat()) && dataFile.isSetReferenced_data_file()) {
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
