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

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Variant;
import com.starrocks.common.StarRocksException;
import com.starrocks.fs.HdfsUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * The partition-source values a load writes for one input file, read from the file's path.
 *
 * <p>Broker Load ({@code COLUMNS FROM PATH}) and FILES ({@code columns_from_path}) both compute a
 * path column on the FE: {@code FileScanNode} runs {@link HdfsUtil#parseColumnsFromPath} over the
 * listed file path and ships the strings to the BE, which copies them into a VARCHAR slot unchanged.
 * Running the same function over the same listed path therefore yields exactly the string the load
 * writes, and {@link #typed} applies the conversion the data-tier sampler applies to a sampled cell,
 * so a file can be attributed to the partition its rows land in without reading it.
 */
final class PathPartitionValues {

    private final List<String> columnsFromPath;
    private final int[] pathColumnIndexes;
    private final List<Column> partitionSourceColumns;

    private PathPartitionValues(List<String> columnsFromPath, int[] pathColumnIndexes,
                                List<Column> partitionSourceColumns) {
        this.columnsFromPath = columnsFromPath;
        this.pathColumnIndexes = pathColumnIndexes;
        this.partitionSourceColumns = partitionSourceColumns;
    }

    /**
     * {@code sourceColumnNames.get(i)} names the column of the load's source that feeds
     * {@code partitionSourceColumns.get(i)}. Returns {@code null} unless there is a partition source and
     * every one of them is fed by a path column. The match is case-insensitive, as column names are for
     * both load kinds; the path is still parsed with the declared names, as the load parses it.
     */
    static PathPartitionValues of(List<String> columnsFromPath, List<Column> partitionSourceColumns,
                                  List<String> sourceColumnNames) {
        Preconditions.checkArgument(partitionSourceColumns.size() == sourceColumnNames.size(),
                "one source column name per partition source column");
        if (columnsFromPath.isEmpty() || partitionSourceColumns.isEmpty()) {
            return null;
        }
        int[] pathColumnIndexes = new int[partitionSourceColumns.size()];
        for (int i = 0; i < pathColumnIndexes.length; i++) {
            pathColumnIndexes[i] = indexOfIgnoringCase(columnsFromPath, sourceColumnNames.get(i));
            if (pathColumnIndexes[i] < 0) {
                return null;
            }
        }
        return new PathPartitionValues(List.copyOf(columnsFromPath), pathColumnIndexes,
                List.copyOf(partitionSourceColumns));
    }

    /** {@link #of(List, List, List)} for a source whose columns carry the partition sources' own names. */
    static PathPartitionValues of(List<String> columnsFromPath, List<Column> partitionSourceColumns) {
        List<String> names = new ArrayList<>(partitionSourceColumns.size());
        for (Column partitionSourceColumn : partitionSourceColumns) {
            names.add(partitionSourceColumn.getName());
        }
        return of(columnsFromPath, partitionSourceColumns, names);
    }

    /** One raw path value per partition source, in partition-source order. */
    List<String> rawValues(String filePath) throws StarRocksException {
        List<String> pathValues = HdfsUtil.parseColumnsFromPath(filePath, columnsFromPath);
        List<String> values = new ArrayList<>(pathColumnIndexes.length);
        for (int pathColumnIndex : pathColumnIndexes) {
            values.add(pathValues.get(pathColumnIndex));
        }
        return values;
    }

    /** Converts {@link #rawValues} to the partition-source column types; throws when a value does not convert. */
    List<Variant> typed(List<String> rawValues) {
        List<Variant> values = new ArrayList<>(rawValues.size());
        for (int i = 0; i < rawValues.size(); i++) {
            values.add(Variant.of(partitionSourceColumns.get(i).getType(), rawValues.get(i)));
        }
        return values;
    }

    private static int indexOfIgnoringCase(List<String> names, String name) {
        if (name == null) {
            return -1;
        }
        for (int i = 0; i < names.size(); i++) {
            if (names.get(i).equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }
}
