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
import com.starrocks.thrift.TFileFormatType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.util.List;

/**
 * The set of file formats whose footer/stripe statistics the meta tier can read,
 * plus the dispatch to the matching reader. Centralizes the "format → reader"
 * choice so both the INSERT-from-FILES and Broker Load providers agree on the
 * supported window. Each provider resolves a {@link MetaTierFormat} from its own
 * format representation (FILES()'s format string vs Broker Load's
 * {@link TFileFormatType}) and then calls {@link #read}; any other format means
 * "fall back to data tier" and surfaces as {@link MetaTierUnavailableException}.
 */
enum MetaTierFormat {
    PARQUET,
    ORC;

    /** Resolve from the FILES() table function's format string (one format per table). */
    static MetaTierFormat fromTableFunctionFormat(String format) throws MetaTierUnavailableException {
        if ("parquet".equalsIgnoreCase(format)) {
            return PARQUET;
        }
        if ("orc".equalsIgnoreCase(format)) {
            return ORC;
        }
        throw new MetaTierUnavailableException(
                "meta tier supports Parquet and ORC sources only; FILES() reported format \"" + format + "\"");
    }

    /** Resolve from Broker Load's per-file resolved {@link TFileFormatType}. */
    static MetaTierFormat fromBrokerFormatType(TFileFormatType formatType, String filePath)
            throws MetaTierUnavailableException {
        if (formatType == TFileFormatType.FORMAT_PARQUET) {
            return PARQUET;
        }
        if (formatType == TFileFormatType.FORMAT_ORC) {
            return ORC;
        }
        throw new MetaTierUnavailableException(String.format(
                "meta tier supports Parquet and ORC sources only; file \"%s\" resolved to format %s",
                filePath, formatType));
    }

    List<RowGroupStatistics> read(FileStatus fileStatus, Configuration hadoopConfig, Column sortKeyColumn)
            throws StarRocksException {
        return switch (this) {
            case PARQUET -> ParquetRowGroupStatisticsReader.read(fileStatus, hadoopConfig, sortKeyColumn);
            case ORC -> OrcStripeStatisticsReader.read(fileStatus, hadoopConfig, sortKeyColumn);
        };
    }
}
