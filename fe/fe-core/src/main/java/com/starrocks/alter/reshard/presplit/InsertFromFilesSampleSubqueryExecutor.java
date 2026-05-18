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
import com.starrocks.common.StarRocksException;
import com.starrocks.thrift.TBrokerFileStatus;

import java.util.List;

/**
 * Production Tier 2 {@link SampleSubqueryExecutor} for the INSERT-from-FILES
 * path. Re-issues the load's original {@code FILES(...)} properties verbatim
 * via {@link FilesSampleSubqueryExecutor}'s shared scaffolding so the BE scan
 * covers the same files the load will scan.
 */
final class InsertFromFilesSampleSubqueryExecutor extends FilesSampleSubqueryExecutor {

    private static final String ERROR_PREFIX = "INSERT-from-FILES Tier 2 ";

    InsertFromFilesSampleSubqueryExecutor() {
        super(ERROR_PREFIX);
    }

    @VisibleForTesting
    InsertFromFilesSampleSubqueryExecutor(SampleQueryRunner sampleQueryRunner) {
        super(ERROR_PREFIX, sampleQueryRunner);
    }

    @Override
    protected Source resolveSource(SampleRequest request) throws StarRocksException {
        ScanContext scanContext = request.getScanContext();
        if (!(scanContext instanceof InsertFromFilesScanContext insertFromFilesContext)) {
            throw new StarRocksException(ERROR_PREFIX + "received a "
                    + scanContext.getClass().getSimpleName() + " — wire only the INSERT-from-FILES load kind here");
        }
        TableFunctionTable sourceTable = insertFromFilesContext.sourceTable();
        return new Source(
                sourceTable.getProperties(),
                sumFileBytes(sourceTable.loadFileList()),
                insertFromFilesContext.computeResource());
    }

    private static long sumFileBytes(List<TBrokerFileStatus> fileStatuses) {
        long total = 0L;
        for (TBrokerFileStatus fileStatus : fileStatuses) {
            if (!fileStatus.isDir) {
                total += fileStatus.size;
            }
        }
        return total;
    }
}
