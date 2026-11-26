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

package com.starrocks.connector.paimon;

import com.starrocks.connector.GetRemoteFilesParams;

import java.util.Map;

/** Identifies final data scan-range planning, excluding optimizer and statistics probes. */
public final class PaimonScanRangeParams extends GetRemoteFilesParams {
    private final Map<Integer, String> columnNames;
    private final PaimonAnnQuery annQuery;

    public PaimonScanRangeParams(GetRemoteFilesParams.Builder builder) {
        this(builder, Map.of());
    }

    public PaimonScanRangeParams(GetRemoteFilesParams.Builder builder, Map<Integer, String> columnNames) {
        this(builder, columnNames, null);
    }

    public PaimonScanRangeParams(GetRemoteFilesParams.Builder builder, Map<Integer, String> columnNames,
                                 PaimonAnnQuery annQuery) {
        super(builder);
        this.columnNames = Map.copyOf(columnNames);
        this.annQuery = annQuery;
    }

    /** Optimizer column-reference ids mapped to the scan's actual table column names. */
    public Map<Integer, String> getColumnNames() {
        return columnNames;
    }

    public PaimonAnnQuery getAnnQuery() {
        return annQuery;
    }
}
