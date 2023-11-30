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

package com.starrocks.sql.ast;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.datacache.DataCacheMetrics;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.List;

public class ShowDataCacheMetricsStmt extends ShowStmt {

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("BackendId", ScalarType.createVarchar(20)))
                    .addColumn(new Column("BackendIP", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Status", ScalarType.createVarchar(30)))
                    .addColumn(new Column("DiskUsage", ScalarType.createVarchar(30)))
                    .addColumn(new Column("MemUsage", ScalarType.createVarchar(30)))
                    .addColumn(new Column("MetaSize", ScalarType.createVarchar(20)))
                    .addColumn(new Column("DiskInfo", ScalarType.createVarchar(30)))
                    .build();

    public ShowDataCacheMetricsStmt(NodePosition nodePosition) {
        super(nodePosition);
    }

    public static List<String> convertDataCacheMetricsToRows(long backendId, String backendIP,
                                                             DataCacheMetrics metrics) {
        List<String> row = new ArrayList<>(META_DATA.getColumnCount());
        row.add(String.valueOf(backendId));
        row.add(backendIP);
        if (metrics == null) {
            for (int i = 0; i < META_DATA.getColumnCount() - 2; i++) {
                row.add("N/A");
            }
        } else {
            row.add(metrics.getStatus().getName());
            row.add(metrics.getDiskUsage());
            row.add(metrics.getMemUsage());
            row.add(metrics.getMetaUsage());
            row.add(metrics.getDiskInfo());
        }
        return row;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowDataCacheMetricsStatement(this, context);
    }
}
