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

import com.starrocks.analysis.TableRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

public class ShowDataSkewStmt extends ShowStmt {
    private static final ShowResultSetMetaData SHOW_DATA_SKEW_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("PartitionName", ScalarType.createVarchar(30)))
                    .addColumn(new Column("BucketId", ScalarType.createVarchar(10)))
                    .addColumn(new Column("RowCount", ScalarType.createVarchar(30)))
                    .addColumn(new Column("RowCount%", ScalarType.createVarchar(10)))
                    .addColumn(new Column("DataSize", ScalarType.createVarchar(30)))
                    .addColumn(new Column("DataSize%", ScalarType.createVarchar(10)))
                    .build();

    private TableRef tblRef;

    public ShowDataSkewStmt(TableRef tblRef) {
        this(tblRef, NodePosition.ZERO);
    }

    public ShowDataSkewStmt(TableRef tblRef, NodePosition pos) {
        super(pos);
        this.tblRef = tblRef;
    }

    public String getDbName() {
        return tblRef.getName().getDb();
    }

    public void setDbName(String dbName) {
        this.tblRef.getName().setDb(dbName);
    }

    public String getTblName() {
        return tblRef.getName().getTbl();
    }

    public PartitionNames getPartitionNames() {
        return tblRef.getPartitionNames();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return SHOW_DATA_SKEW_META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowDataSkewStatement(this, context);
    }
}
