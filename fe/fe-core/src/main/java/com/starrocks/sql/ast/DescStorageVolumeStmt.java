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
import com.starrocks.common.AnalysisException;
import com.starrocks.common.proc.StorageVolumeProcNode;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class DescStorageVolumeStmt extends ShowStmt {
    private final String storageVolumeName;
    private StorageVolumeProcNode node;

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Name", ScalarType.createVarchar(256)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("IsDefault", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Location", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Params", ScalarType.createVarchar(256)))
                    .addColumn(new Column("Enabled", ScalarType.createVarchar(256)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(20)))
                    .build();

    public DescStorageVolumeStmt(String storageVolumeName, NodePosition pos) {
        super(pos);
        this.storageVolumeName = storageVolumeName;
        this.node = new StorageVolumeProcNode(storageVolumeName);
    }

    public String getName() {
        return storageVolumeName;
    }

    public List<List<String>> getResultRows() throws AnalysisException {
        return node.fetchResult().getRows();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDescStorageVolumeStatement(this, context);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DESC STORAGE VOLUME ").append(storageVolumeName);
        return sb.toString();
    }
}
