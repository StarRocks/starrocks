// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class DescribeStmt extends ShowStmt {

    private static final ShowResultSetMetaData DESC_OLAP_TABLE_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Field", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Key", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Default", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Extra", ScalarType.createVarchar(30)))
                    .build();

    private static final ShowResultSetMetaData DESC_OLAP_TABLE_ALL_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("IndexName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("IndexKeysType", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Field", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Key", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Default", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Extra", ScalarType.createVarchar(30)))
                    .build();

    private static final ShowResultSetMetaData DESC_MYSQL_TABLE_ALL_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Host", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Port", ScalarType.createVarchar(10)))
                    .addColumn(new Column("User", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Password", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Database", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Table", ScalarType.createVarchar(30)))
                    .build();

    // empty col num equals to DESC_OLAP_TABLE_ALL_META_DATA.size()
    public static final List<String> EMPTY_ROW = initEmptyRow();

    private final TableName dbTableName;
    private ProcNodeInterface node;

    List<List<String>> totalRows;

    private boolean isAllTables;
    private boolean isOlapTable;
    private boolean isMaterializedView;

    public DescribeStmt(TableName dbTableName, boolean isAllTables) {
        this.dbTableName = dbTableName;
        this.totalRows = new LinkedList<>();
        this.isAllTables = isAllTables;
    }

    public boolean isAllTables() {
        return isAllTables;
    }

    public String getTableName() {
        return dbTableName.getTbl();
    }

    public String getDb() {
        return dbTableName.getDb();
    }

    public TableName getDbTableName() {
        return dbTableName;
    }

    public List<List<String>> getTotalRows() {
        return totalRows;
    }

    public void setMaterializedView(boolean materializedView) {
        isMaterializedView = materializedView;
    }

    public void setAllTables(boolean allTables) {
        isAllTables = allTables;
    }

    public void setNode(ProcNodeInterface node) {
        this.node = node;
    }

    public boolean isOlapTable() {
        return isOlapTable;
    }

    public void setOlapTable(boolean olapTable) {
        isOlapTable = olapTable;
    }

    public List<List<String>> getResultRows() throws AnalysisException {
        if (isAllTables || isMaterializedView) {
            return totalRows;
        } else {
            Preconditions.checkNotNull(node);
            return node.fetchResult().getRows();
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        if (!isAllTables) {
            if (isMaterializedView) {
                return DESC_OLAP_TABLE_META_DATA;
            } else {
                ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

                ProcResult result = null;
                try {
                    result = node.fetchResult();
                } catch (AnalysisException e) {
                    return builder.build();
                }

                for (String col : result.getColumnNames()) {
                    builder.addColumn(new Column(col, ScalarType.createVarchar(30)));
                }
                return builder.build();
            }
        } else {
            if (isOlapTable) {
                return DESC_OLAP_TABLE_ALL_META_DATA;
            } else {
                return DESC_MYSQL_TABLE_ALL_META_DATA;
            }
        }
    }

    private static List<String> initEmptyRow() {
        List<String> emptyRow = new ArrayList<>(DESC_OLAP_TABLE_ALL_META_DATA.getColumns().size());
        for (int i = 0; i < DESC_OLAP_TABLE_ALL_META_DATA.getColumns().size(); i++) {
            emptyRow.add("");
        }
        return emptyRow;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDescTableStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
