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

package com.starrocks.sql.ast.pipe;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.load.pipe.Pipe;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Optional;

public class ShowPipeStmt extends ShowStmt {

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("DATABASE_NAME", ScalarType.createVarchar(64)))
                    .addColumn(new Column("PIPE_ID", ScalarType.BIGINT))
                    .addColumn(new Column("PIPE_NAME", ScalarType.createVarchar(64)))
                    .addColumn(new Column("STATE", ScalarType.createVarcharType(8)))
                    .addColumn(new Column("TABLE_NAME", ScalarType.createVarchar(64)))
                    .addColumn(new Column("LOAD_STATUS", ScalarType.createVarchar(512)))
                    .addColumn(new Column("LAST_ERROR", ScalarType.createVarchar(1024)))
                    .addColumn(new Column("CREATED_TIME", ScalarType.DATETIME))
                    .build();

    private String dbName;
    private final String like;
    private final Expr where;
    private final List<OrderByElement> orderBy;
    private final LimitElement limit;
    private List<OrderByPair> orderByPairs;

    public ShowPipeStmt(String dbName, String like, Expr where, List<OrderByElement> orderBy, LimitElement limit,
                        NodePosition pos) {
        super(pos);
        this.dbName = dbName;
        this.like = like;
        this.where = where;
        this.orderBy = orderBy;
        this.limit = limit;
    }

    /**
     * NOTE: Must be consistent with the META_DATA
     */
    public static void handleShow(List<Comparable> row, Pipe pipe) {
        Optional<Database> db = GlobalStateMgr.getCurrentState().getStarRocksMeta().mayGetDb(pipe.getPipeId().getDbId());
        row.add(db.map(Database::getFullName).orElse(""));
        row.add(String.valueOf(pipe.getPipeId().getId()));
        row.add(pipe.getName());
        row.add(String.valueOf(pipe.getState()));
        row.add(Optional.ofNullable(pipe.getTargetTable()).map(TableName::toString).orElse(""));
        row.add(pipe.getLoadStatus().toJson());
        row.add(pipe.getLastErrorInfo().toJson());
        row.add(DateUtils.formatTimestampInSeconds(pipe.getCreatedTime()));
    }

    public static int findSlotIndex(String name) {
        return META_DATA.getColumnIdx(name);
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getLike() {
        return like;
    }

    public Expr getWhere() {
        return where;
    }

    public List<OrderByElement> getOrderBy() {
        return orderBy;
    }

    public long getLimit() {
        if (limit == null) {
            return -1;
        }
        return limit.getLimit();
    }

    public long getOffset() {
        if (limit == null) {
            return -1;
        }
        return limit.getOffset();
    }

    public List<OrderByPair> getOrderByPairs() {
        return orderByPairs;
    }

    public void setOrderByPairs(List<OrderByPair> orderByPairs) {
        this.orderByPairs = orderByPairs;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowPipeStatement(this, context);
    }
}
