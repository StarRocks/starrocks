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

import com.starrocks.catalog.Database;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.load.pipe.Pipe;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LikePredicate;
import com.starrocks.sql.ast.expression.LimitElement;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Optional;

import static com.starrocks.common.util.Util.normalizeName;

public class ShowPipeStmt extends ShowStmt {
    private String dbName;
    private final String like;
    private final Expr where;
    private final List<OrderByElement> orderBy;
    private final LimitElement limit;
    private List<OrderByPair> orderByPairs;
    private String pattern;
    private PatternMatcher matcher;

    public ShowPipeStmt(String dbName, String like, Expr where, List<OrderByElement> orderBy, LimitElement limit,
            NodePosition pos) {
        super(pos);
        this.dbName = normalizeName(dbName);
        this.like = like;
        this.where = where;
        if (where != null) {
            StringLiteral condition = (StringLiteral) where.getChild(1);
            pattern = condition.getValue();
            if (where instanceof LikePredicate) {
                matcher = PatternMatcher.createMysqlPattern(pattern, true);
            }
        }

        this.orderBy = orderBy;
        this.limit = limit;
    }

    /**
     * NOTE: Must be consistent with the META_DATA
     */
    public static void handleShow(List<Comparable> row, Pipe pipe) {
        Optional<Database> db = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .mayGetDb(pipe.getPipeId().getDbId());
        row.add(db.map(Database::getFullName).orElse(""));
        row.add(String.valueOf(pipe.getPipeId().getId()));
        row.add(pipe.getName());
        row.add(String.valueOf(pipe.getState()));
        row.add(Optional.ofNullable(pipe.getTargetTable()).map(TableName::toString).orElse(""));
        row.add(pipe.getLoadStatus().toJson());
        row.add(pipe.getLastErrorInfo().toJson());
        row.add(DateUtils.formatTimestampInSeconds(pipe.getCreatedTime()));
    }

    public boolean match(Pipe pipe) {
        if (where == null) {
            return true;
        }

        if (matcher != null) {
            return matcher.match(pipe.getName());
        }

        return pattern.equals(pipe.getName());
    }

    public void setDbName(String dbName) {
        this.dbName = normalizeName(dbName);
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
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowPipeStatement(this, context);
    }
}
