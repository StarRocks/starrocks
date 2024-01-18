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


package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.common.concurrent.locks.LockType;
import com.starrocks.common.concurrent.locks.Locker;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.proc.LakeTabletsProcDir;
import com.starrocks.common.proc.LocalTabletsProcDir;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.ShowTabletStmt;

import java.util.ArrayList;
import java.util.List;

public class ShowTabletStmtAnalyzer {

    public static void analyze(ShowTabletStmt statement, ConnectContext context) {
        new ShowTabletStmtAnalyzerVisitor().visit(statement, context);
    }

    static class ShowTabletStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        private long version = -1;
        private long backendId = -1;
        private String indexName = null;
        private Replica.ReplicaState replicaState = null;
        private ArrayList<OrderByPair> orderByPairs = null;

        public void analyze(ShowTabletStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitShowTabletStatement(ShowTabletStmt statement, ConnectContext context) {
            String dbName = statement.getDbName();
            boolean isShowSingleTablet = statement.isShowSingleTablet();
            if (!isShowSingleTablet && Strings.isNullOrEmpty(dbName)) {
                dbName = context.getDatabase();
            }
            statement.setDbName(dbName);

            // partitionNames.
            PartitionNames partitionNames = statement.getPartitionNames();
            if (partitionNames != null) {
                // check if partition name is not empty string
                if (partitionNames.getPartitionNames().stream().anyMatch(entity -> Strings.isNullOrEmpty(entity))) {
                    throw new SemanticException("there are empty partition name");
                }
            }
            // analyze where clause if not null
            Expr whereClause = statement.getWhereClause();
            if (whereClause != null) {
                if (whereClause instanceof CompoundPredicate) {
                    CompoundPredicate cp = (CompoundPredicate) whereClause;
                    if (cp.getOp() != com.starrocks.analysis.CompoundPredicate.Operator.AND) {
                        throw new SemanticException("Only allow compound predicate with operator AND");
                    }
                    analyzeSubPredicate(cp.getChild(0));
                    analyzeSubPredicate(cp.getChild(1));
                } else {
                    analyzeSubPredicate(whereClause);
                }
            }
            // order by
            List<OrderByElement> orderByElements = statement.getOrderByElements();
            if (orderByElements != null && !orderByElements.isEmpty()) {
                Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
                if (db == null) {
                    throw new SemanticException("Database %s is not found", dbName);
                }
                String tableName = statement.getTableName();
                Table table = null;
                Locker locker = new Locker();
                locker.lockDatabase(db, LockType.READ);
                try {
                    table = db.getTable(tableName);
                    if (table == null) {
                        throw new SemanticException("Table %s is not found", tableName);
                    }
                } finally {
                    locker.unLockDatabase(db, LockType.READ);
                }

                orderByPairs = new ArrayList<>();
                for (OrderByElement orderByElement : orderByElements) {
                    if (!(orderByElement.getExpr() instanceof SlotRef)) {
                        throw new SemanticException("Should order by column");
                    }
                    SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                    int index = 0;
                    try {
                        if (table.isCloudNativeTableOrMaterializedView()) {
                            index = LakeTabletsProcDir.analyzeColumn(slotRef.getColumnName());
                        } else {
                            index = LocalTabletsProcDir.analyzeColumn(slotRef.getColumnName());
                        }
                    } catch (AnalysisException e) {
                        throw new SemanticException(e.getMessage());
                    }
                    OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                    orderByPairs.add(orderByPair);
                }
            }

            // Set the statement.
            statement.setVersion(version);
            statement.setIndexName(indexName);
            statement.setReplicaState(replicaState);
            statement.setBackendId(backendId);
            statement.setOrderByPairs(orderByPairs);
            return null;
        }

        private void analyzeSubPredicate(Expr subExpr) {
            if (subExpr == null) {
                return;
            }
            if (subExpr instanceof CompoundPredicate) {
                CompoundPredicate cp = (CompoundPredicate) subExpr;
                if (cp.getOp() != com.starrocks.analysis.CompoundPredicate.Operator.AND) {
                    throw new SemanticException("Only allow compound predicate with operator AND");
                }

                analyzeSubPredicate(cp.getChild(0));
                analyzeSubPredicate(cp.getChild(1));
                return;
            }
            boolean valid = true;
            do {
                if (!(subExpr instanceof BinaryPredicate)) {
                    valid = false;
                    break;
                }
                BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
                if (binaryPredicate.getOp() != BinaryType.EQ) {
                    valid = false;
                    break;
                }

                if (!(subExpr.getChild(0) instanceof SlotRef)) {
                    valid = false;
                    break;
                }
                String leftKey = ((SlotRef) subExpr.getChild(0)).getColumnName();
                if (leftKey.equalsIgnoreCase("version")) {
                    if (!(subExpr.getChild(1) instanceof IntLiteral) || version > -1) {
                        valid = false;
                        break;
                    }
                    version = ((IntLiteral) subExpr.getChild(1)).getValue();
                } else if (leftKey.equalsIgnoreCase("backendid")) {
                    if (!(subExpr.getChild(1) instanceof IntLiteral) || backendId > -1) {
                        valid = false;
                        break;
                    }
                    backendId = ((IntLiteral) subExpr.getChild(1)).getValue();
                } else if (leftKey.equalsIgnoreCase("indexname")) {
                    if (!(subExpr.getChild(1) instanceof StringLiteral) || indexName != null) {
                        valid = false;
                        break;
                    }
                    indexName = ((StringLiteral) subExpr.getChild(1)).getValue();
                } else if (leftKey.equalsIgnoreCase("state")) {
                    if (!(subExpr.getChild(1) instanceof StringLiteral) || replicaState != null) {
                        valid = false;
                        break;
                    }
                    String state = ((StringLiteral) subExpr.getChild(1)).getValue().toUpperCase();
                    try {
                        replicaState = Replica.ReplicaState.valueOf(state);
                    } catch (Exception e) {
                        replicaState = null;
                        valid = false;
                        break;
                    }
                } else {
                    valid = false;
                    break;
                }
            } while (false);

            if (!valid) {
                throw new SemanticException("Where clause should looks like: Version = \"version\","
                        + " or state = \"NORMAL|ROLLUP|CLONE|DECOMMISSION\", or BackendId = 10000,"
                        + " indexname=\"rollup_name\" or compound predicate with operator AND");
            }
        }
    }

}
