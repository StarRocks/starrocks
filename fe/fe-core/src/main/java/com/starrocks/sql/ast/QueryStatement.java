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

import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.OutFileClause;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.qe.OriginStatement;
import com.starrocks.thrift.TExprOpcode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class QueryStatement extends StatementBase {
    private final QueryRelation queryRelation;

    // represent the "INTO OUTFILE" clause
    protected OutFileClause outFileClause;

    private int queryStartIndex = -1;

    public QueryStatement(QueryRelation queryRelation, OriginStatement originStatement) {
        super(queryRelation.getPos());
        this.queryRelation = queryRelation;
        this.origStmt = originStatement;
    }

    public QueryStatement(QueryRelation queryRelation) {
        super(queryRelation.getPos());
        this.queryRelation = queryRelation;
    }

    public QueryRelation getQueryRelation() {
        return queryRelation;
    }

    public void setOutFileClause(OutFileClause outFileClause) {
        this.outFileClause = outFileClause;
    }

    public OutFileClause getOutFileClause() {
        return outFileClause;
    }

    public boolean hasOutFileClause() {
        return outFileClause != null;
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitQueryStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    public int getQueryStartIndex() {
        return queryStartIndex;
    }

    public void setQueryStartIndex(int idx) {
        this.queryStartIndex = idx;
    }

    // only for prepare execute query
    public boolean isPointQuery() {
        if (queryRelation == null || !(queryRelation instanceof SelectRelation)) {
            return false;
        }

        SelectRelation selectRelation = (SelectRelation) queryRelation;
        if (selectRelation.hasLimit() || selectRelation.hasOffset() || selectRelation.hasHavingClause() ||
                selectRelation.hasAggregation() || selectRelation.hasOrderByClause() ||
                selectRelation.hasWithClause()) {
            return false;
        }

        if (!(selectRelation.getRelation() instanceof TableRelation)) {
            return false;
        }

        if (((TableRelation) selectRelation.getRelation()).getTable().getType() != Table.TableType.OLAP) {
            return false;
        }

        Map<SlotRef, Expr> eqPredicates = new HashMap<>();
        eqPredicates = getEQBinaryPredicates(eqPredicates, selectRelation.getPredicate(), TExprOpcode.EQ);
        if (eqPredicates == null) {
            return false;
        }

        OlapTable olapTable = (OlapTable) ((TableRelation) selectRelation.getRelation()).getTable();
        List<Column> pkColumns = olapTable.getKeyColumns();
        if (pkColumns.size() != eqPredicates.size()) {
            return false;
        }

        for (Column column : pkColumns) {
            SlotRef slotRef = findSlotRef(eqPredicates.keySet(), column.getName());
            if (slotRef == null) {
                return false;
            }
        }

        return true;
    }

    private static Map<SlotRef, Expr> getEQBinaryPredicates(Map<SlotRef, Expr> result, Expr expr,
                                                            TExprOpcode eqOpcode) {
        if (expr == null) {
            return null;
        }
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
            if (compoundPredicate.getOp() != CompoundPredicate.Operator.AND) {
                return null;
            }

            result = getEQBinaryPredicates(result, compoundPredicate.getChild(0), eqOpcode);
            if (result == null) {
                return null;
            }
            result = getEQBinaryPredicates(result, compoundPredicate.getChild(1), eqOpcode);
            if (result == null) {
                return null;
            }
            return result;
        } else if (expr instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
            if (binaryPredicate.getOpcode() != eqOpcode) {
                return null;
            }
            Pair<SlotRef, Expr> slotRefExprPair = binaryPredicate.createSlotAndLiteralPair();
            if (slotRefExprPair == null || result.containsKey(slotRefExprPair.first)) {
                return null;
            }

            result.put(slotRefExprPair.first, slotRefExprPair.second);
            return result;
        } else {
            return null;
        }
    }

    private SlotRef findSlotRef(Set<SlotRef> slotRefs, String colName) {
        for (SlotRef slotRef : slotRefs) {
            if (slotRef.getColumnName().equalsIgnoreCase(colName)) {
                return slotRef;
            }
        }
        return null;
    }
}