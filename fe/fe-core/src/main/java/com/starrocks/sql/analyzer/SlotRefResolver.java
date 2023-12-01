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

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.ViewRelation;

/**
 * Resolve the expression(eg: partition column expression) through join/sub-query/view/set operator fo find the slot ref
 * which it comes from.
 */
public class SlotRefResolver {
    private  static final AstVisitor<Expr, Relation> EXPR_SHUTTLE = new AstVisitor<Expr, Relation>() {
        @Override
        public Expr visit(ParseNode node) {
            throw new SemanticException("Cannot resolve materialized view's partition slot ref, " +
                    "statement is not supported:%s", node);
        }

        @Override
        public Expr visitExpression(Expr expr, Relation node) {
            expr = expr.clone();
            for (int i = 0; i < expr.getChildren().size(); i++) {
                Expr child = expr.getChild(i);
                expr.setChild(i, child.accept(this, node));
            }
            return expr;
        }

        @Override
        public Expr visitSlot(SlotRef slotRef, Relation node) {
            String tableName = slotRef.getTblNameWithoutAnalyzed().getTbl();
            if (node.getAlias() != null && !node.getAlias().getTbl().equalsIgnoreCase(tableName)) {
                return slotRef;
            }
            return node.accept(SLOT_REF_RESOLVER, slotRef);
        }

        @Override
        public Expr visitFieldReference(FieldReference fieldReference, Relation node) {
            Field field = node.getScope().getRelationFields()
                    .getFieldByIndex(fieldReference.getFieldIndex());
            SlotRef slotRef = new SlotRef(field.getRelationAlias(), field.getName());
            return node.accept(SLOT_REF_RESOLVER, slotRef);
        }
    };

    private static final AstVisitor<Expr, SlotRef> SLOT_REF_RESOLVER = new AstVisitor<Expr, SlotRef>() {
        @Override
        public Expr visit(ParseNode node) {
            throw new SemanticException("Cannot resolve materialized view's partition expression, " +
                    "statement is not supported:%s", node);
        }

        @Override
        public Expr visitSelect(SelectRelation node, SlotRef slot) {
            for (SelectListItem selectListItem : node.getSelectList().getItems()) {
                TableName tableName = slot.getTblNameWithoutAnalyzed();
                if (selectListItem.getAlias() == null) {
                    if (selectListItem.getExpr() instanceof SlotRef) {
                        SlotRef result = (SlotRef) selectListItem.getExpr();
                        if (result.getColumnName().equalsIgnoreCase(slot.getColumnName())
                                && (tableName == null || tableName.equals(result.getTblNameWithoutAnalyzed()))) {
                            return selectListItem.getExpr().accept(EXPR_SHUTTLE, node.getRelation());
                        }
                    }
                } else {
                    if (tableName != null && tableName.isFullyQualified()) {
                        continue;
                    }
                    if (selectListItem.getAlias().equalsIgnoreCase(slot.getColumnName())) {
                        return selectListItem.getExpr().accept(EXPR_SHUTTLE, node.getRelation());
                    }
                }
            }
            return node.getRelation().accept(this, slot);
        }

        @Override
        public Expr visitSubquery(SubqueryRelation node, SlotRef slot) {
            if (slot.getTblNameWithoutAnalyzed() != null) {
                String tableName = slot.getTblNameWithoutAnalyzed().getTbl();
                if (!node.getAlias().getTbl().equalsIgnoreCase(tableName)) {
                    return null;
                }
                slot = (SlotRef) slot.clone();
                slot.setTblName(null); //clear table name here, not check it inside
            }
            return node.getQueryStatement().getQueryRelation().accept(this, slot);
        }

        @Override
        public Expr visitTable(TableRelation node, SlotRef slot) {
            TableName tableName = slot.getTblNameWithoutAnalyzed();
            if (node.getName().equals(tableName)) {
                return slot;
            }
            if (tableName != null && !node.getResolveTableName().equals(tableName)) {
                return null;
            }
            slot = (SlotRef) slot.clone();
            slot.setTblName(node.getName());
            return slot;
        }

        @Override
        public Expr visitView(ViewRelation node, SlotRef slot) {
            TableName tableName = slot.getTblNameWithoutAnalyzed();
            if (tableName != null && !node.getResolveTableName().equals(tableName)) {
                return null;
            }
            slot = (SlotRef) slot.clone();
            slot.setTblName(null); //clear table name here, not check it inside
            return node.getQueryStatement().getQueryRelation().accept(this, slot);
        }

        @Override
        public Expr visitJoin(JoinRelation node, SlotRef slot) {
            Relation leftRelation = node.getLeft();
            Expr leftExpr = leftRelation.accept(this, slot);
            if (leftExpr != null) {
                return leftExpr;
            }
            Relation rightRelation = node.getRight();
            Expr rightExpr = rightRelation.accept(this, slot);
            if (rightExpr != null) {
                return rightExpr;
            }
            return null;
        }

        @Override
        public Expr visitSetOp(SetOperationRelation node, SlotRef slot) {
            for (Relation relation : node.getRelations()) {
                Expr resolved = relation.accept(this, slot);
                if (resolved != null) {
                    return resolved;
                }
            }
            return null;
        }

        @Override
        public Expr visitCTE(CTERelation node, SlotRef slot) {
            if (slot.getTblNameWithoutAnalyzed() != null) {
                String tableName = slot.getTblNameWithoutAnalyzed().getTbl();
                String cteName = node.getAlias() != null ? node.getAlias().getTbl() : node.getName();
                if (!cteName.equalsIgnoreCase(tableName)) {
                    return null;
                }
                slot = (SlotRef) slot.clone();
                slot.setTblName(null); //clear table name here, not check it inside
            }
            return node.getCteQueryStatement().getQueryRelation().accept(this, slot);
        }

        @Override
        public SlotRef visitValues(ValuesRelation node, SlotRef slot) {
            return null;
        }
    };

    /**
     * Resolve the expression through join/sub-query/view/set operator fo find the slot ref
     * which it comes from.
     */
    public static Expr resolveExpr(Expr expr, QueryStatement queryStatement) {
        return expr.accept(EXPR_SHUTTLE, queryStatement.getQueryRelation());
    }
}