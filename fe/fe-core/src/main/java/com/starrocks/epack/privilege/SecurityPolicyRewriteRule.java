// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SecurityPolicyRewriteRule {
    public static QueryStatement buildView(Relation relation, TableName tableName) {
        SecurityPolicyMgr policyManager = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        if (relation instanceof TableRelation && ((TableRelation) relation).isSyncMVQuery()) {
            return null;
        }

        TableUID tableUID = TableUID.generate(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
        if (!policyManager.hasTableAppliedPolicy(tableUID)) {
            return null;
        }

        List<Column> columns;
        if (relation instanceof ViewRelation) {
            ViewRelation viewRelation = (ViewRelation) relation;
            columns = viewRelation.getView().getBaseSchema();
        } else if (relation instanceof TableRelation) {
            TableRelation tableRelation = (TableRelation) relation;
            columns = tableRelation.getTable().getBaseSchema();
        } else {
            return null;
        }

        List<SelectListItem> selectListItemList = new ArrayList<>();
        Expr rewriteExpr = null;
        PolicyAppliedContext tableAppliedPolicyInfo = policyManager.getTableAppliedPolicyInfo(tableUID);

        if (tableAppliedPolicyInfo.getRowAccessPolicyApply() != null) {
            for (RowAccessPolicyContext rowAccessPolicyInfo : tableAppliedPolicyInfo.getRowAccessPolicyApply()) {
                Policy rowAccessPolicy = policyManager.getPolicyById(rowAccessPolicyInfo.getPolicyId());

                if (!rowAccessPolicyInfo.getOnColumns().isEmpty()) {
                    Map<SlotRef, SlotRef> onColumnsMap = new HashMap<>();
                    List<String> onColumns = rowAccessPolicyInfo.getOnColumns();
                    List<String> argNames = rowAccessPolicy.getArgNames();

                    for (int i = 0; i < rowAccessPolicyInfo.getOnColumns().size(); ++i) {
                        onColumnsMap.put(new SlotRef(null, argNames.get(i)), new SlotRef(tableName, onColumns.get(i)));
                    }

                    RewriteAliasVisitor r = new RewriteAliasVisitor(onColumnsMap);
                    if (rewriteExpr == null) {
                        rewriteExpr = r.visit(rowAccessPolicy.getPolicyExpression().clone());
                    } else {
                        rewriteExpr = Expr.compoundAnd(Lists.newArrayList(
                                r.visit(rowAccessPolicy.getPolicyExpression().clone()), rewriteExpr));
                    }
                } else {
                    rewriteExpr = rowAccessPolicy.getPolicyExpression();
                }
            }
        }

        Map<String, MaskingPolicyContext> maskingPolicyApply = tableAppliedPolicyInfo.getMaskingPolicyApply();
        for (Column column : columns) {
            if (column.getType().isUnknown()) {
                continue;
            }
            MaskingPolicyContext maskingPolicyContext = maskingPolicyApply.get(column.getName());
            if (maskingPolicyContext != null) {
                Policy maskingPolicy = policyManager.getPolicyById(maskingPolicyContext.getPolicyId());
                Map<SlotRef, SlotRef> onColumnsMap = new HashMap<>();
                List<String> usingColumns = maskingPolicyContext.getUsingColumns();
                List<String> argNames = maskingPolicy.getArgNames();

                for (int i = 0; i < maskingPolicyContext.getUsingColumns().size(); ++i) {
                    onColumnsMap.put(new SlotRef(null, argNames.get(i)), new SlotRef(tableName, usingColumns.get(i)));
                }

                RewriteAliasVisitor r = new RewriteAliasVisitor(onColumnsMap);
                Expr project = r.visit(maskingPolicy.getPolicyExpression().clone());
                selectListItemList.add(new SelectListItem(project, column.getName(), NodePosition.ZERO));
            } else {
                selectListItemList.add(
                        new SelectListItem(new SlotRef(tableName, column.getName()), column.getName(), NodePosition.ZERO));
            }
        }

        SelectRelation selectRelation = new SelectRelation(
                new SelectList(selectListItemList, false),
                relation,
                rewriteExpr,
                null,
                null);
        selectRelation.setOrderBy(Collections.emptyList());
        return new QueryStatement(selectRelation);
    }

    private static class RewriteAliasVisitor extends AstVisitor<Expr, Void> {
        Map<SlotRef, SlotRef> map;

        public RewriteAliasVisitor(Map<SlotRef, SlotRef> map) {
            this.map = map;
        }

        @Override
        public Expr visit(ParseNode expr) {
            return visit(expr, null);
        }

        @Override
        public Expr visitExpression(Expr expr, Void context) {
            for (int i = 0; i < expr.getChildren().size(); ++i) {
                expr.setChild(i, visit(expr.getChild(i)));
            }
            return expr;
        }

        @Override
        public Expr visitSlot(SlotRef slotRef, Void context) {
            return map.getOrDefault(slotRef, slotRef);
        }
    }
}
