// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authorization;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.NativeAccessController;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstRewriter;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.SqlParser;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NativeAccessControllerEPack extends NativeAccessController implements AccessControllerEPack {
    @Override
    public void checkPolicyAction(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                  String db, String policy, PrivilegeType privilegeType) throws AccessDeniedException {
        List<String> objectTokens = Lists.newArrayList(catalogName, db, policy);
        ObjectType objectType = policyType.equals(PolicyType.MASKING) ? ObjectTypeEPack.MASKING_POLICY :
                ObjectTypeEPack.ROW_ACCESS_POLICY;
        checkObjectTypeAction(currentUser, roleIds, privilegeType, objectType, objectTokens);
    }

    @Override
    public void checkAnyActionOnPolicy(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                       String db, String policy) throws AccessDeniedException {
        List<String> objectTokens = Lists.newArrayList(catalogName, db, policy);
        ObjectType objectType = policyType.equals(PolicyType.MASKING) ? ObjectTypeEPack.MASKING_POLICY :
                ObjectTypeEPack.ROW_ACCESS_POLICY;
        checkAnyActionOnObject(currentUser, roleIds, objectType, objectTokens);
    }

    @Override
    public void checkAnyActionOnAnyPolicy(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                          String db) throws AccessDeniedException {
        checkAnyActionOnPolicy(currentUser, roleIds, policyType, catalogName, db, "*");
    }

    @Override
    public void checkWarehouseAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        checkObjectTypeAction(currentUser, roleIds, privilegeType, ObjectTypeEPack.WAREHOUSE,
                Collections.singletonList(name));
    }

    @Override
    public void checkAnyActionOnWarehouse(UserIdentity currentUser, Set<Long> roleIds, String name) throws AccessDeniedException {
        checkAnyActionOnObject(currentUser, roleIds, ObjectTypeEPack.WAREHOUSE, Collections.singletonList(name));
    }

    @Override
    public Map<String, Expr> getColumnMaskingPolicy(ConnectContext context, TableName tableName, List<Column> columns) {
        SecurityPolicyMgr policyManager = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        TableUID tableUID = TableUID.generate(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
        if (!policyManager.hasTableAppliedPolicy(tableUID)) {
            return null;
        }

        PolicyAppliedContext tableAppliedPolicyInfo = policyManager.getTableAppliedPolicyInfo(tableUID);
        Map<String, MaskingPolicyContext> maskingPolicyApply = tableAppliedPolicyInfo.getMaskingPolicyApply();

        Map<String, Expr> maskingExprMap = new HashMap<>();
        for (Column column : columns) {
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
                Expr policyExpr = SqlParser.parseSqlToExpr(maskingPolicy.getPolicyExpressionSQL(),
                        context.getSessionVariable().getSqlMode());

                maskingExprMap.put(column.getName(), (Expr) r.visit(policyExpr));
            }
        }
        return maskingExprMap;
    }

    @Override
    public Expr getRowAccessPolicy(ConnectContext context, TableName tableName) {
        SecurityPolicyMgr policyManager = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        TableUID tableUID = TableUID.generate(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
        if (!policyManager.hasTableAppliedPolicy(tableUID)) {
            return null;
        }

        PolicyAppliedContext tableAppliedPolicyInfo = policyManager.getTableAppliedPolicyInfo(tableUID);

        Expr rewriteExpr = null;
        for (RowAccessPolicyContext rowAccessPolicyInfo : tableAppliedPolicyInfo.getRowAccessPolicyApply()) {
            Policy rowAccessPolicy = policyManager.getPolicyById(rowAccessPolicyInfo.getPolicyId());
            Expr policyExpr = SqlParser.parseSqlToExpr(rowAccessPolicy.getPolicyExpressionSQL(),
                    context.getSessionVariable().getSqlMode());

            if (!rowAccessPolicyInfo.getOnColumns().isEmpty()) {
                Map<SlotRef, SlotRef> onColumnsMap = new HashMap<>();
                List<String> onColumns = rowAccessPolicyInfo.getOnColumns();
                List<String> argNames = rowAccessPolicy.getArgNames();

                for (int i = 0; i < rowAccessPolicyInfo.getOnColumns().size(); ++i) {
                    onColumnsMap.put(new SlotRef(null, argNames.get(i), argNames.get(i)),
                            new SlotRef(tableName, onColumns.get(i)));
                }

                RewriteAliasVisitor r = new RewriteAliasVisitor(onColumnsMap);

                if (rewriteExpr == null) {
                    rewriteExpr = (Expr) r.visit(policyExpr);
                } else {
                    rewriteExpr = Expr.compoundAnd(Lists.newArrayList((Expr) r.visit(policyExpr), rewriteExpr));
                }
            } else {
                rewriteExpr = policyExpr;
            }
        }
        return rewriteExpr;
    }

    private static class RewriteAliasVisitor extends AstRewriter<Void> {
        Map<SlotRef, SlotRef> map;

        public RewriteAliasVisitor(Map<SlotRef, SlotRef> map) {
            this.map = map;
        }

        @Override
        public ParseNode visit(ParseNode node) {
            return visit(node, null);
        }

        @Override
        public ParseNode visitExpression(Expr expr, Void context) {
            for (int i = 0; i < expr.getChildren().size(); ++i) {
                expr.setChild(i, (Expr) visit(expr.getChild(i)));
            }
            return expr;
        }

        @Override
        public ParseNode visitSlot(SlotRef slotRef, Void context) {
            return map.getOrDefault(slotRef, slotRef);
        }
    }
}
