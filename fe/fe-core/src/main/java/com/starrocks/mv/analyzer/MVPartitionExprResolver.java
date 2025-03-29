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
package com.starrocks.mv.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.common.util.SRStringUtils;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.SemanticException;
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
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.starrocks.mv.analyzer.MVPartitionExpr.FN_NAMES_WITH_FIRST_SLOT;

/**
 * {@link MVPartitionExprResolver} class is responsible for optimizing partition pruning during the refresh of materialized views.
 * <p> Overview </p>
 * It achieves this by analyzing join conditions and efficiently mapping expressions to corresponding slot references.
 * This optimization is crucial for handling large datasets where partition pruning can significantly reduce
 * refresh execution time.
 * <p> Limitation </p>
 * The class includes mechanisms to process various types of expressions and predicates, ensuring
 * compatibility with different SQL functions; only 'date_trunc' function is supported now.
 */
public class MVPartitionExprResolver {
    private static final Logger LOG = LogManager.getLogger(MVPartitionExprResolver.class);

    static class EqTableInfo {
        final List<Expr> eqExprs = Lists.newArrayList();
        public void addEqExprs(Expr expr) {
            eqExprs.add(expr);
        }
    }

    static class Exprs {
        private final List<Expr> exprs;

        public Exprs() {
            exprs = com.google.api.client.util.Lists.newArrayList();
        }

        public Exprs(List<Expr> exprs) {
            this.exprs = exprs;
        }

        public Exprs(Expr expr) {
            this.exprs = ImmutableList.of(expr);
        }

        public static Exprs of(List<Expr> exprs) {
            return new Exprs(exprs);
        }

        public static Exprs of(Expr expr) {
            return new Exprs(expr);
        }

        public void add(Exprs exprs) {
            this.exprs.addAll(exprs.exprs);
        }

        public List<Expr> getExprs() {
            return exprs;
        }

        public int size() {
            if (exprs == null) {
                return 0;
            }
            return exprs.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public Expr get(int i) {
            return exprs.get(i);
        }
    }

    static class MVExprContext {
        private final Expr expr;
        private final Relation relation;
        private final SlotRef slotRef;
        private final Set<Expr> equivalentExprs;
        private final MVExprContext parent;
        private Optional<Set<Expr>> mergedEquivalentExprs = Optional.empty();

        public MVExprContext(Expr paretExpr, Relation relation, SlotRef slotRef, MVExprContext parent) {
            this(paretExpr, relation, slotRef, Sets.newHashSet(), parent);
        }

        public MVExprContext(Expr paretExpr, Relation relation, SlotRef slotRef,
                             Set<Expr> equivalentExprs, MVExprContext parent) {
            this.expr = paretExpr;
            this.relation = relation;
            this.slotRef = slotRef;
            this.equivalentExprs = equivalentExprs;
            this.parent = parent;
        }

        public Relation getRelation() {
            return relation;
        }

        public void addEquivalentExprs(List<Expr> exprs) {
            this.equivalentExprs.addAll(exprs);
        }

        /**
         * Merge parent's exprs and current exprs into equivalent exprs since current and parent exprs may not be ready yet,
         * so merge them by hand.
         * @return
         */
        public Set<Expr> getEquivalentExprs() {
            if (!mergedEquivalentExprs.isPresent()) {
                Set<Expr> equivalentExprs = Sets.newHashSet(this.equivalentExprs);
                equivalentExprs.add(expr);
                MVExprContext parent = this.parent;
                while (parent != null) {
                    equivalentExprs.add(parent.getExpr());
                    parent = parent.getParent();
                }
                mergedEquivalentExprs = Optional.of(equivalentExprs);
            }
            return mergedEquivalentExprs.get();
        }

        public Expr getExpr() {
            return expr;
        }

        public SlotRef getSlotRef() {
            return slotRef;
        }

        public MVExprContext getParent() {
            return parent;
        }

        public MVExprContext withExpr(Expr expr) {
            return new MVExprContext(expr, relation, slotRef, this.equivalentExprs, this);
        }

        public MVExprContext withSlotRef(SlotRef slotRef) {
            return new MVExprContext(expr, relation, slotRef, this.equivalentExprs, this);
        }

        public MVExprContext withRelation(Relation relation) {
            return new MVExprContext(expr, relation, slotRef, this.equivalentExprs, this);
        }
    }

    private  static final AstVisitor<Exprs, MVExprContext> EXPR_SHUTTLE = new AstVisitor<>() {
        @Override
        public Exprs visit(ParseNode node) {
            throw new SemanticException("Cannot resolve materialized view's partition slot ref, " +
                    "statement is not supported:%s", node);
        }

        private Exprs visitRelation(Exprs result, MVExprContext context) {
            if (result != null) {
                context.equivalentExprs.addAll(result.getExprs());
            }
            return result;
        }

        private Exprs visitExpr(MVExprContext context) {
            Exprs result = context.getExpr().accept(this, context);
            if (result != null) {
                context.equivalentExprs.addAll(result.getExprs());
            }
            return result;
        }

        private Exprs visitRelation(MVExprContext context) {
            Exprs result = context.getRelation().accept(SLOT_REF_RESOLVER, context);
            if (result != null) {
                context.equivalentExprs.addAll(result.getExprs());
            }
            return result;
        }

        @Override
        public Exprs visitExpression(Expr expr, MVExprContext context) {
            if (!MVPartitionExpr.isSupportedMVPartitionExpr(expr)) {
                return null;
            }

            FunctionCallExpr funcExpr = (FunctionCallExpr) expr;
            String fnName = funcExpr.getFnName().getFunction();
            int slotRefIdx = FN_NAMES_WITH_FIRST_SLOT.contains(fnName) ? 0 : 1;
            Exprs eqs = visitExpr(context.withExpr(expr.getChild(slotRefIdx)));
            if (eqs == null) {
                return null;
            }
            List<Expr> result = eqs.getExprs().stream()
                    .map(e -> {
                        Expr cloned = expr.clone();
                        cloned.setChild(slotRefIdx, e);
                        return cloned;
                    })
                    .collect(Collectors.toList());
            return visitRelation(Exprs.of(result), context);
        }

        @Override
        public Exprs visitSlot(SlotRef slotRef, MVExprContext context) {
            Relation relation = context.getRelation();
            if (slotRef.getTblNameWithoutAnalyzed() == null) {
                return visitRelation(context.withSlotRef(slotRef));
            }

            String tableName = slotRef.getTblNameWithoutAnalyzed().getTbl();
            if (relation.getAlias() != null && !relation.getAlias().getTbl().equalsIgnoreCase(tableName)) {
                return null;
            }
            return visitRelation(context.withSlotRef(slotRef));
        }

        @Override
        public Exprs visitFieldReference(FieldReference fieldReference, MVExprContext context) {
            Relation relation = context.getRelation();
            Field field = relation.getScope().getRelationFields()
                    .getFieldByIndex(fieldReference.getFieldIndex());
            SlotRef slotRef = new SlotRef(field.getRelationAlias(), field.getName(), field.getName());
            slotRef.setType(field.getType());
            return visitRelation(context.withSlotRef(slotRef));
        }
    };

    private static final AstVisitor<Exprs, MVExprContext> SLOT_REF_RESOLVER = new AstVisitor<Exprs, MVExprContext>() {
        @Override
        public Exprs visit(ParseNode node) {
            throw new SemanticException("Cannot resolve materialized view's partition expression, " +
                    "statement is not supported:%s", node);
        }

        private Exprs visitRelation(MVExprContext context) {
            Exprs result = context.getRelation().accept(this, context);
            if (result != null) {
                context.addEquivalentExprs(result.getExprs());
            }
            return result;
        }

        private Exprs visitExpr(MVExprContext context) {
            Exprs result = context.expr.accept(EXPR_SHUTTLE, context);
            if (result != null) {
                context.addEquivalentExprs(result.getExprs());
            }
            return result;
        }

        @Override
        public Exprs visitSelect(SelectRelation node, MVExprContext context) {
            SlotRef slot = context.getSlotRef();
            Relation relation = node.getRelation();

            for (SelectListItem selectListItem : node.getSelectList().getItems()) {
                TableName expTableName = slot.getTblNameWithoutAnalyzed();
                if (selectListItem.getAlias() == null) {
                    Expr item = selectListItem.getExpr();
                    if (item instanceof SlotRef) {
                        SlotRef result = (SlotRef) item;
                        TableName actTableName = result.getTblNameWithoutAnalyzed();
                        if (result.getColumnName().equalsIgnoreCase(slot.getColumnName())
                                && (expTableName == null || expTableName.equals(actTableName))) {
                            return visitExpr(context.withExpr(result).withRelation(relation));
                        }
                    }
                } else {
                    if (expTableName != null && expTableName.isFullyQualified()) {
                        continue;
                    }
                    if (selectListItem.getAlias().equalsIgnoreCase(slot.getColumnName())) {
                        return visitExpr(context.withExpr(selectListItem.getExpr()).withRelation(relation));
                    }
                }
            }
            return visitRelation(context.withRelation(relation));
        }

        @Override
        public Exprs visitSubqueryRelation(SubqueryRelation node, MVExprContext context) {
            SlotRef slot = context.getSlotRef();
            if (slot.getTblNameWithoutAnalyzed() != null) {
                String tableName = slot.getTblNameWithoutAnalyzed().getTbl();
                if (!node.getAlias().getTbl().equalsIgnoreCase(tableName)) {
                    return null;
                }
                slot = (SlotRef) slot.clone();
                slot.setTblName(null); //clear table name here, not check it inside
            }
            Relation subRelation = node.getQueryStatement().getQueryRelation();
            return visitRelation(context.withSlotRef(slot).withRelation(subRelation));
        }

        @Override
        public Exprs visitTable(TableRelation node, MVExprContext context) {
            SlotRef slot = context.getSlotRef();
            TableName tableName = slot.getTblNameWithoutAnalyzed();
            if (node.getName().equals(tableName)) {
                return Exprs.of(slot);
            }
            if (tableName != null && !node.getResolveTableName().equals(tableName)) {
                return null;
            }
            slot = (SlotRef) slot.clone();
            slot.setTblName(node.getName());
            // add into equivalent exprs
            context.equivalentExprs.add(slot);
            return Exprs.of(slot);
        }

        @Override
        public Exprs visitView(ViewRelation node, MVExprContext context) {
            SlotRef slot = context.getSlotRef();
            TableName tableName = slot.getTblNameWithoutAnalyzed();
            if (tableName != null && !node.getResolveTableName().equals(tableName)) {
                return null;
            }
            List<Field> fields = node.getRelationFields().resolveFields(slot);
            if (fields.isEmpty()) {
                return null;
            }
            Relation newRelation = node.getQueryStatement().getQueryRelation();
            Expr newExpr = fields.get(0).getOriginExpression();
            return visitExpr(context.withRelation(newRelation).withExpr(newExpr));
        }

        @Override
        public Exprs visitJoin(JoinRelation node, MVExprContext context) {
            Relation leftRelation = node.getLeft();
            Relation rightRelation = node.getRight();
            Expr joinOnPredicate = node.getOnPredicate();

            Exprs result = handleJoinChild(context, joinOnPredicate, leftRelation, rightRelation);
            if (result != null) {
                return result;
            }
            return handleJoinChild(context, joinOnPredicate, rightRelation, leftRelation);
        }

        private Exprs handleJoinChild(MVExprContext context, Expr joinOnPredicate,
                                      Relation in, Relation out) {
            MVExprContext inContext = context.withRelation(in);
            Exprs inExprs = visitRelation(inContext);
            // not eager to return since mv support multi ref base tables
            if (inExprs != null) {
                Exprs result = new Exprs();
                result.add(inExprs);
                // merge it from an opposite
                mergeEquivalentExprs(inContext, in, out, joinOnPredicate, result);
                return result;
            }
            return null;
        }

        public void mergeEquivalentExprs(MVExprContext context, Relation in, Relation out,
                                         Expr joinOnPredicate, Exprs result) {
            List<Expr> candidates = getEquivalentExprs(context, in, joinOnPredicate);
            mergeOthers(context, out, candidates, result);
        }

        public void mergeOthers(MVExprContext context, Relation other, List<Expr> candidates, Exprs result) {
            if (candidates == null || candidates.isEmpty()) {
                return;
            }
            candidates.stream().map(e -> visitExpr(context.withExpr(e).withRelation(other)))
                    .filter(e -> e != null)
                    .forEach(result::add);
        }

        private Expr getEquivalentExpr(MVExprContext context,
                                       Relation out,
                                       BinaryPredicate expr) {
            Expr child0 = expr.getChild(0);
            Expr child1 = expr.getChild(1);
            if (areEqualExprs(context, out, child0)) {
                return child1;
            } else if (areEqualExprs(context, out, child1)) {
                return child0;
            } else {
                return null;
            }
        }

        private boolean areEqualExprs(MVExprContext context, Relation in, Expr target) {
            if (target == null) {
                return false;
            }
            // check if the target is in the equivalent exprs
            Set<Expr> equivalentExprs = context.getEquivalentExprs();
            if (equivalentExprs.stream().anyMatch(e -> MVPartitionExprEqChecker.areEqualExprs(e, target))) {
                return true;
            }
            // check after target is resolved
            Exprs resolved = visitExpr(context.withExpr(target).withRelation(in));
            if (resolved == null) {
                return false;
            }
            return resolved.getExprs().stream().anyMatch(t ->
                    equivalentExprs.stream().anyMatch(e -> MVPartitionExprEqChecker.areEqualExprs(e, t)));
        }

        private List<Expr> getEquivalentExprs(MVExprContext context, Relation in, Expr expr) {
            List<Expr> result = Lists.newArrayList();
            if (expr == null) {
                return result;
            }
            if (expr instanceof BinaryPredicate) {
                Expr eq = getEquivalentExpr(context, in, (BinaryPredicate) expr);
                if (eq != null) {
                    result.add(eq);
                }
            } else if (expr instanceof CompoundPredicate) {
                result.addAll(getCompoundPredicate(context, in, (CompoundPredicate) expr));
            }
            return result;
        }

        private List<Expr> getCompoundPredicate(MVExprContext context, Relation in,
                                                CompoundPredicate compoundPredicate) {
            List<Expr> result = Lists.newArrayList();
            if (compoundPredicate.getOp() != CompoundPredicate.Operator.AND) {
                return result;
            }
            for (Expr expr : compoundPredicate.getChildren()) {
                result.addAll(getEquivalentExprs(context, in, expr));
            }
            return result;
        }

        private int findSlotRefIndex(SetOperationRelation setOp, SlotRef slot) {
            Relation firstRelation = setOp.getRelations().get(0);
            return IntStream.range(0, setOp.getOutputExpression().size())
                    .mapToObj(i -> Pair.create(Integer.valueOf(i), firstRelation.getColumnOutputNames().get(i)))
                    .filter(p -> SRStringUtils.areColumnNamesEqual(slot.getColumnName(), p.second))
                    .findFirst()
                    .map(p -> p.first)
                    .orElse(-1);
        }

        @Override
        public Exprs visitSetOp(SetOperationRelation node, MVExprContext context) {
            SlotRef slot = context.getSlotRef();
            int slotRefIdx = findSlotRefIndex(node, slot);
            Exprs result = new Exprs();
            node.getRelations().stream()
                    .map(r -> {
                        if (slotRefIdx != -1) {
                            Expr expr = r.getOutputExpression().get(slotRefIdx);
                            return visitExpr(context.withExpr(expr).withRelation(r));
                        } else {
                            return visitRelation(context.withRelation(r));
                        }
                    })
                    .filter(r -> r != null)
                    .forEach(result::add);
            return result.isEmpty() ? null : result;
        }

        @Override
        public Exprs visitCTE(CTERelation node, MVExprContext context) {
            SlotRef slot = context.getSlotRef();
            if (slot.getTblNameWithoutAnalyzed() != null) {
                String tableName = slot.getTblNameWithoutAnalyzed().getTbl();
                String cteName = node.getAlias() != null ? node.getAlias().getTbl() : node.getName();
                if (!cteName.equalsIgnoreCase(tableName)) {
                    return null;
                }
                slot = (SlotRef) slot.clone();
                slot.setTblName(null); //clear table name here, not check it inside
            }
            Relation relation = node.getCteQueryStatement().getQueryRelation();
            return visitRelation(context.withRelation(relation).withSlotRef(slot));
        }

        @Override
        public Exprs visitValues(ValuesRelation node, MVExprContext slot) {
            return null;
        }
    };

    public static Exprs resolveMVPartitionExpr(Expr expr, QueryStatement queryStatement) {
        final MVExprContext context = new MVExprContext(expr, queryStatement.getQueryRelation(), null, null);
        return expr.accept(EXPR_SHUTTLE, context);
    }

    private static List<MVPartitionExpr> getMVEquivalentPartExpr(Exprs equivalentExprs) {
        return equivalentExprs.getExprs().stream()
                .map(MVPartitionExpr::getSupportMvPartitionExpr)
                .filter(e -> e != null)
                .collect(Collectors.toList());
    }

    /**
     * 1. Ensure the result `partitionExprMaps` is valid, otherwise use the original partition expr.
     * 2. Ensure the result is ordered by mv's defined partition expression orders.
     */
    public static LinkedHashMap<Expr, SlotRef> getMVPartitionExprsChecked(List<Expr> mvRefPartitionExprs,
                                                                          QueryStatement stmt,
                                                                          List<BaseTableInfo> baseTableInfos) {
        LinkedHashMap<Expr, SlotRef> mvPartitionExprMaps = Maps.newLinkedHashMap();
        int refBaseTableCols = -1;
        for (Expr mvRefPartitionExpr : mvRefPartitionExprs) {
            List<MVPartitionExpr> partitionExprMaps = getMVPartitionExprs(mvRefPartitionExpr, stmt);
            if (partitionExprMaps == null) {
                LOG.warn("The partition expr maps slot ref should be from the base table, eqExprs:{}",
                        partitionExprMaps);
                throw new SemanticException("Failed to build mv partition expr from base table: " + stmt.getOrigStmt());
            }
            // If mv contains multi partition columns, needs to ensure each partition column refers the same base tables' count.
            if (refBaseTableCols == -1) {
                refBaseTableCols = partitionExprMaps.size();
            } else if (refBaseTableCols != partitionExprMaps.size()) {
                throw new SemanticException(String.format("The current partition expr maps size %s should be equal to " +
                        "the size of the first partition expr maps: %s", partitionExprMaps.size(), refBaseTableCols));
            }
            partitionExprMaps.stream()
                            .forEach(eq -> mvPartitionExprMaps.put(eq.getExpr(), eq.getSlotRef()));
        }
        if (baseTableInfos != null) {
            Set<List<MVPartitionExpr>> mvPartitionExprs = Sets.newHashSet();
            for (BaseTableInfo baseTableInfo : baseTableInfos) {
                Table table = MvUtils.getTableChecked(baseTableInfo);
                List<MVPartitionExpr> refPartitionExprs = MvUtils.getMvPartitionExpr(mvPartitionExprMaps, table);
                if (!refPartitionExprs.isEmpty()) {
                    mvPartitionExprs.add(refPartitionExprs);
                }
            }
            Preconditions.checkState(mvPartitionExprs.size() <= mvPartitionExprMaps.size(),
                    String.format("The size of mv partition exprs %s should be less or equal to the size of " +
                                    "partition expr maps: %s", mvPartitionExprs, mvPartitionExprMaps));
        }
        return mvPartitionExprMaps;
    }

    /**
     * Resolve the expression through join/sub-query/view/set operator fo find the slot ref
     * which it comes from.
     * Constructs a mapping of partitioned expressions to slot references for join operations in a materialized view.
     */
    private static List<MVPartitionExpr> getMVPartitionExprs(Expr partitionExpr,
                                                             QueryStatement statement) {
        List<MVPartitionExpr> eqExprs = Lists.newArrayList();
        Exprs mvEquivalentExprs = resolveMVPartitionExpr(partitionExpr, statement);
        if (mvEquivalentExprs == null || mvEquivalentExprs.isEmpty()) {
            // no join predicate
            LOG.warn("Build mv equivalent partition expr failed:{}", partitionExpr);
            return eqExprs;
        }

        Map<TableName, EqTableInfo> eqTableInfos = Maps.newHashMap();
        List<MVPartitionExpr> mvEquivalentPartitionExprs = getMVEquivalentPartExpr(mvEquivalentExprs);
        for (MVPartitionExpr eq : mvEquivalentPartitionExprs) {
            SlotRef slotRef = eq.getSlotRef();
            Expr eqExpr = eq.getExpr();
            eqExprs.add(eq);
            eqTableInfos.computeIfAbsent(slotRef.getTblNameWithoutAnalyzed(), (ignored) -> new EqTableInfo())
                    .addEqExprs(eqExpr);
        }

        // 1. To be compatible with the previous version, always add the partition expr to the result
        // 2. Be careful for the partition expr appears more than once
        // For a table appears more than once, we should remove them
        // For example:
        // t1 join a on t1.k1 = a.k1 join a on t1.k1 = date_trunc(a.k1)
        // t1 join a on t1.k1 = a.k1 join a on t1.k1 = a.k2
        // at least the partition expr should be in mvEquivalentExprsMap
        for (Iterator<MVPartitionExpr> iter = eqExprs.iterator(); iter.hasNext();) {
            MVPartitionExpr eq = iter.next();
            TableName tableName = eq.getSlotRef().getTblNameWithoutAnalyzed();
            EqTableInfo eqTableInfo = eqTableInfos.get(tableName);
            if (eqTableInfo == null) {
                continue;
            }
            if (eqTableInfo.eqExprs.size() == 0) {
                iter.remove();
            }
            // only keeps the first one
            if (!eqTableInfo.eqExprs.get(0).equals(eq.getExpr())) {
                iter.remove();
            }
        }
        return eqExprs;
    }
}
