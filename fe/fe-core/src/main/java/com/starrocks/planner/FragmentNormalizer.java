package com.starrocks.planner;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.analysis.BetweenPredicate;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.IdGenerator;
import com.starrocks.common.Pair;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TExpr;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TSimpleJSONProtocol;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

public class FragmentNormalizer {
    private ExecPlan execPlan;
    private PlanFragment fragment;
    private Map<PlanNodeId, PlanNodeId> planNodeIdRemapping = Maps.newHashMap();
    private Map<SlotId, SlotId> slotIdRemapping = Maps.newHashMap();
    private Map<TupleId, TupleId> tupleIdRemapping = Maps.newHashMap();
    private IdGenerator<PlanNodeId> planNodeIdGen = PlanNodeId.createGenerator();
    private IdGenerator<TupleId> tupleIdIdGen = TupleId.createGenerator();
    private IdGenerator<SlotId> slotIdGen = SlotId.createGenerator();
    private List<TNormalPlanNode> normalizedPlanNodes = Lists.newArrayList();
    private Map<Long, String> selectedRangeMap = Maps.newHashMap();
    private boolean uncacheable = false;

    public FragmentNormalizer(ExecPlan execPlan, PlanFragment fragment) {
        this.execPlan = execPlan;
        this.fragment = fragment;
    }

    public static Range<PartitionKey> toClosedOpenRange(Range<PartitionKey> range) {
        PartitionKey lowerBound = range.lowerEndpoint();
        PartitionKey upperBound = range.upperEndpoint();
        if (!lowerBound.isMinValue() && !range.contains(lowerBound)) {
            lowerBound = lowerBound.successor();
        }

        if (!upperBound.isMaxValue() && range.contains(upperBound)) {
            upperBound = upperBound.successor();
        }
        return Range.closedOpen(lowerBound, upperBound);
    }

    public boolean isUncacheable() {
        return uncacheable;
    }

    public void setUncacheable(boolean uncacheable) {
        this.uncacheable = uncacheable;
    }

    private static String toHexString(byte[] bytes) {
        StringBuffer s = new StringBuffer(bytes.length * 2);
        char[] d = "0123456789abcdef".toCharArray();
        for (byte a : bytes) {
            s.append(d[(a >>> 4) & 0xf]);
            s.append(d[a & 0xf]);
        }
        return s.toString();
    }

    public ExecPlan getExecPlan() {
        return execPlan;
    }

    public List<Integer> remapTupleIds(List<TupleId> ids) {
        return ids.stream().map(id -> remapTupleId(id).asInt()).collect(Collectors.toList());
    }

    public PlanNodeId remapPlanNodeId(PlanNodeId planNodeId) {
        return planNodeIdRemapping.computeIfAbsent(planNodeId, arg -> planNodeIdGen.getNextId());
    }

    public SlotId remapSlotId(SlotId slotId) {
        return slotIdRemapping.computeIfAbsent(slotId, arg -> slotIdGen.getNextId());
    }

    public List<Integer> remapSlotIds(List<SlotId> slotIds) {
        return slotIds.stream().map(this::remapSlotId).map(SlotId::asInt).collect(Collectors.toList());
    }

    public boolean containsAllSlotIds(List<SlotId> slotIds) {
        return slotIds.stream().allMatch(slotIdRemapping::containsKey);
    }

    public TupleId remapTupleId(TupleId tupleId) {
        return tupleIdRemapping.computeIfAbsent(tupleId, arg -> tupleIdIdGen.getNextId());
    }

    public ByteBuffer normalizeExpr(Expr expr) {
        uncacheable = uncacheable || hasNonDeterministicFunctions(expr);
        TExpr texpr = expr.normalize(this);
        TSerializer ser = new TSerializer(new TCompactProtocol.Factory());
        try {
            return ByteBuffer.wrap(ser.serialize(texpr));
        } catch (Exception ignored) {
            Preconditions.checkArgument(false);
        }
        return null;
    }

    public Pair<List<Integer>, List<ByteBuffer>> normalizeSlotIdsAndExprs(Map<SlotId, Expr> exprMap) {
        List<Pair<SlotId, ByteBuffer>> slotIdsAndStringFunctions = exprMap.entrySet().stream()
                .map(e -> new Pair<>(e.getKey(), normalizeExpr(e.getValue())))
                .sorted(Pair.comparingBySecond()).collect(Collectors.toList());
        List<SlotId> slotIds = slotIdsAndStringFunctions.stream().map(e -> e.first).collect(Collectors.toList());
        List<ByteBuffer> exprs = slotIdsAndStringFunctions.stream().map(e -> e.second).collect(Collectors.toList());
        return new Pair<>(remapSlotIds(slotIds), exprs);
    }

    public List<ByteBuffer> normalizeExprs(List<Expr> exprList) {
        if (exprList == null || exprList.isEmpty()) {
            return Collections.emptyList();
        }
        return exprList.stream().map(this::normalizeExpr).sorted(ByteBuffer::compareTo).collect(Collectors.toList());
    }

    boolean isNormalizable(PlanNode node) {
        Preconditions.checkArgument(node != null);
        return node instanceof OlapScanNode ||
                node instanceof ProjectNode ||
                node instanceof SelectNode ||
                node instanceof AggregationNode ||
                node instanceof DecodeNode;
    }

    public void normalize() {
        try {
            PlanNode topmostPlanNode = findMaximumNormalizableSubTree(fragment.getPlanRoot());
            if (!(topmostPlanNode instanceof AggregationNode) || selectedRangeMap.isEmpty()) {
                return;
            }
            TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
            MessageDigest digest = MessageDigest.getInstance("SHA-256");

            for (TNormalPlanNode node : normalizedPlanNodes) {
                byte[] data = serializer.serialize(node);
                digest.update(data);
            }
            List<SlotId> slotIds = topmostPlanNode.getOutputSlotIds(execPlan.getDescTbl());
            List<Integer> remappedSlotIds = remapSlotIds(slotIds);
            Map<Integer, Integer> outputSlotIdRemapping = Maps.newHashMap();
            for (int i = 0; i < slotIds.size(); ++i) {
                outputSlotIdRemapping.put(slotIds.get(i).asInt(), remappedSlotIds.get(i));
            }
            fragment.setCachePlanNodeId(topmostPlanNode.getId());
            fragment.setDigest(ByteBuffer.wrap(digest.digest()));
            fragment.setSlotRemapping(outputSlotIdRemapping);
            fragment.setRangeMap(selectedRangeMap);
        } catch (TException | NoSuchAlgorithmException ignored) {
        }
    }

    public PlanNode findMaximumNormalizableSubTree(PlanNode node) {
        boolean allNormalized = true;
        PlanNode leftMostTree = null;
        for (PlanNode child : node.getChildren()) {
            PlanNode subtree = findMaximumNormalizableSubTree(child);
            // return quickly if the deep-most aggregation node is visited
            if (subtree instanceof AggregationNode) {
                return subtree;
            }

            if (subtree == null) {
                allNormalized = false;
                continue;
            }
            if (subtree != child) {
                return child;
            }
            if (leftMostTree == null) {
                leftMostTree = subtree;
            }
        }

        if (!allNormalized || uncacheable) {
            return null;
        }

        if (!isNormalizable(node)) {
            return leftMostTree;
        } else {
            // ignore trivial ProjectNode
            if ((node instanceof ProjectNode) && ((ProjectNode) node).isTrivial()) {
                return node;
            }
            TNormalPlanNode canonNode = node.normalize(this);
            if (uncacheable) {
                return null;
            }
            normalizedPlanNodes.add(canonNode);
            return node;
        }
    }

    List<Expr> flatAndPredicate(Expr conjunct) {
        if (!(conjunct instanceof CompoundPredicate)) {
            return Arrays.asList(conjunct);
        }
        CompoundPredicate compoundPredicate = (CompoundPredicate) conjunct;
        if (compoundPredicate.getOp() != CompoundPredicate.Operator.AND) {
            return Arrays.asList(conjunct);
        } else {
            return compoundPredicate.getChildren().stream()
                    .flatMap(child -> flatAndPredicate(child).stream()).collect(Collectors.toList());
        }
    }

    boolean isSimpleRegionPredicate(Expr expr) {

        if (!(expr instanceof BetweenPredicate) && !(expr instanceof BinaryPredicate)) {
            return false;
        }
        boolean simple = expr.getChild(0) instanceof SlotRef &&
                expr.getChildren().subList(1, expr.getChildren().size())
                        .stream().allMatch(e -> (e instanceof LiteralExpr) && !(e instanceof NullLiteral));
        if (!simple) {
            return false;
        }
        if (expr instanceof BetweenPredicate) {
            return !((BetweenPredicate) expr).isNotBetween();
        }
        if (expr instanceof BinaryPredicate) {
            return ((BinaryPredicate) expr).getOp() != BinaryPredicate.Operator.EQ_FOR_NULL;
        }
        return true;
    }

    boolean hasNonDeterministicFunctions(Expr expr) {
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr callExpr = (FunctionCallExpr) expr;
            if (FunctionSet.nonDeterministicFunctions.contains(callExpr.getFn().functionName())) {
                return true;
            }
        }
        return expr.getChildren().stream().anyMatch(e -> hasNonDeterministicFunctions(e));
    }

    List<Range<PartitionKey>> convertPredicateToRange(Column partitionColumn, Expr expr) {
        List<Range<PartitionKey>> result = Lists.newArrayList();
        PartitionKey minKey = null;
        PartitionKey maxKey = null;
        try {
            minKey = PartitionKey.createInfinityPartitionKey(Arrays.asList(partitionColumn), false);
            maxKey = PartitionKey.createInfinityPartitionKey(Arrays.asList(partitionColumn), true);
        } catch (AnalysisException ignored) {
        }
        Preconditions.checkArgument(minKey != null && maxKey != null);
        if (expr instanceof BinaryPredicate) {
            BinaryPredicate predicate = (BinaryPredicate) expr;
            if (predicate.getOp() == BinaryPredicate.Operator.EQ_FOR_NULL) {
                return result;
            }
            LiteralExpr rhs = (LiteralExpr) predicate.getChild(1);
            PartitionKey rhsKey = new PartitionKey();
            rhsKey.pushColumn(rhs, partitionColumn.getPrimitiveType());
            switch (predicate.getOp()) {
                case EQ:
                    result.add(Range.closed(rhsKey, rhsKey));
                    break;
                case NE:
                    result.add(Range.open(minKey, rhsKey));
                    result.add(Range.open(rhsKey, maxKey));
                    break;
                case LE:
                    result.add(Range.openClosed(minKey, rhsKey));
                    break;
                case GE:
                    result.add(Range.closedOpen(rhsKey, maxKey));
                    break;
                case LT:
                    result.add(Range.open(minKey, rhsKey));
                    break;
                case GT:
                    result.add(Range.open(rhsKey, maxKey));
                    break;
                case EQ_FOR_NULL:
                    break;
            }
            return result;
        } else if (expr instanceof BetweenPredicate) {
            BetweenPredicate predicate = (BetweenPredicate) expr;
            LiteralExpr lowerBound = (LiteralExpr) expr.getChild(1);
            LiteralExpr upperBound = (LiteralExpr) expr.getChild(2);
            PartitionKey lowerKey = new PartitionKey();
            PartitionKey upperKey = new PartitionKey();
            lowerKey.pushColumn(lowerBound, partitionColumn.getPrimitiveType());
            lowerKey.pushColumn(upperBound, partitionColumn.getPrimitiveType());
            if (predicate.isNotBetween()) {
                result.add(Range.open(minKey, lowerKey));
                result.add(Range.open(upperKey, upperKey));
            } else {
                result.add(Range.closed(lowerKey, upperKey));
            }
            return result;
        } else if (expr instanceof InPredicate) {
            InPredicate predicate = (InPredicate) expr;
            for (Expr elem : predicate.getListChildren()) {
                LiteralExpr literal = (LiteralExpr) elem;
                PartitionKey key = new PartitionKey();
                key.pushColumn(literal, partitionColumn.getPrimitiveType());
                if (predicate.isNotIn()) {
                    result.add(Range.open(minKey, key));
                    result.add(Range.open(key, maxKey));
                } else {
                    result.add(Range.closed(key, key));
                }
            }
            return result;
        } else {
            return Lists.newArrayList();
        }
    }

    List<Expr> getPartitionRangePredicates(List<Expr> conjuncts,
                                           List<Map.Entry<Long, Range<PartitionKey>>> rangeMap,
                                           RangePartitionInfo partitionInfo,
                                           SlotId partitionSlotId) {

        List<Expr> exprs = conjuncts.stream().flatMap(e -> flatAndPredicate(e).stream()).collect(Collectors.toList());
        List<Expr> unboundExprs = Lists.newArrayList();
        List<Expr> boundSimpleRegionExprs = Lists.newArrayList();
        List<Expr> boundOtherExprs = Lists.newArrayList();
        for (Expr e : exprs) {
            if (!e.isBound(partitionSlotId)) {
                unboundExprs.add(e);
                continue;
            }
            if (isSimpleRegionPredicate(e)) {
                boundSimpleRegionExprs.add(e);
            } else {
                boundOtherExprs.add(e);
            }
        }

        // TODO(by satanson): If the bound exprs contain no simple range exprs but only contain complex exprs, then
        //  cached result is reused poorly. so we mark the fragment uncacheable. As a matter of fact, some complex
        //  exprs can also be decomposed into simple range exprs, later we all extends the scope of simple range exprs.
        if (!boundOtherExprs.isEmpty() && boundSimpleRegionExprs.isEmpty()) {
             uncacheable = true;
             return conjuncts;
        }

        if (boundSimpleRegionExprs.isEmpty()) {
            for (Map.Entry<Long, Range<PartitionKey>> range : rangeMap) {
                selectedRangeMap.put(range.getKey(), range.getValue().toString());
            }
            return conjuncts;
        }

        Column partitionColumn = partitionInfo.getPartitionColumns().get(0);
        List<Range<PartitionKey>> partitionRanges = rangeMap.stream()
                .map(Map.Entry::getValue).collect(Collectors.toList());

        // compute the intersection region of partition range and region predicates
        for (Expr expr : boundSimpleRegionExprs) {
            List<Range<PartitionKey>> ranges = convertPredicateToRange(partitionColumn, expr);
            if (ranges.isEmpty()) {
                continue;
            }
            for (Range<PartitionKey> r : ranges) {
                partitionRanges = partitionRanges.stream().filter(pr ->
                        pr.isConnected(r)).map(pr -> pr.intersection(r)).collect(Collectors.toList());
            }
        }
        // select the partition ranges should be cached
        for (int i = 0; i < partitionRanges.size(); ++i) {
            Range<PartitionKey> range = partitionRanges.get(i);
            if (range.isEmpty()) {
                continue;
            }
            range = toClosedOpenRange(range);
            Map.Entry<Long, Range<PartitionKey>> partitionKeyRange = rangeMap.get(i);
            // when the range is to total cover this partition, we also cache it
            if (!range.isEmpty()) {
                selectedRangeMap.put(partitionKeyRange.getKey(), range.toString());
            }
        }
        if (selectedRangeMap.isEmpty()) {
            uncacheable = true;
            return conjuncts;
        } else {
            List<Expr> remainConjuncts = Lists.newArrayList();
            remainConjuncts.addAll(unboundExprs);
            remainConjuncts.addAll(boundOtherExprs);
            return remainConjuncts;
        }
    }
}
