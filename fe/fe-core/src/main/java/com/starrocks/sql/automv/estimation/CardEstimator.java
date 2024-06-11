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

package com.starrocks.sql.automv.estimation;

import com.google.api.client.util.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.automv.column.ColumnAlias;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.generator.QueryGenerateContext;
import com.starrocks.sql.automv.generator.QueryGenerateResult;
import com.starrocks.sql.automv.generator.QueryGenerator;
import com.starrocks.sql.automv.lattice.Lattice;
import com.starrocks.sql.automv.lattice.LatticeNode;
import com.starrocks.sql.automv.lattice.LatticeNodeId;
import com.starrocks.sql.automv.options.AutoMVOptions;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.TablePiece;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.pn.Var;
import com.starrocks.sql.automv.qe.CustomizedQueryExecutor;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.automv.util.Util;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import org.apache.hadoop.util.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CardEstimator {

    private final AutoMVOptions options;
    private final Lattice lattice;

    private final List<LatticeNode> bfsNodes;
    private final List<Integer> dimensionColumnIds;

    private final List<Pair<Var, Integer>> dimensionalVarAndTableIds;

    private final Supplier<Optional<List<Op>>> nextSamplingConjuncts;
    private final Supplier<Optional<Double>> nextSamplingRatio;

    private final Supplier<String> nextCteName;
    private final List<CardRecord> estimatedCards;
    private long prevAccTimeUsage;
    private double currSamplingRatio;
    private long currAccTimeUsage;
    private transient List<Long> allZerosCards = null;

    public CardEstimator(AutoMVOptions options, Lattice lattice) {
        this.options = Objects.requireNonNull(options);
        this.lattice = Objects.requireNonNull(lattice);
        //TODO(by satanson): the number of multi-column cardinality estimators is equivalent
        // to the number of the Lattice's LatticeNodes, we should limit the number of estimators
        // to avoid generating time-consuming sampling query.
        this.bfsNodes = lattice.bfs();
        this.dimensionColumnIds = lattice.getDimensionColumnIds();
        List<Integer> dimensionColumnFreqs = Lists.newArrayListWithCapacity(this.dimensionColumnIds.size());
        for (int i = 0; i < this.dimensionColumnIds.size(); ++i) {
            dimensionColumnFreqs.add(0);
        }

        bfsNodes.forEach(node -> {
            node.getId().getColumnOrdinals().forEach(columnOrdinal ->
                    dimensionColumnFreqs.set(columnOrdinal, dimensionColumnFreqs.get(columnOrdinal) + 1));
        });

        List<Pair<Var, Integer>> varFreqs =
                collectUsedOriginalVars(lattice.getFlatTable(), this.dimensionColumnIds, dimensionColumnFreqs);

        this.dimensionalVarAndTableIds = pickupVarsForSampleConjunct(lattice.getFlatTable(), varFreqs, 3);
        this.nextCteName = Util.nextStringGenerator("cte_", "");
        List<Var> conjunctVars = this.dimensionalVarAndTableIds.stream().map(p -> p.first).collect(Collectors.toList());

        if (conjunctVars.isEmpty()) {
            this.nextSamplingConjuncts = Util.onePoint(Collections.emptyList());
            this.nextSamplingRatio = Util.onePoint(1.0);
        } else {
            this.nextSamplingConjuncts =
                    OpUtil.getSamplingConjunctsGenerator(conjunctVars, options.getSamplingBuckets());
            this.nextSamplingRatio = OpUtil.getSamplingRatio(conjunctVars, options.getSamplingBuckets());
        }
        this.prevAccTimeUsage = 0;
        this.currAccTimeUsage = 0;
        this.estimatedCards = this.bfsNodes.stream()
                .map(LatticeNode::getId).map(CardRecord::new).collect(Collectors.toList());
    }

    private static List<Pair<Var, Integer>> pickupVarsForSampleConjunct(PlanPiece piece,
                                                                        List<Pair<Var, Integer>> varFreqs,
                                                                        int numVars) {
        varFreqs.sort(Collections.reverseOrder(Comparator.comparing(p -> p.second)));
        List<Var> chosenVars = varFreqs.subList(0, Math.min(numVars, varFreqs.size()))
                .stream().map(p -> p.first).collect(Collectors.toList());
        List<TablePiece> tablePieces = PlanPiece.collect(piece, TablePiece.class);

        Function<Integer, Integer> lookupTablePieceId = id -> tablePieces.stream()
                .filter(tablePiece -> tablePiece.getColumns().containsKey(id))
                .map(tablePiece -> tablePiece.getAuxState().getId())
                .findFirst().orElse(null);

        return chosenVars.stream()
                .map(v -> Pair.create(v, Objects.requireNonNull(lookupTablePieceId.apply(v.getId()))))
                .collect(Collectors.toList());
    }

    private static List<Pair<Var, Integer>> collectUsedOriginalVars(PlanPiece piece, List<Integer> columnIds,
                                                                    List<Integer> columnFreqs) {
        Map<Integer, Integer> freqs = Maps.newHashMap();
        IntStream.range(0, columnIds.size()).forEach(i -> freqs.put(columnIds.get(i), columnFreqs.get(i)));
        ColumnRefSet requiredColumnIds = ColumnRefSet.createByIds(columnIds);
        Map<Integer, Var> vars = Maps.newHashMap();
        collectUsedOriginalVars(piece, requiredColumnIds, freqs, vars);
        return vars.entrySet().stream()
                .map(e -> Pair.create(e.getValue(), freqs.get(e.getKey())))
                .collect(Collectors.toList());
    }

    private static void collectUsedOriginalVars(PlanPiece piece, ColumnRefSet requiredColumnIds,
                                                Map<Integer, Integer> freqs,
                                                Map<Integer, Var> usedVars) {
        Map<Boolean, TieredMap<Integer, GenericColumn>> columnGroups = piece.getColumns().entrySet()
                .stream()
                .filter(e -> requiredColumnIds.contains(e.getKey()))
                .collect(Collectors.partitioningBy(e -> e.getValue().isOriginal(), TieredMap.toMap()));

        TieredMap<Integer, GenericColumn> requiredOriginalColumns = columnGroups.get(true);
        TieredMap<Integer, GenericColumn> requiredDerivedColumns = columnGroups.get(false);

        for (Map.Entry<Integer, GenericColumn> e : requiredOriginalColumns.entrySet()) {
            Var var = OpUtil.columnToOp(e.getKey(), e.getValue()).cast();
            usedVars.putIfAbsent(e.getKey(), var);
        }

        ColumnRefSet allUsedColumnIds = ColumnRefSet.of();
        TieredMap<Integer, GenericColumn> originalColumns = piece.getColumns().entrySet()
                .stream().filter(e -> e.getValue().isOriginal())
                .collect(TieredMap.toMap());
        ColumnRefSet originalColumnIds = ColumnRefSet.createByIds(originalColumns.keySet());
        for (Map.Entry<Integer, GenericColumn> e : requiredDerivedColumns.entrySet()) {
            Integer freq = freqs.get(e.getKey());
            ColumnRefSet usedColumns = e.getValue().getUsedColumns().orElseGet(ColumnRefSet::of);
            allUsedColumnIds.union(usedColumns);
            usedColumns.getStream().forEach(cid -> {
                freqs.merge(cid, freq, Integer::sum);
                if (originalColumnIds.contains(cid)) {
                    Var var = OpUtil.columnToOp(cid, originalColumns.get(cid)).cast();
                    usedVars.putIfAbsent(cid, var);
                }
            });
        }

        for (PlanPiece child : piece.getInputPieces()) {
            collectUsedOriginalVars(child, allUsedColumnIds, freqs, usedVars);
        }
    }

    public Map<LatticeNodeId, CardRecord> getEstimatedCards() {
        return estimatedCards.stream().collect(ImmutableMap.toImmutableMap(CardRecord::getId, Function.identity()));
    }

    // reviseFlatTable is used to generate sampling conjuncts, then modifies the PlanPiece
    // by add the each sampling conjunct to the corresponding TablePiece that has the
    // OriginalColumn used by the sampling conjunct.
    public Optional<PlanPiece> reviseFlatTable() {
        PlanPiece flatTable = lattice.getFlatTable();
        Optional<List<Op>> optConjuncts;
        do {
            optConjuncts = this.nextSamplingConjuncts.get();
            if (!optConjuncts.isPresent()) {
                return Optional.empty();
            }
            currSamplingRatio = this.nextSamplingRatio.get().orElse(1.00);
        } while (currSamplingRatio < options.getSamplingRatioLowBound());

        List<Op> conjuncts = optConjuncts.get();
        Preconditions.checkArgument(conjuncts.size() == this.dimensionalVarAndTableIds.size());

        Map<Integer, TieredList<Op>> tableIdToConjuncts =
                IntStream.range(0, conjuncts.size())
                        .mapToObj(i -> Pair.create(this.dimensionalVarAndTableIds.get(i).second, conjuncts.get(i)))
                        .collect(Collectors.groupingBy(p -> p.first,
                                Collectors.mapping(p -> p.second, TieredList.<Op>toList())));

        ColumnRefSet requiredColumnIds = ColumnRefSet.createByIds(this.dimensionColumnIds);
        flatTable.getConjuncts().forEach(conjunct -> requiredColumnIds.union(conjunct.getIds()));
        Map<Boolean, List<Pair<Integer, GenericColumn>>> columnGroups = requiredColumnIds.getStream()
                .map(cId -> Pair.create(cId, flatTable.getColumns().get(cId)))
                .collect(Collectors.partitioningBy(p -> p.second.isOriginal()));

        List<Pair<Integer, GenericColumn>> derivedColumnList = columnGroups.get(false);
        List<Pair<Integer, GenericColumn>> originalColumnList = columnGroups.get(true);
        TieredMap<Integer, GenericColumn> originalColumns = originalColumnList
                .stream().collect(TieredMap.toMap(p -> p.first, p -> p.second));
        ColumnRefSet originalColumnIds = ColumnRefSet.createByIds(originalColumns.keySet());
        ColumnRefSet extraColumnIds = ColumnRefSet.of();
        derivedColumnList.forEach(p -> p.second.getUsedColumns().ifPresent(extraColumnIds::union));
        Optional.ofNullable(tableIdToConjuncts.get(flatTable.getAuxState().getId()))
                .ifPresent(flatTableConjuncts -> flatTableConjuncts.forEach(op -> extraColumnIds.union(op.getIds())));
        extraColumnIds.except(originalColumnIds);

        TieredMap<Integer, GenericColumn> extraOriginalColumns = flatTable.getColumns().entrySet()
                .stream().filter(e -> extraColumnIds.contains(e.getKey()))
                .collect(TieredMap.toMap());

        TieredMap<Integer, GenericColumn> derivedColumns =
                derivedColumnList.stream().collect(TieredMap.toMap(p -> p.first, p -> p.second));

        TieredMap<Integer, GenericColumn> columns = derivedColumns.merge(extraOriginalColumns).merge(originalColumns);

        Function<PlanPiece, PlanPiece> revisor = piece ->
                Optional.ofNullable(tableIdToConjuncts.get(piece.getAuxState().getId()))
                        .map(piece::setConjuncts)
                        .orElse(piece.builder().build());
        PlanPiece revisedFlatTable = flatTable.revise(revisor).setColumns(columns);
        return Optional.of(revisedFlatTable);
    }

    public Optional<String> getEstimateSql() {
        Preconditions.checkArgument(!bfsNodes.isEmpty());

        Optional<PlanPiece> revisedFlatTable = reviseFlatTable();
        if (!revisedFlatTable.isPresent()) {
            return Optional.empty();
        }

        String cteName = this.nextCteName.get();

        QueryGenerateContext context = QueryGenerateContext.of(false, false);
        QueryGenerateResult result = QueryGenerator.generate(revisedFlatTable.get(), context);
        Map<Integer, ColumnAlias> columnAliases = result.getColumnAliases();

        Supplier<Integer> nextDefaultValue = Util.nextIdGenerator(1);
        Supplier<String> nextColName = Util.nextStringGenerator("c", "");
        List<String> cteColumnNames = Lists.newArrayListWithCapacity(this.dimensionColumnIds.size());
        this.dimensionColumnIds.forEach(cId -> cteColumnNames.add(nextColName.get()));
        Iterator<String> colNameIter = cteColumnNames.iterator();
        List<PrettyPrinter> items = this.dimensionColumnIds.stream()
                .map(columnAliases::get)
                .map(ColumnAlias::getQualifiedName)
                .map(name -> new PrettyPrinter()
                        .add("coalesce(murmur_hash3_32(").add(name).add("), ")
                        .add(nextDefaultValue.get())
                        .add(") AS ").add(colNameIter.next()))
                .collect(Collectors.toList());

        PrettyPrinter cteSqlMaker = new PrettyPrinter();
        cteSqlMaker.add("SELECT").newLine();
        cteSqlMaker.indentEnclose(() -> {
            if (items.isEmpty()) {
                cteSqlMaker.add(1);
            } else {
                cteSqlMaker.addSuperStepsWithNlDel(",", items);
            }
        });
        if (result.getTableAlias() == null) {
            cteSqlMaker.newLine().add("FROM").newLine();
            cteSqlMaker.indentEnclose(() -> {
                cteSqlMaker.addSuperStepWithIndent(result.getSubquery());
            });
        } else {
            cteSqlMaker.newLine().add("FROM (").newLine();
            cteSqlMaker.indentEnclose(() -> {
                cteSqlMaker.addSuperStepWithIndent(result.getSubquery());
            });
            cteSqlMaker.newLine().add(") ").add(result.getTableAlias());
        }
        PrettyPrinter sqlMaker = new PrettyPrinter();
        sqlMaker.add("WITH ").add(cteName).add(" AS (").newLine();
        sqlMaker.indentEnclose(() -> {
            sqlMaker.addSuperStepWithIndent(cteSqlMaker);
        });
        sqlMaker.newLine().add(")").newLine();

        PrettyPrinter rowCount = new PrettyPrinter().add("COUNT(1) as rowCount");

        TieredList<PrettyPrinter> ndvCards = bfsNodes.stream()
                .map(node -> cardEstimation(node.getId().getColumnOrdinals(), cteColumnNames))
                .collect(TieredList.toList());

        PrettyPrinter jsonArrayCards = new PrettyPrinter();
        jsonArrayCards.add("json_array(").newLine();
        jsonArrayCards.indentEnclose(() -> {
            jsonArrayCards.addSuperStepsWithNlDel(",", ndvCards);
        });
        jsonArrayCards.newLine().add(") AS cards");

        List<PrettyPrinter> selectItems = Arrays.asList(rowCount, jsonArrayCards);
        sqlMaker.add("SELECT").newLine();
        sqlMaker.indentEnclose(() -> {
            sqlMaker.addSuperStepsWithNlDel(",", selectItems);
        });
        sqlMaker.newLine().add("FROM ").add(cteName);
        return Optional.of(sqlMaker.getResult());
    }

    private PrettyPrinter cardEstimation(List<Integer> columnOrdinals, List<String> names) {
        if (columnOrdinals.isEmpty()) {
            return new PrettyPrinter().add(1);
        }
        PrettyPrinter hashMaker = new PrettyPrinter();
        hashMaker.add(names.get(columnOrdinals.get(0)));
        for (int i = 1; i < columnOrdinals.size(); ++i) {
            String name = names.get(columnOrdinals.get(i));
            PrettyPrinter newHashMaker = new PrettyPrinter();
            newHashMaker.add("(").addSuperStep(hashMaker).add(")*31+").add(name);
            hashMaker = newHashMaker;
        }
        PrettyPrinter ndvMaker = new PrettyPrinter();
        ndvMaker.add("ndv(").addSuperStep(hashMaker).add(")");
        return ndvMaker;
    }

    private List<Long> getAllZerosCards() {
        if (allZerosCards == null) {
            List<Long> cards = new ArrayList<>(estimatedCards.size());
            for (int i = 0; i < estimatedCards.size(); ++i) {
                cards.add(0L);
            }
            allZerosCards = cards;
        }
        return allZerosCards;
    }

    private Optional<MultiColumnCards> computeCards(ConnectContext ctx) {
        Optional<String> sql = getEstimateSql();
        if (!sql.isPresent()) {
            return Optional.empty();
        }
        CustomizedQueryExecutor executor = new CustomizedQueryExecutor();
        long startMs = System.currentTimeMillis();
        List<MultiColumnCards> mcCards =
                executor.query(MultiColumnCards.class, MultiColumnCards.getColumns(), ctx, sql.get());
        long timeUsage = System.currentTimeMillis() - startMs;

        if (mcCards.size() == 0) {
            MultiColumnCards result = new MultiColumnCards();
            result.setCards(getAllZerosCards());
            result.setTimeUsage(timeUsage);
            return Optional.of(result);
        }
        // CustomizedQueryExecutor::query always return a list of tuples.
        // the sampling query is a non-group-by aggregation query, so it is
        // always return single row.
        Preconditions.checkArgument(mcCards.size() == 1);
        MultiColumnCards result = mcCards.get(0);
        result.setTimeUsage(timeUsage);
        return Optional.of(result);
    }

    private void mergeResult(MultiColumnCards mcCards) {
        this.prevAccTimeUsage = this.currAccTimeUsage;
        this.currAccTimeUsage = this.currAccTimeUsage + mcCards.getTimeUsage();

        Preconditions.checkArgument(mcCards.getCards().size() == estimatedCards.size());
        long rowCount = mcCards.getRowCount();
        for (int i = 0; i < mcCards.getCards().size(); ++i) {
            estimatedCards.get(i).updateCard(rowCount, mcCards.getCards().get(i), options.getMinSamplingRows(),
                    options.getRelativeErrorBound());
        }
    }

    private boolean isConvergent() {
        if (currAccTimeUsage >= options.getSamplingTimeout()) {
            return true;
        }
        return estimatedCards.stream().allMatch(CardRecord::isConvergent);
    }

    public CardEstimateState converge(ConnectContext ctx) {
        int calcSteps = 0;
        while (true) {
            ++calcSteps;
            Optional<MultiColumnCards> mcCards = Optional.empty();
            try {
                mcCards = computeCards(ctx);
            } catch (Throwable ex) {
                ex.printStackTrace();
                return new CardEstimateState(options, CardEstimateState.HaltReason.ERROR, currAccTimeUsage,
                        currSamplingRatio, calcSteps);
            }
            if (!mcCards.isPresent()) {
                return new CardEstimateState(options, CardEstimateState.HaltReason.OVERALL, currAccTimeUsage,
                        currSamplingRatio, calcSteps);
            }
            mergeResult(mcCards.get());
            if (isConvergent()) {
                return new CardEstimateState(options, CardEstimateState.HaltReason.CONVERGENT, currAccTimeUsage,
                        currSamplingRatio, calcSteps);
            }
            if (currAccTimeUsage >= options.getSamplingTimeout()) {
                return new CardEstimateState(options, CardEstimateState.HaltReason.TIMEOUT, currAccTimeUsage,
                        currSamplingRatio, calcSteps);
            }
            if (calcSteps >= options.getMaxCalculateSteps()) {
                return new CardEstimateState(options, CardEstimateState.HaltReason.REACH_LIMIT, currAccTimeUsage,
                        currSamplingRatio, calcSteps);
            }

        }
    }
}
