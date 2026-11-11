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

package com.starrocks.sql.optimizer.base;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.Table;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.DataStreamSink;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ProjectNode;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TPaimonTableSink;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import org.apache.paimon.CoreOptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Fixed buckets have one global writer owner, regardless of the optional connector shuffle policy. */
public final class PaimonSinkShuffleSpec implements ConnectorSinkShuffleSpec {
    private final PaimonTable paimonTable;

    public PaimonSinkShuffleSpec(PaimonTable table) {
        paimonTable = table;
    }

    @Override
    public Table table() {
        return paimonTable;
    }

    @Override
    public boolean shouldShuffle(InsertStmt insertStmt, SessionVariable session) {
        return new CoreOptions(paimonTable.getNativeTable().options()).bucket() > 0;
    }

    @Override
    public PreOptimizeResult prepare(InsertStmt insertStmt, List<ColumnRefOperator> outputColumns,
                                     ColumnRefFactory columnRefFactory, OptExpression root,
                                     ColumnRefSet requiredColumns) {
        // A SQL hash of bucket keys is NOT Paimon's bucket calculator. The mandatory native
        // calculation and exchange are installed after physical planning, including LIMIT/VALUES.
        return new PreOptimizeResult(root, requiredColumns, Collections.emptyList());
    }

    /**
     * Install the final write distribution, after the target's frozen descriptor has been captured.
     * Returns the actual native bucket slot, or -1 for an append-unaware table.
     * An internal SDK-backed scalar computes the bucket, which is never a target-schema field.
     */
    public static int installBucketRouting(ExecPlan plan, TPaimonTableSink sink) {
        if (!sink.isSetBucket_count()) {
            throw new SemanticException("Paimon write contract is missing its bucket layout");
        }
        if (sink.getBucket_count() == -1) {
            if (sink.isSetPrimary_key_columns() && !sink.getPrimary_key_columns().isEmpty()) {
                throw new SemanticException("Paimon primary-key writes require fixed buckets");
            }
            return -1;
        }
        if (sink.getBucket_count() <= 0) {
            throw new SemanticException("Paimon writes require a fixed or append-unaware bucket layout");
        }
        if (!plan.getConnectContext().getSessionVariable().isEnablePipelineEngine()) {
            throw new SemanticException("Paimon bucket routing requires the pipeline engine");
        }
        List<Expr> sourceExprs = new ArrayList<>(plan.getOutputExprs());
        if (!sink.isSetPartition_columns() || !sink.isSetColumn_names()
                || sourceExprs.size() != sink.getColumn_namesSize() || sourceExprs.isEmpty()) {
            throw new SemanticException("Paimon bucket routing requires every target schema column in native order");
        }
        if (plan.getFragments().isEmpty()) {
            throw new SemanticException("Paimon bucket routing requires an input fragment");
        }

        DescriptorTable descriptors = plan.getDescTbl();
        // Optimizer slots have explicit IDs; DescriptorTable's auto-ID cursor need not follow them.
        int nextSlot = descriptors.getTupleDescs().stream().flatMap(t -> t.getSlots().stream())
                .mapToInt(s -> s.getId().asInt()).max().orElse(-1) + 1;
        TupleDescriptor tuple = descriptors.createTupleDescriptor("Paimon bucket routing");
        Map<SlotId, Expr> projection = new LinkedHashMap<>();
        List<Expr> schemaExprs = new ArrayList<>();
        for (int i = 0; i < sourceExprs.size(); ++i) {
            Expr expression = sourceExprs.get(i);
            SlotDescriptor slot = descriptors.addSlotDescriptor(tuple, new SlotId(nextSlot++));
            slot.setType(expression.getType());
            slot.setIsNullable(expression.isNullable());
            slot.setIsMaterialized(true);
            slot.setIsOutputColumn(true);
            slot.setLabel(sink.getColumn_names().get(i));
            projection.put(slot.getId(), expression);
            schemaExprs.add(new SlotRef(slot));
        }
        tuple.computeMemLayout();
        PlanFragment input = plan.getTopFragment();
        ProjectNode materialize = new ProjectNode(plan.getNextNodeId(), tuple,
                input.getPlanRoot(), projection, Collections.emptyMap());
        TupleDescriptor routedTuple = descriptors.createTupleDescriptor("Paimon bucket function");
        Map<SlotId, Expr> routedProjection = new LinkedHashMap<>();
        List<Expr> routedSchemaExprs = new ArrayList<>();
        for (int i = 0; i < schemaExprs.size(); ++i) {
            Expr expression = schemaExprs.get(i);
            SlotDescriptor slot = descriptors.addSlotDescriptor(routedTuple, new SlotId(nextSlot++));
            slot.setType(expression.getType());
            slot.setIsNullable(expression.isNullable());
            slot.setIsMaterialized(true);
            slot.setIsOutputColumn(true);
            routedProjection.put(slot.getId(), expression);
            routedSchemaExprs.add(new SlotRef(slot));
        }
        SlotDescriptor bucket = descriptors.addSlotDescriptor(routedTuple, new SlotId(nextSlot));
        bucket.setType(IntegerType.INT);
        bucket.setIsNullable(false);
        bucket.setIsMaterialized(true);
        bucket.setIsOutputColumn(true);
        bucket.setLabel("__paimon_native_bucket");
        routedProjection.put(bucket.getId(), bucketFunction(sink, schemaExprs));
        routedTuple.computeMemLayout();
        schemaExprs = routedSchemaExprs;

        List<Expr> routeKeys = new ArrayList<>();
        for (String name : sink.getPartition_columns()) {
            int index = sink.getColumn_names().indexOf(name);
            if (index < 0) {
                throw new SemanticException("Paimon partition column is absent from the frozen target schema");
            }
            routeKeys.add(schemaExprs.get(index));
        }
        routeKeys.add(new SlotRef(bucket));
        DataPartition partition = DataPartition.hashPartitioned(routeKeys);
        ProjectNode routing = new ProjectNode(plan.getNextNodeId(), routedTuple, materialize,
                routedProjection, Collections.emptyMap());
        input.setPlanRoot(routing);
        input.disableRuntimeAdaptiveDop();
        ExchangeNode exchange = new ExchangeNode(plan.getNextNodeId(), routing, partition);
        PlanFragment output = new PlanFragment(plan.getNextFragmentId(), exchange, partition);
        input.setDestination(exchange);
        input.setOutputPartition(partition);
        DataStreamSink stream = new DataStreamSink(exchange.getId());
        stream.setPartition(partition);
        input.setSink(stream);
        List<Expr> transportExprs = new ArrayList<>(schemaExprs);
        transportExprs.add(new SlotRef(bucket));
        input.setOutputExprs(transportExprs);
        output.setOutputExprs(schemaExprs);
        output.setHasPaimonTableSink();
        plan.getFragments().add(0, output);
        plan.getOutputExprs().clear();
        plan.getOutputExprs().addAll(schemaExprs);
        // A physical write-only operator is not part of the optimizer/query-cache digest.
        plan.getFragments().forEach(fragment -> fragment.setCacheParam(null));
        return bucket.getId().asInt();
    }
    private static Expr bucketFunction(TPaimonTableSink sink, List<Expr> schemaExprs) {
        if (sink.getSchema_json() == null || sink.getSchema_json().length() >= 1024 * 1024) {
            throw new SemanticException("Invalid Paimon bucket schema configuration size");
        }
        JsonObject config = new JsonObject();
        config.addProperty("version", 1);
        config.addProperty("bucket_count", sink.getBucket_count());
        config.addProperty("is_pk", !sink.getPrimary_key_columns().isEmpty());
        config.addProperty("algorithm", sink.getOptions().getOrDefault("bucket-function", "default"));
        config.addProperty("time_zone", sink.getTime_zone());
        config.addProperty("memory_limit", sink.getOptions().getOrDefault(
                "starrocks.paimon.max-writer-memory-bytes", "268435456"));
        JsonArray nativeFields;
        try {
            nativeFields = JsonParser.parseString(sink.getSchema_json()).getAsJsonObject().getAsJsonArray("fields");
            if (nativeFields == null || nativeFields.size() != schemaExprs.size()) {
                throw new IllegalArgumentException();
            }
        } catch (RuntimeException e) {
            throw new SemanticException("Invalid Paimon bucket schema configuration");
        }
        JsonArray keyFields = new JsonArray();
        List<Expr> keys = new ArrayList<>();
        ArrayList<StructField> fields = new ArrayList<>();
        for (String name : sink.getBucket_columns()) {
            int index = sink.getColumn_names().indexOf(name);
            if (index < 0) {
                throw new SemanticException("Paimon bucket key is absent from the frozen schema");
            }
            JsonObject field = new JsonObject();
            field.addProperty("name", name);
            field.add("type", nativeFields.get(index).getAsJsonObject().get("type").deepCopy());
            keyFields.add(field);
            keys.add(schemaExprs.get(index));
            fields.add(new StructField(name, schemaExprs.get(index).getType()));
        }
        config.add("fields", keyFields);
        // Empty key rows hash to a constant bucket; Project expands the constant result to the chunk size.
        if (keys.isEmpty()) {
            keys.add(new IntLiteral(0, IntegerType.INT));
            fields.add(new StructField("__row_count", IntegerType.INT));
        }
        StructType keyType = new StructType(fields);
        FunctionCallExpr row = new FunctionCallExpr("row", keys);
        ScalarFunction rowFn = ScalarFunction.createBuiltin("row", "StructFunctions::new_struct",
                new ArrayList<>(keys.stream().map(Expr::getType).toList()), false, keyType, false);
        rowFn.setFunctionId(170500);
        rowFn.setIsNullable(false);
        row.setFn(rowFn);
        row.setType(keyType);
        String json = config.toString();
        if (json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 1024 * 1024) {
            throw new SemanticException("Paimon bucket function configuration exceeds 1 MiB");
        }
        StringLiteral configuration = new StringLiteral(json);
        FunctionCallExpr call = new FunctionCallExpr("__paimon_bucket", List.of(configuration, row));
        ScalarFunction fn = ScalarFunction.createBuiltin("__paimon_bucket", "PaimonFunctions::bucket",
                new ArrayList<>(List.of(configuration.getType(), keyType)), false, IntegerType.INT, false);
        fn.setFunctionId(10360);
        fn.setIsNullable(false);
        call.setFn(fn);
        call.setType(IntegerType.INT);
        return call;
    }

}
