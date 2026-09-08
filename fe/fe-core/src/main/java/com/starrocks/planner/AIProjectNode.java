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

package com.starrocks.planner;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.planner.expression.ExprToThrift;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.sql.common.AIModelConfigs.SystemChatConfig;
import com.starrocks.thrift.TAIModelSource;
import com.starrocks.thrift.TAIProjectNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** FE plan boundary for AI expression evaluation. */
public final class AIProjectNode extends ProjectNode {
    private final Map<String, AIModelConfigs.ModelConfig> modelConfigs;

    public AIProjectNode(PlanNodeId id, TupleDescriptor tupleDescriptor, PlanNode child,
                         Map<SlotId, Expr> slotMap, Map<SlotId, Expr> commonSlotMap,
                         SystemChatConfig systemChatConfig) {
        this(id, tupleDescriptor, child, slotMap, commonSlotMap,
                Map.of(AIModelConfigs.SYSTEM_CHAT_CONFIG_ID, AIModelConfigs.fromSystemChat(systemChatConfig)));
    }

    public AIProjectNode(PlanNodeId id, TupleDescriptor tupleDescriptor, PlanNode child,
                         Map<SlotId, Expr> slotMap, Map<SlotId, Expr> commonSlotMap,
                         Map<String, AIModelConfigs.ModelConfig> modelConfigs) {
        this(id, tupleDescriptor, child, copyExpressionMaps(slotMap, commonSlotMap), modelConfigs);
    }

    private AIProjectNode(PlanNodeId id, TupleDescriptor tupleDescriptor, PlanNode child,
                          ExpressionMaps expressionMaps, Map<String, AIModelConfigs.ModelConfig> modelConfigs) {
        super(id, tupleDescriptor, child, expressionMaps.slotMap(), expressionMaps.commonSlotMap());
        planNodeName = "AIProject";

        Preconditions.checkState(Collections.disjoint(getSlotMap().keySet(), getCommonSlotMap().keySet()),
                "AIProject output and common slots must be disjoint");
        Set<SlotId> aiOutputSlots = new HashSet<>();
        List<FunctionCallExpr> aiCalls = new ArrayList<>();
        getSlotMap().forEach((slot, expression) -> {
            if (expression instanceof SlotRef slotRef) {
                Preconditions.checkState(slot.equals(slotRef.getSlotId()),
                        "AIProject pass-through expressions must preserve slot identity");
                return;
            }
            Preconditions.checkState(expression instanceof FunctionCallExpr call
                            && call.getFn() != null && call.getFn().isAi(),
                    "AIProject output expressions must be identity slots or AI calls");
            aiOutputSlots.add(slot);
            int previousAICallCount = aiCalls.size();
            collectAICalls(expression, aiCalls);
            Preconditions.checkState(aiCalls.size() == previousAICallCount + 1,
                    "AIProject output expressions must contain exactly one AI call");
        });
        Preconditions.checkState(!aiCalls.isEmpty(), "AIProject must contain at least one AI call");
        Preconditions.checkState(getCommonSlotMap().values().stream()
                        .noneMatch(AIProjectNode::isNonReusableExpression),
                "AIProject common expressions must be deterministic and non-AI");
        Preconditions.checkState(getCommonSlotMap().values().stream()
                        .noneMatch(expression -> referencesAnySlot(expression, aiOutputSlots)),
                "AIProject common expressions must not depend on AI outputs");
        this.modelConfigs = Map.copyOf(modelConfigs);
        Set<String> usedConfigIds = new HashSet<>();
        for (FunctionCallExpr call : aiCalls) {
            String configId = call.getAiModelConfigId();
            Preconditions.checkState(configId != null && this.modelConfigs.containsKey(configId),
                    "AIProject requires a model configuration for each AI call");
            AIModelConfigs.ModelConfig config = this.modelConfigs.get(configId);
            Preconditions.checkState(config.source() == call.getFn().getAiModelSource(),
                    "AIProject model source does not match the AI function");
            if (config.source() == TAIModelSource.AI_MODEL) {
                Preconditions.checkState(config.modelId() > 0 && config.credentialRef() != null,
                        "AIProject requires a bound model identity and credential reference");
                Preconditions.checkState(!AIModelConfigs.SYSTEM_CHAT_CONFIG_ID.equals(configId)
                                && !AIModelConfigs.SYSTEM_TEXT_EMBEDDING_CONFIG_ID.equals(configId),
                        "Named AI models must not use SYSTEM configuration IDs");
            } else {
                Preconditions.checkState(AIModelConfigs.configId(call.getFn(), call.getChildren()).equals(configId),
                        "AIProject AI call has an invalid SYSTEM configuration ID");
            }
            usedConfigIds.add(configId);
            boolean embedding = AIModelConfigs.isTextEmbedding(call.getFn());
            Preconditions.checkState((embedding ? "TEXT_EMBEDDING" : "CHAT").equals(config.capability()),
                    "AIProject model capability does not match the AI function");
            if (call.getFn().getAiModelSource() == TAIModelSource.SYSTEM) {
                AIModelConfigs.DefaultModelRequirement requirement = AIModelConfigs.hasExplicitModel(call.getFn())
                        ? AIModelConfigs.DefaultModelRequirement.OPTIONAL : AIModelConfigs.DefaultModelRequirement.REQUIRED;
                if (embedding) {
                    AIModelConfigs.validateSystemEmbedding(config, requirement);
                } else {
                    AIModelConfigs.validateSystemChat(
                            new SystemChatConfig(config.endpoint(), config.model(), config.provider()), requirement);
                }
            }
        }
        Preconditions.checkState(usedConfigIds.equals(this.modelConfigs.keySet()),
                "AIProject must contain only used model configurations");
    }

    @Override
    protected void toThrift(TPlanNode message) {
        TAIProjectNode aiProject = new TAIProjectNode();
        aiProject.setSlot_map(new HashMap<>());
        getSlotMap().forEach((slot, expression) ->
                aiProject.putToSlot_map(slot.asInt(), ExprToThrift.treeToThrift(expression)));
        aiProject.setCommon_slot_map(new HashMap<>());
        getCommonSlotMap().forEach((slot, expression) ->
                aiProject.putToCommon_slot_map(slot.asInt(), ExprToThrift.treeToThrift(expression)));
        aiProject.setAi_model_configs(new HashMap<>());
        modelConfigs.forEach((id, config) -> aiProject.putToAi_model_configs(id, config.toThrift()));
        message.setNode_type(TPlanNodeType.AI_PROJECT_NODE);
        message.setAi_project_node(aiProject);
    }

    @Override
    public boolean canPushDownRuntimeFilter() {
        return false;
    }

    @Override
    public Optional<List<Expr>> candidatesOfSlotExpr(
            Expr expression, java.util.function.Function<Expr, Boolean> couldBound) {
        return Optional.empty();
    }

    @Override
    public Optional<List<List<Expr>>> candidatesOfSlotExprs(
            List<Expr> expressions, java.util.function.Function<Expr, Boolean> couldBound) {
        return Optional.empty();
    }

    @Override
    public boolean pushDownRuntimeFilters(RuntimeFilterPushDownContext context,
                                          Expr probeExpression,
                                          List<Expr> partitionByExpressions) {
        return false;
    }

    private static void collectAICalls(Expr expression, List<FunctionCallExpr> calls) {
        if (expression instanceof FunctionCallExpr call && call.getFn() != null && call.getFn().isAi()) {
            calls.add(call);
        }
        expression.getChildren().forEach(child -> collectAICalls(child, calls));
    }

    private static boolean referencesAnySlot(Expr expression, Set<SlotId> slots) {
        if (expression instanceof SlotRef slotRef && slots.contains(slotRef.getSlotId())) {
            return true;
        }
        return expression.getChildren().stream().anyMatch(child -> referencesAnySlot(child, slots));
    }

    private static ExpressionMaps copyExpressionMaps(Map<SlotId, Expr> slotMap,
                                                      Map<SlotId, Expr> commonSlotMap) {
        return new ExpressionMaps(immutableExpressionMap(slotMap), immutableExpressionMap(commonSlotMap));
    }

    private static Map<SlotId, Expr> immutableExpressionMap(Map<SlotId, Expr> expressions) {
        Preconditions.checkNotNull(expressions, "AIProject expression maps must not be null");
        Map<SlotId, Expr> copies = new LinkedHashMap<>();
        expressions.forEach((slot, expression) -> {
            Preconditions.checkNotNull(slot, "AIProject expression slots must not be null");
            Preconditions.checkNotNull(expression, "AIProject expressions must not be null");
            copies.put(slot, expression.clone());
        });
        return Collections.unmodifiableMap(copies);
    }

    private static boolean isNonReusableExpression(Expr expression) {
        if (expression instanceof FunctionCallExpr call && call.getFn() != null) {
            Function function = call.getFn();
            if (function.isAi() || FunctionSet.allNonDeterministicFunctions.contains(
                    function.functionName().toLowerCase(Locale.ROOT))) {
                return true;
            }
        }
        return expression.getChildren().stream().anyMatch(AIProjectNode::isNonReusableExpression);
    }

    private record ExpressionMaps(Map<SlotId, Expr> slotMap, Map<SlotId, Expr> commonSlotMap) {
    }
}
