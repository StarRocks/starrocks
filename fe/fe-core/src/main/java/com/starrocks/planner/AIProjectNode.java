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
import com.starrocks.thrift.TAIEndpointConfig;
import com.starrocks.thrift.TAIModelConfiguration;
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
    private final SystemChatConfig systemChatConfig;

    public AIProjectNode(PlanNodeId id, TupleDescriptor tupleDescriptor, PlanNode child,
                         Map<SlotId, Expr> slotMap, Map<SlotId, Expr> commonSlotMap,
                         SystemChatConfig systemChatConfig) {
        this(id, tupleDescriptor, child, copyExpressionMaps(slotMap, commonSlotMap), systemChatConfig);
    }

    private AIProjectNode(PlanNodeId id, TupleDescriptor tupleDescriptor, PlanNode child,
                          ExpressionMaps expressionMaps, SystemChatConfig systemChatConfig) {
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
        Preconditions.checkState(aiCalls.stream().allMatch(call ->
                        AIModelConfigs.SYSTEM_CHAT_CONFIG_ID.equals(call.getAiModelConfigId())),
                "AIProject AI calls must use the SYSTEM model configuration");
        Preconditions.checkState(getCommonSlotMap().values().stream()
                        .noneMatch(AIProjectNode::isNonReusableExpression),
                "AIProject common expressions must be deterministic and non-AI");
        Preconditions.checkState(getCommonSlotMap().values().stream()
                        .noneMatch(expression -> referencesAnySlot(expression, aiOutputSlots)),
                "AIProject common expressions must not depend on AI outputs");
        boolean requiresDefaultModel = aiCalls.stream().anyMatch(AIProjectNode::requiresDefaultModel);
        SystemChatConfig checkedSystemChatConfig = Preconditions.checkNotNull(systemChatConfig,
                "AIProject requires a SYSTEM model configuration");
        AIModelConfigs.validateSystemChat(checkedSystemChatConfig, requiresDefaultModel
                ? AIModelConfigs.DefaultModelRequirement.REQUIRED
                : AIModelConfigs.DefaultModelRequirement.OPTIONAL);
        this.systemChatConfig = checkedSystemChatConfig;
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
        aiProject.setAi_model_configs(Map.of(
                AIModelConfigs.SYSTEM_CHAT_CONFIG_ID, toThrift(systemChatConfig)));
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

    private static boolean requiresDefaultModel(FunctionCallExpr call) {
        Function function = call.getFn();
        int semanticArity = function.getNumArgs();
        boolean hasOptions = semanticArity > 1 && function.getArgs()[semanticArity - 1].isMapType();
        return semanticArity - (hasOptions ? 1 : 0) == 1;
    }

    private static TAIModelConfiguration toThrift(SystemChatConfig config) {
        TAIEndpointConfig chat = new TAIEndpointConfig();
        chat.setEndpoint(config.endpoint());
        chat.setModel(config.model());
        chat.setProvider(config.provider());
        TAIModelConfiguration configuration = new TAIModelConfiguration();
        configuration.setChat(chat);
        return configuration;
    }

    private record ExpressionMaps(Map<SlotId, Expr> slotMap, Map<SlotId, Expr> commonSlotMap) {
    }
}
