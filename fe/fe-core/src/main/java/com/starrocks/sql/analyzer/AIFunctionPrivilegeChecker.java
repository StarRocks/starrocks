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

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Function;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.thrift.TAIModelSource;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** AI execution rights always belong to the caller, including calls expanded from any view security mode. */
final class AIFunctionPrivilegeChecker {
    private AIFunctionPrivilegeChecker() {
    }

    static void check(StatementBase statement, ConnectContext context) {
        if (context.isBypassAuthorizerCheck()) {
            return;
        }
        List<FunctionCallExpr> calls = ResolvedAIFunctionDetector.findAll(statement);
        if (calls.isEmpty()) {
            return;
        }
        Set<String> families = new LinkedHashSet<>();
        Set<String> providers = new LinkedHashSet<>();
        for (FunctionCallExpr call : calls) {
            Function function = call.getFn();
            String family = GlobalStateMgr.getCurrentState().getGrantableAIFunctionFamily(function.functionName());
            if (family == null) {
                reportAccessDenied(context, "AI FUNCTION", function.functionName());
            }
            if (families.add(family)) {
                try {
                    Authorizer.checkAIFunctionAction(context, family, PrivilegeType.USAGE);
                } catch (AccessDeniedException e) {
                    reportAccessDenied(context, "AI FUNCTION", family);
                }
            }
            if (function.getAiModelSource() == TAIModelSource.PROVIDER) {
                providers.add(AIModelConfigs.providerName(call.getChild(AIModelConfigs.getProviderArgument(function))));
            }
        }
        for (String name : providers) {
            AIProvider provider = GlobalStateMgr.getCurrentState().getAIProviderMgr().getProvider(name);
            if (provider == null) {
                throw new SemanticException("AI provider '%s' does not exist", name);
            }
            try {
                Authorizer.checkAIProviderAction(context, provider, PrivilegeType.USAGE);
            } catch (AccessDeniedException e) {
                reportAccessDenied(context, "AI PROVIDER", name);
            }
        }
    }

    private static void reportAccessDenied(ConnectContext context, String objectType, String name) {
        AccessDeniedException.reportAccessDenied(null, context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                PrivilegeType.USAGE.name(), objectType, name);
    }
}
