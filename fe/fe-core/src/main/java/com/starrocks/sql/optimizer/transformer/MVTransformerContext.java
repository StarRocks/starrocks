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
package com.starrocks.sql.optimizer.transformer;

import com.google.common.collect.Maps;
<<<<<<< HEAD
import com.starrocks.analysis.ParseNode;
=======
import com.starrocks.catalog.View;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.ParseNode;
>>>>>>> 32810c222f ([BugFix] Fix view based rewrite bugs (#62918))
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.util.Box;

import java.util.Map;

public class MVTransformerContext {
    // Map from operator to AST tree which is used for text based mv rewrite
    // Use Box to ensure the identity of the operator because the operator may be different even
    // if `Operator`'s equals method is true.
    private final Map<Box<Operator>, ParseNode> opToASTMap = Maps.newHashMap();

    private final ConnectContext connectContext;
    // Whether the current transformer is for inline view, in some cases(eg: mv optimizer builder), needs to disable inline
    // view directly, otherwise it's true by default.
    private final boolean isInlineView;
    // Whether enable text based mv rewrite
    private final boolean isEnableTextBasedMVRewrite;
    // Whether enable view based mv rewrite
    private final boolean isEnableViewBasedMVRewrite;

    public MVTransformerContext(ConnectContext context, boolean isInlineView) {
        this.connectContext = context;
        this.isInlineView = isInlineView;
        // set session variable
        SessionVariable sessionVariable = context.getSessionVariable();
        if (sessionVariable.isDisableMaterializedViewRewrite() ||
                !sessionVariable.isEnableMaterializedViewRewrite()) {
            this.isEnableTextBasedMVRewrite = false;
            this.isEnableViewBasedMVRewrite = false;
        } else {
            this.isEnableTextBasedMVRewrite = sessionVariable.isEnableMaterializedViewTextMatchRewrite();
            this.isEnableViewBasedMVRewrite = sessionVariable.isEnableViewBasedMvRewrite();
        }
    }

    public static MVTransformerContext of(ConnectContext context, boolean isInlineView) {
        return new MVTransformerContext(context, isInlineView);
    }

    /**
     * Whether enable view based mv rewrite, only if
     * - session variable enable_view_based_mv_rewrite is true
     * - view contains related mvs
     */
    public boolean isEnableViewBasedMVRewrite(View view) {
        if (view == null) {
            return false;
        }
        // ensure view's related views not empty
        return isEnableViewBasedMVRewrite && !view.getRelatedMaterializedViews().isEmpty();
    }

    public boolean isInlineView() {
        return isInlineView;
    }

    public boolean isEnableTextBasedMVRewrite() {
        return isEnableTextBasedMVRewrite;
    }

    /**
     * Register the AST tree for the given operator in transformer stage
     * @param op input operator
     * @param ast AST tree of this operator
     */
    public void registerOpAST(Operator op, ParseNode ast) {
        if (op == null || !isEnableTextBasedMVRewrite) {
            return;
        }
        opToASTMap.put(Box.of(op), ast);
    }

    /**
     * Check whether the AST tree for the given operator is registered in transformer stage
     */
    public boolean hasOpAST(Operator op) {
        return opToASTMap.containsKey(Box.of(op));
    }

    /**
     * Check whether the operator to AST tree map is empty or not.
     */
    public boolean isOpASTEmpty() {
        return opToASTMap.isEmpty();
    }

    /**
     * Get the AST tree for the given operator in transformer stage
     */
    public ParseNode getOpAST(Operator op) {
        return opToASTMap.get(Box.of(op));
    }
}
