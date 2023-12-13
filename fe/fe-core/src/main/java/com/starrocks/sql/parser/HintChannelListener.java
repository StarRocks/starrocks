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

package com.starrocks.sql.parser;

import com.google.common.collect.Lists;
import com.starrocks.analysis.HintNode;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HintChannelListener extends StarRocksBaseListener {

    private static final int HINT_CHANNEL = 2;

    private CommonTokenStream tokenStream;

    private IdentityHashMap<ParserRuleContext, List<Token>> contextWithTokenMap = new IdentityHashMap<>();

    private List<Token> tokenList = Lists.newArrayList();

    public HintChannelListener(CommonTokenStream tokenStream) {
        this.tokenStream = tokenStream;
    }

    public IdentityHashMap<ParserRuleContext, List<Token>> getContextWithTokenMap() {
        return contextWithTokenMap;
    }

    public IdentityHashMap<ParserRuleContext, List<HintNode>> getContextWithHintMap() {
        IdentityHashMap<ParserRuleContext, List<HintNode>> map = new IdentityHashMap<>();
        for (Map.Entry<ParserRuleContext, List<Token>> entry : contextWithTokenMap.entrySet()) {
            ParserRuleContext key = entry.getKey();
            List<HintNode> hintNodes = entry.getValue().stream()
                    .map(HintFactory::buildHintNode).filter(e -> e != null)
                    .collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(hintNodes)) {
                map.put(key, hintNodes);
            }
        }
        return map;
    }
    @Override
    public void exitSubmitTaskStatement(StarRocksParser.SubmitTaskStatementContext ctx) {
        extractHintToRight(ctx);
    }

    @Override
    public void exitInsertStatement(StarRocksParser.InsertStatementContext ctx) {
        extractHintToRight(ctx);
    }

    @Override
    public void exitUpdateStatement(StarRocksParser.UpdateStatementContext ctx) {
        extractHintToRight(ctx);
    }

    @Override
    public void exitDeleteStatement(StarRocksParser.DeleteStatementContext ctx) {
        extractHintToRight(ctx);
    }

    @Override
    public void exitLoadStatement(StarRocksParser.LoadStatementContext ctx) {
        extractHintToRight(ctx);
    }

    @Override
    public void exitQuerySpecification(StarRocksParser.QuerySpecificationContext ctx) {
        extractHintToRight(ctx);
    }


    private void extractHintToRight(ParserRuleContext ctx) {
        Token semi = ctx.start;
        int i = semi.getTokenIndex();
        List<Token> hintTokens = tokenStream.getHiddenTokensToRight(i, HINT_CHANNEL);
        if (hintTokens != null) {
            contextWithTokenMap.computeIfAbsent(ctx, e -> new ArrayList<>()).addAll(hintTokens);
        }
    }
}
