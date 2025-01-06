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
import com.starrocks.qe.SessionVariable;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HintCollector extends StarRocksBaseVisitor<Void> {

    private static final int HINT_CHANNEL = 2;

    private CommonTokenStream tokenStream;

    private SessionVariable sessionVariable;

    private IdentityHashMap<ParserRuleContext, List<Token>> contextWithTokenMap = new IdentityHashMap<>();

    private List<Token> tokenList = Lists.newArrayList();

    public HintCollector(CommonTokenStream tokenStream, SessionVariable sessionVariable) {
        this.tokenStream = tokenStream;
        this.sessionVariable = sessionVariable;
    }

    public void collect(StarRocksParser.SingleStatementContext context) {
        visit(context);
    }

    public IdentityHashMap<ParserRuleContext, List<HintNode>> getContextWithHintMap() {
        IdentityHashMap<ParserRuleContext, List<HintNode>> map = new IdentityHashMap<>();
        for (Map.Entry<ParserRuleContext, List<Token>> entry : contextWithTokenMap.entrySet()) {
            ParserRuleContext key = entry.getKey();
            List<HintNode> hintNodes = entry.getValue().stream()
                    .map(e -> HintFactory.buildHintNode(e, sessionVariable))
                    .filter(e -> e != null)
                    .collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(hintNodes)) {
                map.put(key, hintNodes);
            }
        }
        return map;
    }

    @Override
    public Void visitSingleStatement(StarRocksParser.SingleStatementContext context) {
        if (context.statement() != null) {
            return visit(context.statement());
        }
        return null;
    }

    @Override
    public Void visitDataCacheSelectStatement(StarRocksParser.DataCacheSelectStatementContext context) {
        extractHintToRight(context, context.SELECT().getSymbol().getTokenIndex());
        return null;
    }

    @Override
    public Void visitSubmitTaskStatement(StarRocksParser.SubmitTaskStatementContext context) {
        extractHintToRight(context);
        if (context.createTableAsSelectStatement() != null) {
            visit(context.createTableAsSelectStatement());
        } else if (context.insertStatement() != null) {
            visit(context.insertStatement());
        }
        return null;
    }


    @Override
    public Void visitCreateTableAsSelectStatement(StarRocksParser.CreateTableAsSelectStatementContext context) {
        visit(context.queryStatement());
        return null;
    }

    @Override
    public Void visitInsertStatement(StarRocksParser.InsertStatementContext context) {
        extractHintToRight(context);
        if (context.queryStatement() != null) {
            visit(context.queryStatement());
        }
        return null;
    }

    @Override
    public Void visitUpdateStatement(StarRocksParser.UpdateStatementContext context) {
        extractHintToRight(context, context.UPDATE().getSymbol().getTokenIndex());
        if (!(context.fromClause() instanceof StarRocksParser.DualContext)) {
            StarRocksParser.FromContext fromContext = (StarRocksParser.FromContext) context.fromClause();
            if (fromContext.relations() != null) {
                fromContext.relations().relation().stream().forEach(this::visit);
            }
        }
        return null;
    }

    @Override
    public Void visitDeleteStatement(StarRocksParser.DeleteStatementContext context) {
        extractHintToRight(context, context.DELETE().getSymbol().getTokenIndex());
        if (context.using != null) {
            context.using.relation().stream().forEach(this::visit);
        }
        return null;
    }

    @Override
    public Void visitLoadStatement(StarRocksParser.LoadStatementContext context) {
        extractHintToRight(context);
        return null;
    }

    @Override
    public Void visitQueryStatement(StarRocksParser.QueryStatementContext context) {
        visit(context.queryRelation());
        return null;
    }

    @Override
    public Void visitQueryRelation(StarRocksParser.QueryRelationContext context) {
        visit(context.queryNoWith());
        return null;
    }

    @Override
    public Void visitQueryNoWith(StarRocksParser.QueryNoWithContext context) {
        visit(context.queryPrimary());
        return null;
    }

    @Override
    public Void visitQuerySpecification(StarRocksParser.QuerySpecificationContext context) {
        extractHintToRight(context);
        if (!(context.fromClause() instanceof StarRocksParser.DualContext)) {
            StarRocksParser.FromContext fromContext = (StarRocksParser.FromContext) context.fromClause();
            if (fromContext.relations() != null) {
                fromContext.relations().relation().stream().forEach(this::visit);
            }
        }
        return null;
    }

    @Override
    public Void visitSubquery(StarRocksParser.SubqueryContext context) {
        return visit(context.queryRelation());
    }

    @Override
    public Void visitRelation(StarRocksParser.RelationContext context) {
        StarRocksParser.RelationPrimaryContext relationPrimaryCtx = context.relationPrimary();
        if (relationPrimaryCtx instanceof StarRocksParser.SubqueryWithAliasContext) {
            StarRocksParser.SubqueryWithAliasContext subqueryCtx =
                    (StarRocksParser.SubqueryWithAliasContext) relationPrimaryCtx;
            visit(subqueryCtx.subquery());
        }
        return null;
    }

    @Override
    public Void visitSetOperation(StarRocksParser.SetOperationContext context) {
        visit(context.left);
        visit(context.right);
        return null;
    }

    private void extractHintToLeft(ParserRuleContext ctx) {
        Token semi = ctx.start;
        int i = semi.getTokenIndex();
        List<Token> hintTokens = tokenStream.getHiddenTokensToLeft(i, HINT_CHANNEL);
        if (hintTokens != null) {
            contextWithTokenMap.computeIfAbsent(ctx, e -> new ArrayList<>()).addAll(hintTokens);
        }
    }

    private void extractHintToRight(ParserRuleContext ctx) {
        Token semi = ctx.start;
        int i = semi.getTokenIndex();
        List<Token> hintTokens = tokenStream.getHiddenTokensToRight(i, HINT_CHANNEL);
        if (hintTokens != null) {
            contextWithTokenMap.computeIfAbsent(ctx, e -> new ArrayList<>()).addAll(hintTokens);
        }
    }

    private void extractHintToRight(ParserRuleContext ctx, int fromIndex) {
        List<Token> hintTokens = tokenStream.getHiddenTokensToRight(fromIndex, HINT_CHANNEL);
        if (hintTokens != null) {
            contextWithTokenMap.computeIfAbsent(ctx, e -> new ArrayList<>()).addAll(hintTokens);
        }
    }
}
