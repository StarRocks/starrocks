// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.parser;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.GroupByClause;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SelectList;
import com.starrocks.analysis.SelectListItem;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.ValueList;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.Identifier;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.optimizer.base.SetQualifier;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class AstBuilder extends StarRocksBaseVisitor<ParseNode> {
    @Override
    public ParseNode visitSingleStatement(StarRocksParser.SingleStatementContext context) {
        return visit(context.statement());
    }

    // -------------------------------- Statement ------------------------------

    @Override
    public ParseNode visitQueryStatement(StarRocksParser.QueryStatementContext context) {
        QueryRelation queryRelation = (QueryRelation) visit(context.query());
        return new QueryStatement(queryRelation);
    }

    @Override
    public ParseNode visitQuery(StarRocksParser.QueryContext context) {
        QueryRelation queryRelation = (QueryRelation) visit(context.queryNoWith());

        List<CTERelation> withQuery = new ArrayList<>();
        if (context.with() != null) {
            withQuery = visit(context.with().namedQuery(), CTERelation.class);
        }
        withQuery.forEach(queryRelation::addCTERelation);

        return queryRelation;
    }

    @Override
    public ParseNode visitNamedQuery(StarRocksParser.NamedQueryContext context) {
        Optional<List<Identifier>> columns = Optional.empty();
        if (context.columnAliases() != null) {
            columns = Optional.of(visit(context.columnAliases().identifier(), Identifier.class));
        }

        List<String> columnNames = null;
        if (columns.isPresent()) {
            columnNames = columns.get().stream().map(Identifier::getValue).collect(toList());
        }

        return new CTERelation(
                ((Identifier) visit(context.name)).getValue(),
                columnNames,
                (QueryRelation) visit(context.query()));
    }

    @Override
    public ParseNode visitQueryNoWith(StarRocksParser.QueryNoWithContext context) {

        List<OrderByElement> orderByElements = new ArrayList<>();
        if (context.ORDER() != null) {
            orderByElements.addAll(visit(context.sortItem(), OrderByElement.class));
        }

        LimitElement limitElement = null;
        if (context.limitElement() != null) {
            limitElement = (LimitElement) visit(context.limitElement());
        }

        QueryRelation term = (QueryRelation) visit(context.queryTerm());
        term.setOrderBy(orderByElements);
        term.setLimitElement(limitElement);
        return term;
    }

    @Override
    public ParseNode visitQuerySpecification(StarRocksParser.QuerySpecificationContext context) {
        Relation from = null;
        List<SelectListItem> selectItems = visit(context.selectItem(), SelectListItem.class);

        if (context.fromClause() instanceof StarRocksParser.DualContext) {
            from = null;
        } else {
            StarRocksParser.FromContext fromContext = (StarRocksParser.FromContext) context.fromClause();
            List<Relation> relations = visit(fromContext.relation(), Relation.class);
            if (!relations.isEmpty()) {
                Iterator<Relation> iterator = relations.iterator();
                Relation relation = iterator.next();
                while (iterator.hasNext()) {
                    relation = new JoinRelation(JoinOperator.CROSS_JOIN, relation, iterator.next(), null, false);
                }
                from = relation;
            }
        }

        if (from == null) {
            ArrayList<Expr> row = new ArrayList<>();
            List<String> columnNames = new ArrayList<>();
            for (SelectListItem selectListItem : selectItems) {
                row.add(selectListItem.getExpr());

                String name;
                if (selectListItem.getAlias() != null) {
                    name = selectListItem.getAlias();
                } else if (selectListItem.getExpr() instanceof SlotRef) {
                    name = ((SlotRef) selectListItem.getExpr()).getColumnName();
                } else {
                    name = selectListItem.getExpr().toColumnLabel();
                }
                columnNames.add(name);
            }
            List<ArrayList<Expr>> rows = new ArrayList<>();
            rows.add(row);
            return new ValuesRelation(rows, columnNames);
        }

        boolean isDistinct = context.setQuantifier() != null && context.setQuantifier().DISTINCT() != null;
        return new SelectRelation(
                new SelectList(selectItems, isDistinct),
                from,
                visitIfPresent(context.where, Expr.class).orElse(null),
                visitIfPresent(context.groupingElement(), GroupByClause.class).orElse(null),
                visitIfPresent(context.having, Expr.class).orElse(null));
    }

    // ------------------------------------------- Relation -------------------------------------------

    @Override
    public ParseNode visitTableName(StarRocksParser.TableNameContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName tableName = qualifiedNameToTableName(qualifiedName);
        TableRelation tableRelation = new TableRelation(tableName);
        if (context.hint() != null) {
            for (TerminalNode hint : context.hint().IDENTIFIER()) {
                if (hint.getText().equalsIgnoreCase("_META_")) {
                    tableRelation.setMetaQuery(true);
                }
            }
        }
        return tableRelation;
    }

    @Override
    public ParseNode visitAliasedRelation(StarRocksParser.AliasedRelationContext context) {
        Relation child = (Relation) visit(context.relationPrimary());

        if (context.identifier() == null) {
            return child;
        }
        Identifier identifier = (Identifier) visit(context.identifier());
        child.setAlias(new TableName(null, identifier.getValue()));
        return child;
    }

    @Override
    public ParseNode visitJoinRelation(StarRocksParser.JoinRelationContext context) {
        Relation left = (Relation) visit(context.left);
        Relation right;

        if (context.CROSS() != null) {
            right = (Relation) visit(context.right);
            return new JoinRelation(JoinOperator.CROSS_JOIN, left, right, null, false);
        }

        right = (Relation) visit(context.rightRelation);

        JoinOperator joinType = JoinOperator.INNER_JOIN;
        if (context.joinType().LEFT() != null) {
            if (context.joinType().OUTER() != null) {
                joinType = JoinOperator.LEFT_OUTER_JOIN;
            } else if (context.joinType().SEMI() != null) {
                joinType = JoinOperator.LEFT_SEMI_JOIN;
            } else if (context.joinType().ANTI() != null) {
                joinType = JoinOperator.LEFT_ANTI_JOIN;
            } else {
                joinType = JoinOperator.LEFT_OUTER_JOIN;
            }
        } else if (context.joinType().RIGHT() != null) {
            if (context.joinType().OUTER() != null) {
                joinType = JoinOperator.RIGHT_OUTER_JOIN;
            } else if (context.joinType().SEMI() != null) {
                joinType = JoinOperator.RIGHT_SEMI_JOIN;
            } else if (context.joinType().ANTI() != null) {
                joinType = JoinOperator.RIGHT_ANTI_JOIN;
            } else {
                joinType = JoinOperator.RIGHT_OUTER_JOIN;
            }
        } else if (context.joinType().FULL() != null) {
            joinType = JoinOperator.FULL_OUTER_JOIN;
        }

        Expr predicate = null;
        List<String> usingColNames = null;
        if (context.joinCriteria() != null) {
            if (context.joinCriteria().ON() != null) {
                predicate = (Expr) visit(context.joinCriteria().expression());
            } else if (context.joinCriteria().USING() != null) {
                List<Identifier> criteria = visit(context.joinCriteria().identifier(), Identifier.class);
                usingColNames = criteria.stream().map(Identifier::getValue).collect(Collectors.toList());
            } else {
                throw new IllegalArgumentException("Unsupported join criteria");
            }
        }

        JoinRelation joinRelation = new JoinRelation(joinType, left, right, predicate, context.LATERAL() != null);
        joinRelation.setUsingColNames(usingColNames);
        if (context.hint() != null) {
            joinRelation.setJoinHint(context.hint().IDENTIFIER(0).getText());
        }

        return joinRelation;
    }

    @Override
    public ParseNode visitSetOperation(StarRocksParser.SetOperationContext context) {
        QueryRelation left = (QueryRelation) visit(context.left);
        QueryRelation right = (QueryRelation) visit(context.right);

        boolean distinct = true;
        if (context.setQuantifier() != null) {
            if (context.setQuantifier().DISTINCT() != null) {
                distinct = true;
            } else if (context.setQuantifier().ALL() != null) {
                distinct = false;
            }
        }

        SetQualifier setQualifier = distinct ? SetQualifier.DISTINCT : SetQualifier.ALL;
        switch (context.operator.getType()) {
            case StarRocksLexer.UNION:
                if (left instanceof UnionRelation && ((UnionRelation) left).getQualifier().equals(setQualifier)) {
                    ((UnionRelation) left).addRelation(right);
                    return left;
                } else {
                    return new UnionRelation(Lists.newArrayList(left, right), setQualifier);
                }
            case StarRocksLexer.INTERSECT:
                if (left instanceof IntersectRelation &&
                        ((IntersectRelation) left).getQualifier().equals(setQualifier)) {
                    ((IntersectRelation) left).addRelation(right);
                    return left;
                } else {
                    return new IntersectRelation(Lists.newArrayList(left, right), setQualifier);
                }
            case StarRocksLexer.EXCEPT:
                if (left instanceof ExceptRelation && ((ExceptRelation) left).getQualifier().equals(setQualifier)) {
                    ((ExceptRelation) left).addRelation(right);
                    return left;
                } else {
                    return new ExceptRelation(Lists.newArrayList(left, right), setQualifier);
                }
        }
        throw new IllegalArgumentException("Unsupported set operation: " + context.operator.getText());
    }

    @Override
    public ParseNode visitInlineTable(StarRocksParser.InlineTableContext context) {
        List<ValueList> rowValues = visit(context.rowConstructor(), ValueList.class);
        List<ArrayList<Expr>> rows = rowValues.stream().map(ValueList::getFirstRow).collect(toList());

        List<String> colNames = new ArrayList<>();
        for (int i = 0; i < rows.get(0).size(); ++i) {
            colNames.add("column_" + i);
        }

        return new ValuesRelation(rows, colNames);
    }

    @Override
    public ParseNode visitUnnest(StarRocksParser.UnnestContext context) {
        return new TableFunctionRelation("unnest",
                new FunctionParams(false, visit(context.expression(), Expr.class)));
    }

    private QualifiedName getQualifiedName(StarRocksParser.QualifiedNameContext context) {
        List<String> parts = visit(context.identifier(), Identifier.class).stream()
                .map(Identifier::getValue)
                .collect(Collectors.toList());

        return QualifiedName.of(parts);
    }

    // ------------------------------------------- Util Functions -------------------------------------------

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
        return contexts.stream()
                .map(this::visit)
                .map(clazz::cast)
                .collect(toList());
    }

    private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz) {
        return Optional.ofNullable(context)
                .map(this::visit)
                .map(clazz::cast);
    }

    private TableName qualifiedNameToTableName(QualifiedName qualifiedName) {
        if (qualifiedName.getParts().size() == 2) {
            return new TableName(qualifiedName.getParts().get(0), qualifiedName.getParts().get(1));
        } else if (qualifiedName.getParts().size() == 1) {
            return new TableName(null, qualifiedName.getParts().get(0));
        } else {
            throw new ParsingException("error table name ");
        }
    }
}