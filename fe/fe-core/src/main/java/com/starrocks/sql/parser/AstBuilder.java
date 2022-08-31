// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.parser;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.analysis.AlterViewStmt;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.ArrayElementExpr;
import com.starrocks.analysis.ArrayExpr;
import com.starrocks.analysis.ArrowExpr;
import com.starrocks.analysis.BetweenPredicate;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CaseWhenClause;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.ColWithComment;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.CreateTableAsSelectStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.DefaultValueExpr;
import com.starrocks.analysis.DistributionDesc;
import com.starrocks.analysis.ExistsPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.GroupByClause;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.HashDistributionDesc;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.InformationFunction;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.InsertTarget;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MultiRangePartitionDesc;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.OdbcScalarFunctionCall;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.OutFileClause;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.PartitionDesc;
import com.starrocks.analysis.PartitionKeyDesc;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.analysis.PartitionValue;
import com.starrocks.analysis.RangePartitionDesc;
import com.starrocks.analysis.SelectList;
import com.starrocks.analysis.SelectListItem;
import com.starrocks.analysis.SetType;
import com.starrocks.analysis.ShowDbStmt;
import com.starrocks.analysis.ShowTableStmt;
import com.starrocks.analysis.SingleRangePartitionDesc;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.SysVariableDesc;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.analysis.TypeDef;
import com.starrocks.analysis.UseStmt;
import com.starrocks.analysis.ValueList;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.NotImplementedException;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.Identifier;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.IntervalLiteral;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.Property;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.UnitIdentifier;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.SetQualifier;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class AstBuilder extends StarRocksBaseVisitor<ParseNode> {
    private final long sqlMode;

    public AstBuilder(long sqlMode) {
        this.sqlMode = sqlMode;
    }

    @Override
    public ParseNode visitSingleStatement(StarRocksParser.SingleStatementContext context) {
        return visit(context.statement());
    }

    // -------------------------------- Statement ------------------------------

    @Override
    public ParseNode visitShowDatabases(StarRocksParser.ShowDatabasesContext context) {
        if (context.pattern != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.pattern);
            return new ShowDbStmt(stringLiteral.getValue());
        } else if (context.expression() != null) {
            return new ShowDbStmt(null, (Expr) visit(context.expression()));
        } else {
            return new ShowDbStmt(null, null);
        }
    }

    @Override
    public ParseNode visitShowTables(StarRocksParser.ShowTablesContext context) {
        boolean isVerbose = context.FULL() != null;
        String database = null;
        if (context.qualifiedName() != null) {
            database = getQualifiedName(context.qualifiedName()).toString();
        }

        if (context.pattern != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.pattern);
            return new ShowTableStmt(database, isVerbose, stringLiteral.getValue());
        } else if (context.expression() != null) {
            return new ShowTableStmt(database, isVerbose, null, (Expr) visit(context.expression()));
        } else {
            return new ShowTableStmt(database, isVerbose, null);
        }
    }

    @Override
    public ParseNode visitUse(StarRocksParser.UseContext context) {
        Identifier identifier = (Identifier) visit(context.identifier());
        return new UseStmt(identifier.getValue());
    }

    @Override
    public ParseNode visitQueryStatement(StarRocksParser.QueryStatementContext context) {
        QueryRelation queryRelation = (QueryRelation) visit(context.query());
        QueryStatement queryStatement = new QueryStatement(queryRelation);
        if (context.outfile() != null) {
            queryStatement.setOutFileClause((OutFileClause) visit(context.outfile()));
        }
        return queryStatement;
    }

    @Override
    public ParseNode visitExplain(StarRocksParser.ExplainContext context) {
        QueryStatement queryStatement = (QueryStatement) visit(context.queryStatement());
        StatementBase.ExplainLevel explainLevel = StatementBase.ExplainLevel.NORMAL;
        if (context.LOGICAL() != null) {
            explainLevel = StatementBase.ExplainLevel.LOGICAL;
        } else if (context.VERBOSE() != null) {
            explainLevel = StatementBase.ExplainLevel.VERBOSE;
        } else if (context.COSTS() != null) {
            explainLevel = StatementBase.ExplainLevel.COST;
        }
        queryStatement.setIsExplain(true, explainLevel);
        return queryStatement;
    }

    @Override
    public ParseNode visitInsert(StarRocksParser.InsertContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        PartitionNames partitionNames = null;
        if (context.partitionNames() != null) {
            partitionNames = (PartitionNames) visit(context.partitionNames());
        }

        QueryStatement queryStatement;
        if (context.VALUES() != null) {
            List<ValueList> rowValues = visit(context.expressionsWithDefault(), ValueList.class);
            List<ArrayList<Expr>> rows = rowValues.stream().map(ValueList::getFirstRow).collect(toList());

            List<String> colNames = new ArrayList<>();
            for (int i = 0; i < rows.get(0).size(); ++i) {
                colNames.add("column_" + i);
            }

            queryStatement = new QueryStatement(new ValuesRelation(rows, colNames));
        } else {
            queryStatement = (QueryStatement) visit(context.queryStatement());
        }

        List<String> targetColumnNames = null;
        if (context.columnAliases() != null) {
            // StarRocks tables are not case-sensitive, so targetColumnNames are converted
            // to lowercase characters to facilitate subsequent matching.
            List<Identifier> targetColumnNamesIdentifiers =
                    visitIfPresent(context.columnAliases().identifier(), Identifier.class);
            if (targetColumnNamesIdentifiers != null) {
                targetColumnNames = targetColumnNamesIdentifiers.stream()
                        .map(Identifier::getValue).map(String::toLowerCase).collect(toList());
            }
        }
        if (context.EXPLAIN() != null) {
            queryStatement.setIsExplain(true, StatementBase.ExplainLevel.NORMAL);
        }

        return new InsertStmt(
                new InsertTarget(targetTableName, partitionNames),
                context.lable == null ? null : ((Identifier) visit(context.lable)).getValue(),
                targetColumnNames,
                queryStatement,
                Lists.newArrayList());
    }

    @Override
    public ParseNode visitExpressionsWithDefault(StarRocksParser.ExpressionsWithDefaultContext context) {
        ArrayList<Expr> row = Lists.newArrayList();
        for (int i = 0; i < context.expressionOrDefault().size(); ++i) {
            StarRocksParser.ExpressionOrDefaultContext expressionOrDefaultContext = context.expressionOrDefault(i);
            if (expressionOrDefaultContext.DEFAULT() != null) {
                row.add(new DefaultValueExpr());
            } else {
                row.add((Expr) visit(expressionOrDefaultContext.expression()));
            }
        }

        return new ValueList(row);
    }

    @Override
    public ParseNode visitCreateTableAsSelect(StarRocksParser.CreateTableAsSelectContext context) {
        Map<String, String> properties = new HashMap<>();
        if (context.properties() != null) {
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }

        CreateTableStmt createTableStmt = new CreateTableStmt(
                context.IF() != null,
                false,
                qualifiedNameToTableName(getQualifiedName(context.qualifiedName())),
                null,
                "olap",
                null,
                context.partitionDesc() == null ? null : (PartitionDesc) visit(context.partitionDesc()),
                context.distributionDesc() == null ? null : (DistributionDesc) visit(context.distributionDesc()),
                properties,
                null,
                context.comment() == null ? null :
                        ((StringLiteral) visit(context.comment().string())).getStringValue());

        List<Identifier> columns = visitIfPresent(context.identifier(), Identifier.class);
        return new CreateTableAsSelectStmt(
                createTableStmt,
                columns == null ? null : columns.stream().map(Identifier::getValue).collect(toList()),
                (QueryStatement) visit(context.queryStatement()));
    }

    @Override
    public ParseNode visitPartitionDesc(StarRocksParser.PartitionDescContext context) {
        List<Identifier> identifierList = visit(context.identifierList().identifier(), Identifier.class);
        List<PartitionDesc> partitionDesc = visit(context.rangePartitionDesc(), PartitionDesc.class);
        return new RangePartitionDesc(
                identifierList.stream().map(Identifier::getValue).collect(toList()),
                partitionDesc);
    }

    @Override
    public ParseNode visitSingleRangePartition(StarRocksParser.SingleRangePartitionContext context) {
        PartitionKeyDesc partitionKeyDesc = (PartitionKeyDesc) visit(context.partitionKeyDesc());
        return new SingleRangePartitionDesc(false, ((Identifier) visit(context.identifier())).getValue(),
                partitionKeyDesc, null);
    }

    @Override
    public ParseNode visitMultiRangePartition(StarRocksParser.MultiRangePartitionContext context) {
        if (context.interval() != null) {
            IntervalLiteral intervalLiteral = (IntervalLiteral) visit(context.interval());
            Expr expr = intervalLiteral.getValue();
            long intervalVal;
            if (expr instanceof IntLiteral) {
                intervalVal = ((IntLiteral) expr).getLongValue();
            } else {
                throw new IllegalArgumentException("Unsupported interval expr: " + expr);
            }
            return new MultiRangePartitionDesc(
                    ((StringLiteral) visit(context.string(0))).getStringValue(),
                    ((StringLiteral) visit(context.string(1))).getStringValue(),
                    intervalVal,
                    intervalLiteral.getUnitIdentifier().getDescription());
        } else {
            return new MultiRangePartitionDesc(
                    ((StringLiteral) visit(context.string(0))).getStringValue(),
                    ((StringLiteral) visit(context.string(1))).getStringValue(),
                    Long.parseLong(context.INTEGER_VALUE().getText()));
        }
    }

    @Override
    public ParseNode visitPartitionKeyDesc(StarRocksParser.PartitionKeyDescContext context) {
        PartitionKeyDesc partitionKeyDesc;
        if (context.LESS() != null) {
            List<PartitionValue> partitionValueList =
                    visit(context.partitionValueList().get(0).partitionValue(), PartitionValue.class);
            partitionKeyDesc = new PartitionKeyDesc(partitionValueList);
        } else {
            List<PartitionValue> lowerPartitionValueList =
                    visit(context.partitionValueList().get(0).partitionValue(), PartitionValue.class);
            List<PartitionValue> upperPartitionValueList =
                    visit(context.partitionValueList().get(1).partitionValue(), PartitionValue.class);
            partitionKeyDesc = new PartitionKeyDesc(lowerPartitionValueList, upperPartitionValueList);
        }
        return partitionKeyDesc;
    }

    @Override
    public ParseNode visitPartitionValue(StarRocksParser.PartitionValueContext context) {
        if (context.MAXVALUE() != null) {
            return PartitionValue.MAX_VALUE;
        } else {
            return new PartitionValue(((StringLiteral) visit(context.string())).getStringValue());
        }
    }

    @Override
    public ParseNode visitDistributionDesc(StarRocksParser.DistributionDescContext context) {
        //default buckets number
        int buckets = 10;

        if (context.INTEGER_VALUE() != null) {
            buckets = Integer.parseInt(context.INTEGER_VALUE().getText());
        }
        List<Identifier> identifierList = visit(context.identifierList().identifier(), Identifier.class);

        return new HashDistributionDesc(buckets, identifierList.stream().map(Identifier::getValue).collect(toList()));
    }

    @Override
    public ParseNode visitProperty(StarRocksParser.PropertyContext context) {
        return new Property(
                ((StringLiteral) visit(context.key)).getStringValue(),
                ((StringLiteral) visit(context.value)).getStringValue());
    }

    @Override
    public ParseNode visitOutfile(StarRocksParser.OutfileContext context) {
        Map<String, String> properties = new HashMap<>();
        if (context.properties() != null) {
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }

        String format = null;
        if (context.fileFormat() != null) {
            if (context.fileFormat().identifier() != null) {
                format = ((Identifier) visit(context.fileFormat().identifier())).getValue();
            } else if (context.fileFormat().string() != null) {
                format = ((StringLiteral) visit(context.fileFormat().string())).getStringValue();
            }
        }

        return new OutFileClause(
                ((StringLiteral) visit(context.file)).getStringValue(),
                format,
                properties);
    }

    @Override
    public ParseNode visitCreateView(StarRocksParser.CreateViewContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);

        List<ColWithComment> colWithComments = null;
        if (context.columnNameWithComment().size() > 0) {
            colWithComments = visit(context.columnNameWithComment(), ColWithComment.class);
        }
        return new CreateViewStmt(
                context.IF() != null,
                targetTableName,
                colWithComments,
                context.comment() == null ? null : ((StringLiteral) visit(context.comment())).getStringValue(),
                (QueryStatement) visit(context.queryStatement()));
    }

    @Override
    public ParseNode visitAlterView(StarRocksParser.AlterViewContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);

        List<ColWithComment> colWithComments = null;
        if (context.columnNameWithComment().size() > 0) {
            colWithComments = visit(context.columnNameWithComment(), ColWithComment.class);
        }

        return new AlterViewStmt(targetTableName, colWithComments, (QueryStatement) visit(context.queryStatement()));
    }

    @Override
    public ParseNode visitColumnNameWithComment(StarRocksParser.ColumnNameWithCommentContext context) {
        String comment = null;
        if (context.comment() != null) {
            comment = ((StringLiteral) visit(context.comment())).getStringValue();
        }

        return new ColWithComment(((Identifier) visit(context.identifier())).getValue(), comment);
    }

    // ------------------------------------------- Query Relation -------------------------------------------
    @Override
    public ParseNode visitQuery(StarRocksParser.QueryContext context) {
        QueryRelation queryRelation = (QueryRelation) visit(context.queryNoWith());

        List<CTERelation> withQuery = new ArrayList<>();
        if (context.withClause() != null) {
            withQuery = visit(context.withClause().commonTableExpression(), CTERelation.class);
        }
        withQuery.forEach(queryRelation::addCTERelation);

        return queryRelation;
    }

    @Override
    public ParseNode visitCommonTableExpression(StarRocksParser.CommonTableExpressionContext context) {
        List<Identifier> columns = null;
        if (context.columnAliases() != null) {
            columns = visit(context.columnAliases().identifier(), Identifier.class);
        }

        List<String> columnNames = null;
        if (columns != null) {
            columnNames = columns.stream().map(Identifier::getValue).collect(toList());
        }

        QueryRelation queryRelation = (QueryRelation) visit(context.query());
        return new CTERelation(
                RelationId.of(queryRelation).hashCode(),
                ((Identifier) visit(context.name)).getValue(),
                columnNames,
                new QueryStatement(queryRelation));
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
        term.setLimit(limitElement);
        return term;
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
            case StarRocksLexer.MINUS:
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
    public ParseNode visitQuerySpecification(StarRocksParser.QuerySpecificationContext context) {
        Relation from = null;
        List<SelectListItem> selectItems = visit(context.selectItem(), SelectListItem.class);

        if (context.fromClause() instanceof StarRocksParser.DualContext) {
            if (selectItems.stream().anyMatch(SelectListItem::isStar)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_TABLES_USED);
            }
        } else {
            StarRocksParser.FromContext fromContext = (StarRocksParser.FromContext) context.fromClause();
            if (fromContext.relations() != null) {
                List<Relation> relations = visit(fromContext.relations().relation(), Relation.class);
                Iterator<Relation> iterator = relations.iterator();
                Relation relation = iterator.next();
                while (iterator.hasNext()) {
                    relation = new JoinRelation(null, relation, iterator.next(), null, false);
                }
                from = relation;
            }
        }

        /*
          from == null means a statement without from or from dual, add a single row of null values here,
          so that the semantics are the same, and the processing of subsequent query logic can be simplified,
          such as select sum(1) or select sum(1) from dual, will be converted to select sum(1) from (values(null)) t.
          This can share the same logic as select sum(1) from table
         */
        if (from == null) {
            ArrayList<Expr> row = new ArrayList<>();
            List<String> columnNames = new ArrayList<>();
            row.add(NullLiteral.create(Type.NULL));
            columnNames.add("");
            List<ArrayList<Expr>> rows = new ArrayList<>();
            rows.add(row);
            ValuesRelation valuesRelation = new ValuesRelation(rows, columnNames);
            valuesRelation.setNullValues(true);
            from = valuesRelation;
        }

        boolean isDistinct = context.setQuantifier() != null && context.setQuantifier().DISTINCT() != null;
        SelectList selectList = new SelectList(selectItems, isDistinct);
        if (context.hint() != null) {
            Map<String, String> selectHints = new HashMap<>();
            for (StarRocksParser.HintContext hintContext : context.hint()) {
                for (StarRocksParser.HintMapContext hintMapContext : hintContext.hintMap()) {
                    selectHints.put(hintMapContext.k.getText(),
                            ((LiteralExpr) visit(hintMapContext.v)).getStringValue());
                }
            }
            selectList.setOptHints(selectHints);
        }
        return new SelectRelation(
                selectList,
                from,
                (Expr) visitIfPresent(context.where),
                (GroupByClause) visitIfPresent(context.groupingElement()),
                (Expr) visitIfPresent(context.having));
    }

    @Override
    public ParseNode visitSelectSingle(StarRocksParser.SelectSingleContext context) {
        String alias = null;
        if (context.identifier() != null) {
            alias = ((Identifier) visit(context.identifier())).getValue();
        } else if (context.string() != null) {
            alias = ((StringLiteral) visit(context.string())).getStringValue();
        }

        return new SelectListItem((Expr) visit(context.expression()), alias);
    }

    @Override
    public ParseNode visitSelectAll(StarRocksParser.SelectAllContext context) {
        if (context.qualifiedName() != null) {
            QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
            return new SelectListItem(qualifiedNameToTableName(qualifiedName));
        }
        return new SelectListItem(null);
    }

    @Override
    public ParseNode visitSingleGroupingSet(StarRocksParser.SingleGroupingSetContext context) {
        return new GroupByClause(new ArrayList<>(visit(context.expression(), Expr.class)),
                GroupByClause.GroupingType.GROUP_BY);
    }

    @Override
    public ParseNode visitRollup(StarRocksParser.RollupContext context) {
        List<Expr> groupingExprs = visit(context.expression(), Expr.class);
        return new GroupByClause(new ArrayList<>(groupingExprs), GroupByClause.GroupingType.ROLLUP);
    }

    @Override
    public ParseNode visitCube(StarRocksParser.CubeContext context) {
        List<Expr> groupingExprs = visit(context.expression(), Expr.class);
        return new GroupByClause(new ArrayList<>(groupingExprs), GroupByClause.GroupingType.CUBE);
    }

    @Override
    public ParseNode visitMultipleGroupingSets(StarRocksParser.MultipleGroupingSetsContext context) {
        List<ArrayList<Expr>> groupingSets = new ArrayList<>();
        for (StarRocksParser.GroupingSetContext groupingSetContext : context.groupingSet()) {
            List<Expr> l = visit(groupingSetContext.expression(), Expr.class);
            groupingSets.add(new ArrayList<>(l));
        }

        return new GroupByClause(groupingSets, GroupByClause.GroupingType.GROUPING_SETS);
    }

    @Override
    public ParseNode visitGroupingOperation(StarRocksParser.GroupingOperationContext context) {
        List<Expr> arguments = visit(context.expression(), Expr.class);
        return new GroupingFunctionCallExpr("grouping", arguments);
    }

    @Override
    public ParseNode visitWindowFrame(StarRocksParser.WindowFrameContext context) {
        if (context.end != null) {
            return new AnalyticWindow(
                    getFrameType(context.frameType),
                    (AnalyticWindow.Boundary) visit(context.start),
                    (AnalyticWindow.Boundary) visit(context.end));
        } else {
            return new AnalyticWindow(
                    getFrameType(context.frameType),
                    (AnalyticWindow.Boundary) visit(context.start));
        }
    }

    private static AnalyticWindow.Type getFrameType(Token type) {
        switch (type.getType()) {
            case StarRocksLexer.RANGE:
                return AnalyticWindow.Type.RANGE;
            case StarRocksLexer.ROWS:
                return AnalyticWindow.Type.ROWS;
        }

        throw new IllegalArgumentException("Unsupported frame type: " + type.getText());
    }

    @Override
    public ParseNode visitUnboundedFrame(StarRocksParser.UnboundedFrameContext context) {
        return new AnalyticWindow.Boundary(getUnboundedFrameBoundType(context.boundType), null);
    }

    @Override
    public ParseNode visitBoundedFrame(StarRocksParser.BoundedFrameContext context) {
        return new AnalyticWindow.Boundary(getBoundedFrameBoundType(context.boundType),
                (Expr) visit(context.expression()));
    }

    @Override
    public ParseNode visitCurrentRowBound(StarRocksParser.CurrentRowBoundContext context) {
        return new AnalyticWindow.Boundary(AnalyticWindow.BoundaryType.CURRENT_ROW, null);
    }

    private static AnalyticWindow.BoundaryType getBoundedFrameBoundType(Token token) {
        switch (token.getType()) {
            case StarRocksLexer.PRECEDING:
                return AnalyticWindow.BoundaryType.PRECEDING;
            case StarRocksLexer.FOLLOWING:
                return AnalyticWindow.BoundaryType.FOLLOWING;
        }

        throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
    }

    private static AnalyticWindow.BoundaryType getUnboundedFrameBoundType(Token token) {
        switch (token.getType()) {
            case StarRocksLexer.PRECEDING:
                return AnalyticWindow.BoundaryType.UNBOUNDED_PRECEDING;
            case StarRocksLexer.FOLLOWING:
                return AnalyticWindow.BoundaryType.UNBOUNDED_FOLLOWING;
        }

        throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
    }

    @Override
    public ParseNode visitSortItem(StarRocksParser.SortItemContext context) {
        return new OrderByElement(
                (Expr) visit(context.expression()),
                getOrderingType(context.ordering),
                getNullOrderingType(getOrderingType(context.ordering), context.nullOrdering));
    }

    private boolean getNullOrderingType(boolean isAsc, Token token) {
        if (token == null) {
            return (!SqlModeHelper.check(sqlMode, SqlModeHelper.MODE_SORT_NULLS_LAST)) == isAsc;
        }
        switch (token.getType()) {
            case StarRocksLexer.FIRST:
                return true;
            case StarRocksLexer.LAST:
                return false;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static boolean getOrderingType(Token token) {
        if (token == null) {
            return true;
        }
        switch (token.getType()) {
            case StarRocksLexer.ASC:
                return true;
            case StarRocksLexer.DESC:
                return false;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    @Override
    public ParseNode visitLimitElement(StarRocksParser.LimitElementContext context) {
        long limit = Long.parseLong(context.limit.getText());
        long offset = 0;
        if (context.offset != null) {
            offset = Long.parseLong(context.offset.getText());
        }
        return new LimitElement(offset, limit);
    }

    // ------------------------------------------- Relation -------------------------------------------

    @Override
    public ParseNode visitRelation(StarRocksParser.RelationContext context) {
        Relation relation = (Relation) visit(context.relationPrimary());
        List<JoinRelation> joinRelations = visit(context.joinRelation(), JoinRelation.class);

        Relation leftChildRelation = relation;
        for (JoinRelation joinRelation : joinRelations) {
            joinRelation.setLeft(leftChildRelation);
            leftChildRelation = joinRelation;
        }
        return leftChildRelation;
    }

    @Override
    public ParseNode visitParenthesizedRelation(StarRocksParser.ParenthesizedRelationContext context) {
        if (context.relations().relation().size() == 1) {
            return visit(context.relations().relation().get(0));
        } else {
            List<Relation> relations = visit(context.relations().relation(), Relation.class);
            Iterator<Relation> iterator = relations.iterator();
            Relation relation = iterator.next();
            while (iterator.hasNext()) {
                relation = new JoinRelation(null, relation, iterator.next(), null, false);
            }
            return relation;
        }
    }

    @Override
    public ParseNode visitTableAtom(StarRocksParser.TableAtomContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName tableName = qualifiedNameToTableName(qualifiedName);
        PartitionNames partitionNames = null;
        if (context.partitionNames() != null) {
            partitionNames = (PartitionNames) visit(context.partitionNames());
        }

        List<Long> tabletIds = Lists.newArrayList();
        if (context.tabletList() != null) {
            tabletIds = context.tabletList().INTEGER_VALUE().stream().map(ParseTree::getText)
                    .map(Long::parseLong).collect(toList());
        }

        TableRelation tableRelation = new TableRelation(tableName, partitionNames, tabletIds);
        if (context.bracketHint() != null) {
            for (TerminalNode hint : context.bracketHint().IDENTIFIER()) {
                if (hint.getText().equalsIgnoreCase("_META_")) {
                    tableRelation.setMetaQuery(true);
                }
            }
        }

        if (context.alias != null) {
            Identifier identifier = (Identifier) visit(context.alias);
            tableRelation.setAlias(new TableName(null, identifier.getValue()));
        }
        return tableRelation;
    }

    @Override
    public ParseNode visitJoinRelation(StarRocksParser.JoinRelationContext context) {
        // Because left recursion is required to parse the leftmost atom table first.
        // Therefore, the parsed result does not contain the information of the left table,
        // which is temporarily assigned to Null,
        // and the information of the left table will be filled in visitRelation
        Relation left = null;
        Relation right = (Relation) visit(context.rightRelation);

        JoinOperator joinType = JoinOperator.INNER_JOIN;
        if (context.crossOrInnerJoinType() != null) {
            if (context.crossOrInnerJoinType().CROSS() != null) {
                joinType = JoinOperator.CROSS_JOIN;
            } else {
                joinType = JoinOperator.INNER_JOIN;
            }
        } else if (context.outerAndSemiJoinType().LEFT() != null) {
            if (context.outerAndSemiJoinType().OUTER() != null) {
                joinType = JoinOperator.LEFT_OUTER_JOIN;
            } else if (context.outerAndSemiJoinType().SEMI() != null) {
                joinType = JoinOperator.LEFT_SEMI_JOIN;
            } else if (context.outerAndSemiJoinType().ANTI() != null) {
                joinType = JoinOperator.LEFT_ANTI_JOIN;
            } else {
                joinType = JoinOperator.LEFT_OUTER_JOIN;
            }
        } else if (context.outerAndSemiJoinType().RIGHT() != null) {
            if (context.outerAndSemiJoinType().OUTER() != null) {
                joinType = JoinOperator.RIGHT_OUTER_JOIN;
            } else if (context.outerAndSemiJoinType().SEMI() != null) {
                joinType = JoinOperator.RIGHT_SEMI_JOIN;
            } else if (context.outerAndSemiJoinType().ANTI() != null) {
                joinType = JoinOperator.RIGHT_ANTI_JOIN;
            } else {
                joinType = JoinOperator.RIGHT_OUTER_JOIN;
            }
        } else if (context.outerAndSemiJoinType().FULL() != null) {
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
        if (context.bracketHint() != null) {
            joinRelation.setJoinHint(context.bracketHint().IDENTIFIER(0).getText());
        }

        return joinRelation;
    }

    @Override
    public ParseNode visitInlineTable(StarRocksParser.InlineTableContext context) {
        List<ValueList> rowValues = visit(context.rowConstructor(), ValueList.class);
        List<ArrayList<Expr>> rows = rowValues.stream().map(ValueList::getFirstRow).collect(toList());

        List<String> colNames = new ArrayList<>();
        for (int i = 0; i < rows.get(0).size(); ++i) {
            colNames.add("column_" + i);
        }

        ValuesRelation valuesRelation = new ValuesRelation(rows, colNames);

        if (context.alias != null) {
            Identifier identifier = (Identifier) visit(context.alias);
            valuesRelation.setAlias(new TableName(null, identifier.getValue()));
        }
        return valuesRelation;
    }

    @Override
    public ParseNode visitTableFunction(StarRocksParser.TableFunctionContext context) {
        TableFunctionRelation tableFunctionRelation = new TableFunctionRelation(
                getQualifiedName(context.qualifiedName()).toString(),
                new FunctionParams(false, visit(context.expression(), Expr.class)));

        if (context.alias != null) {
            Identifier identifier = (Identifier) visit(context.alias);
            tableFunctionRelation.setAlias(new TableName(null, identifier.getValue()));
        }
        return tableFunctionRelation;
    }

    @Override
    public ParseNode visitRowConstructor(StarRocksParser.RowConstructorContext context) {
        ArrayList<Expr> row = new ArrayList<>(visit(context.expression(), Expr.class));
        return new ValueList(row);
    }

    @Override
    public ParseNode visitPartitionNames(StarRocksParser.PartitionNamesContext context) {
        List<Identifier> identifierList = visit(context.identifier(), Identifier.class);
        return new PartitionNames(context.TEMPORARY() != null,
                identifierList.stream().map(Identifier::getValue).collect(toList()));
    }

    @Override
    public ParseNode visitSubquery(StarRocksParser.SubqueryContext context) {
        return new SubqueryRelation(new QueryStatement((QueryRelation) visit(context.query())));
    }

    @Override
    public ParseNode visitSubqueryPrimary(StarRocksParser.SubqueryPrimaryContext context) {
        SubqueryRelation subqueryRelation = (SubqueryRelation) visit(context.subquery());
        return subqueryRelation.getQueryStatement().getQueryRelation();
    }

    @Override
    public ParseNode visitSubqueryRelation(StarRocksParser.SubqueryRelationContext context) {
        SubqueryRelation subqueryRelation = (SubqueryRelation) visit(context.subquery());
        if (context.alias != null) {
            Identifier identifier = (Identifier) visit(context.alias);
            subqueryRelation.setAlias(new TableName(null, identifier.getValue()));
        }
        return subqueryRelation;
    }

    @Override
    public ParseNode visitSubqueryExpression(StarRocksParser.SubqueryExpressionContext context) {
        SubqueryRelation subqueryRelation = (SubqueryRelation) visit(context.subquery());
        return new Subquery(subqueryRelation.getQueryStatement());
    }

    @Override
    public ParseNode visitInSubquery(StarRocksParser.InSubqueryContext context) {
        boolean isNotIn = context.NOT() != null;
        QueryRelation query = (QueryRelation) visit(context.query());

        return new InPredicate((Expr) visit(context.value), new Subquery(new QueryStatement(query)), isNotIn);
    }

    @Override
    public ParseNode visitExists(StarRocksParser.ExistsContext context) {
        QueryRelation query = (QueryRelation) visit(context.query());
        return new ExistsPredicate(new Subquery(new QueryStatement(query)), false);
    }

    @Override
    public ParseNode visitScalarSubquery(StarRocksParser.ScalarSubqueryContext context) {
        BinaryPredicate.Operator op = getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0))
                .getSymbol());
        Subquery subquery = new Subquery(new QueryStatement((QueryRelation) visit(context.query())));
        return new BinaryPredicate(op, (Expr) visit(context.booleanExpression()), subquery);
    }

    // ------------------------------------------- Logical Expression -------------------------------------------

    @Override
    public ParseNode visitLogicalNot(StarRocksParser.LogicalNotContext context) {
        return new CompoundPredicate(CompoundPredicate.Operator.NOT, (Expr) visit(context.expression()), null);
    }

    @Override
    public ParseNode visitLogicalBinary(StarRocksParser.LogicalBinaryContext context) {
        Expr left = (Expr) visit(context.left);
        Expr right = (Expr) visit(context.right);

        if (context.operator.getType() == StarRocksLexer.LOGICAL_OR) {
            return new CompoundPredicate(CompoundPredicate.Operator.OR, left, right);
        } else {
            return new CompoundPredicate(getLogicalBinaryOperator(context.operator), left, right);
        }
    }

    private static CompoundPredicate.Operator getLogicalBinaryOperator(Token token) {
        switch (token.getType()) {
            case StarRocksLexer.AND:
                return CompoundPredicate.Operator.AND;
            case StarRocksLexer.OR:
                return CompoundPredicate.Operator.OR;
        }

        throw new IllegalArgumentException("Unsupported operator: " + token.getText());
    }

    // ------------------------------------------- Predicate Expression -------------------------------------------

    @Override
    public ParseNode visitPredicate(StarRocksParser.PredicateContext context) {
        if (context.predicateOperations() != null) {
            return visit(context.predicateOperations());
        } else {
            return visit(context.valueExpression());
        }
    }

    @Override
    public ParseNode visitIsNull(StarRocksParser.IsNullContext context) {
        Expr child = (Expr) visit(context.booleanExpression());

        if (context.NOT() == null) {
            return new IsNullPredicate(child, false);
        } else {
            return new IsNullPredicate(child, true);
        }
    }

    @Override
    public ParseNode visitComparison(StarRocksParser.ComparisonContext context) {
        BinaryPredicate.Operator op = getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0))
                .getSymbol());
        return new BinaryPredicate(op, (Expr) visit(context.left), (Expr) visit(context.right));
    }

    private static BinaryPredicate.Operator getComparisonOperator(Token symbol) {
        switch (symbol.getType()) {
            case StarRocksParser.EQ:
                return BinaryPredicate.Operator.EQ;
            case StarRocksParser.NEQ:
                return BinaryPredicate.Operator.NE;
            case StarRocksParser.LT:
                return BinaryPredicate.Operator.LT;
            case StarRocksParser.LTE:
                return BinaryPredicate.Operator.LE;
            case StarRocksParser.GT:
                return BinaryPredicate.Operator.GT;
            case StarRocksParser.GTE:
                return BinaryPredicate.Operator.GE;
            case StarRocksParser.EQ_FOR_NULL:
                return BinaryPredicate.Operator.EQ_FOR_NULL;
        }

        throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
    }

    @Override
    public ParseNode visitInList(StarRocksParser.InListContext context) {
        boolean isNotIn = context.NOT() != null;
        return new InPredicate(
                (Expr) visit(context.value),
                visit(context.expression(), Expr.class), isNotIn);
    }

    @Override
    public ParseNode visitBetween(StarRocksParser.BetweenContext context) {
        boolean isNotBetween = context.NOT() != null;

        return new BetweenPredicate(
                (Expr) visit(context.value),
                (Expr) visit(context.lower),
                (Expr) visit(context.upper),
                isNotBetween);
    }

    @Override
    public ParseNode visitLike(StarRocksParser.LikeContext context) {
        LikePredicate likePredicate;
        if (context.REGEXP() != null || context.RLIKE() != null) {
            likePredicate = new LikePredicate(LikePredicate.Operator.REGEXP,
                    (Expr) visit(context.value),
                    (Expr) visit(context.pattern));
        } else {
            likePredicate = new LikePredicate(
                    LikePredicate.Operator.LIKE,
                    (Expr) visit(context.value),
                    (Expr) visit(context.pattern));
        }
        if (context.NOT() != null) {
            return new CompoundPredicate(CompoundPredicate.Operator.NOT, likePredicate, null);
        } else {
            return likePredicate;
        }
    }

    @Override
    public ParseNode visitSimpleCase(StarRocksParser.SimpleCaseContext context) {
        return new CaseExpr(
                (Expr) visit(context.caseExpr),
                visit(context.whenClause(), CaseWhenClause.class),
                (Expr) visitIfPresent(context.elseExpression));
    }

    @Override
    public ParseNode visitSearchedCase(StarRocksParser.SearchedCaseContext context) {
        return new CaseExpr(
                null,
                visit(context.whenClause(), CaseWhenClause.class),
                (Expr) visitIfPresent(context.elseExpression));
    }

    @Override
    public ParseNode visitWhenClause(StarRocksParser.WhenClauseContext context) {
        return new CaseWhenClause((Expr) visit(context.condition), (Expr) visit(context.result));
    }

    // ------------------------------------------- Value Expression -------------------------------------------

    @Override
    public ParseNode visitArithmeticUnary(StarRocksParser.ArithmeticUnaryContext context) {
        Expr child = (Expr) visit(context.primaryExpression());
        switch (context.operator.getType()) {
            case StarRocksLexer.MINUS_SYMBOL:
                if (child.isLiteral() && child.getType().isNumericType()) {
                    try {
                        ((LiteralExpr) child).swapSign();
                    } catch (NotImplementedException e) {
                        throw new ParsingException(e.getMessage());
                    }
                    return child;
                } else {
                    return new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, new IntLiteral(-1), child);
                }
            case StarRocksLexer.PLUS_SYMBOL:
                return child;
            case StarRocksLexer.BITNOT:
                return new ArithmeticExpr(ArithmeticExpr.Operator.BITNOT, child, null);
            case StarRocksLexer.LOGICAL_NOT:
                return new CompoundPredicate(CompoundPredicate.Operator.NOT, child, null);
            default:
                throw new UnsupportedOperationException("Unsupported sign: " + context.operator.getText());
        }
    }

    @Override
    public ParseNode visitArithmeticBinary(StarRocksParser.ArithmeticBinaryContext context) {
        Expr left = (Expr) visit(context.left);
        Expr right = (Expr) visit(context.right);
        if (left instanceof IntervalLiteral) {
            return new TimestampArithmeticExpr(getArithmeticBinaryOperator(context.operator), right,
                    ((IntervalLiteral) left).getValue(),
                    ((IntervalLiteral) left).getUnitIdentifier().getDescription(), true);
        }

        if (right instanceof IntervalLiteral) {
            return new TimestampArithmeticExpr(getArithmeticBinaryOperator(context.operator), left,
                    ((IntervalLiteral) right).getValue(),
                    ((IntervalLiteral) right).getUnitIdentifier().getDescription(), false);
        }

        return new ArithmeticExpr(getArithmeticBinaryOperator(context.operator), left, right);
    }

    private static ArithmeticExpr.Operator getArithmeticBinaryOperator(Token operator) {
        switch (operator.getType()) {
            case StarRocksLexer.PLUS_SYMBOL:
                return ArithmeticExpr.Operator.ADD;
            case StarRocksLexer.MINUS_SYMBOL:
                return ArithmeticExpr.Operator.SUBTRACT;
            case StarRocksLexer.ASTERISK_SYMBOL:
                return ArithmeticExpr.Operator.MULTIPLY;
            case StarRocksLexer.SLASH_SYMBOL:
                return ArithmeticExpr.Operator.DIVIDE;
            case StarRocksLexer.PERCENT_SYMBOL:
                return ArithmeticExpr.Operator.MOD;
            case StarRocksLexer.INT_DIV:
                return ArithmeticExpr.Operator.INT_DIVIDE;
            case StarRocksLexer.BITAND:
                return ArithmeticExpr.Operator.BITAND;
            case StarRocksLexer.BITOR:
                return ArithmeticExpr.Operator.BITOR;
            case StarRocksLexer.BITXOR:
                return ArithmeticExpr.Operator.BITXOR;
        }

        throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
    }

    @Override
    public ParseNode visitOdbcFunctionCallExpression(StarRocksParser.OdbcFunctionCallExpressionContext context) {
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) visit(context.functionCall());
        OdbcScalarFunctionCall odbcScalarFunctionCall = new OdbcScalarFunctionCall(functionCallExpr);
        return odbcScalarFunctionCall.mappingFunction();
    }

    private static final List<String> DATE_FUNCTIONS =
            Lists.newArrayList("DATE_ADD", "ADDDATE", "DAYS_ADD", "DATE_SUB", "SUBDATE", "DAYS_SUB");

    @Override
    public ParseNode visitSimpleFunctionCall(StarRocksParser.SimpleFunctionCallContext context) {

        String functionName = getQualifiedName(context.qualifiedName()).toString();

        if (DATE_FUNCTIONS.contains(functionName.toUpperCase())) {
            if (context.expression().size() != 2) {
                throw new ParsingException(
                        functionName + " must as format " + functionName + "(date,INTERVAL expr unit)");
            }

            Expr e1 = (Expr) visit(context.expression(0));
            Expr e2 = (Expr) visit(context.expression(1));
            if (!(e2 instanceof IntervalLiteral)) {
                e2 = new IntervalLiteral(e2, new UnitIdentifier("DAY"));
            }
            IntervalLiteral intervalLiteral = (IntervalLiteral) e2;

            return new TimestampArithmeticExpr(functionName, e1, intervalLiteral.getValue(),
                    intervalLiteral.getUnitIdentifier().getDescription());
        }

        if (functionName.equalsIgnoreCase("isnull")) {
            List<Expr> params = visit(context.expression(), Expr.class);
            if (params.size() != 1) {
                throw new SemanticException("No matching function with signature: %s(%s).", functionName,
                        Joiner.on(", ").join(params.stream().map(p -> p.getType().toSql()).collect(toList())));
            }
            return new IsNullPredicate(params.get(0), false);
        }

        FunctionName fnName = FunctionName.createFnName(functionName);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(fnName,
                new FunctionParams(false, visit(context.expression(), Expr.class)));

        if (context.over() != null) {
            return buildOverClause(functionCallExpr, context.over());
        }
        return functionCallExpr;
    }

    @Override
    public ParseNode visitAggregationFunctionCall(StarRocksParser.AggregationFunctionCallContext context) {

        String functionName;
        if (context.aggregationFunction().COUNT() != null) {
            functionName = "count";
        } else if (context.aggregationFunction().AVG() != null) {
            functionName = "avg";
        } else if (context.aggregationFunction().SUM() != null) {
            functionName = "sum";
        } else if (context.aggregationFunction().MIN() != null) {
            functionName = "min";
        } else if (context.aggregationFunction().MAX() != null) {
            functionName = "max";
        } else {
            throw new StarRocksPlannerException("Aggregate functions are not being parsed correctly",
                    ErrorType.INTERNAL_ERROR);
        }
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(functionName,
                context.aggregationFunction().ASTERISK_SYMBOL() == null ?
                        new FunctionParams(context.aggregationFunction().DISTINCT() != null,
                                visit(context.aggregationFunction().expression(), Expr.class)) :
                        FunctionParams.createStarParam());

        if (context.over() != null) {
            return buildOverClause(functionCallExpr, context.over());
        }
        return functionCallExpr;
    }

    @Override
    public ParseNode visitWindowFunctionCall(StarRocksParser.WindowFunctionCallContext context) {
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) visit(context.windowFunction());
        return buildOverClause(functionCallExpr, context.over());
    }

    public static final ImmutableSet<String> WindowFunctionSet = ImmutableSet.of(
            "row_number", "rank", "dense_rank", "lead", "lag", "first_value", "last_value");

    @Override
    public ParseNode visitWindowFunction(StarRocksParser.WindowFunctionContext context) {
        if (WindowFunctionSet.contains(context.name.getText().toLowerCase())) {
            return new FunctionCallExpr(context.name.getText().toLowerCase(),
                    new FunctionParams(false, visit(context.expression(), Expr.class)));
        }
        throw new ParsingException("Unknown window function " + context.name.getText());
    }

    private AnalyticExpr buildOverClause(FunctionCallExpr functionCallExpr, StarRocksParser.OverContext context) {
        functionCallExpr.setIsAnalyticFnCall(true);
        List<OrderByElement> orderByElements = new ArrayList<>();
        if (context.ORDER() != null) {
            orderByElements = visit(context.sortItem(), OrderByElement.class);
        }
        List<Expr> partitionExprs = visit(context.partition, Expr.class);

        return new AnalyticExpr(functionCallExpr, partitionExprs, orderByElements,
                (AnalyticWindow) visitIfPresent(context.windowFrame()));
    }

    @Override
    public ParseNode visitExtract(StarRocksParser.ExtractContext context) {
        String fieldString = context.identifier().getText();
        return new FunctionCallExpr(fieldString,
                new FunctionParams(Lists.newArrayList((Expr) visit(context.valueExpression()))));
    }

    @Override
    public ParseNode visitCast(StarRocksParser.CastContext context) {
        return new CastExpr(new TypeDef(getType(context.type())), (Expr) visit(context.expression()));
    }

    @Override
    public ParseNode visitInformationFunctionExpression(StarRocksParser.InformationFunctionExpressionContext context) {
        if (context.name.getText().equalsIgnoreCase("database")
                || context.name.getText().equalsIgnoreCase("schema")
                || context.name.getText().equalsIgnoreCase("user")
                || context.name.getText().equalsIgnoreCase("current_user")
                || context.name.getText().equalsIgnoreCase("connection_id")) {
            return new InformationFunction(context.name.getText().toUpperCase());
        }
        throw new ParsingException("Unknown special function " + context.name.getText());
    }

    @Override
    public ParseNode visitSpecialFunctionExpression(StarRocksParser.SpecialFunctionExpressionContext context) {
        if (context.CHAR() != null) {
            return new FunctionCallExpr("char", visit(context.expression(), Expr.class));
        } else if (context.DAY() != null) {
            return new FunctionCallExpr("day", visit(context.expression(), Expr.class));
        } else if (context.HOUR() != null) {
            return new FunctionCallExpr("hour", visit(context.expression(), Expr.class));
        } else if (context.IF() != null) {
            return new FunctionCallExpr("if", visit(context.expression(), Expr.class));
        } else if (context.LEFT() != null) {
            return new FunctionCallExpr("left", visit(context.expression(), Expr.class));
        } else if (context.LIKE() != null) {
            return new FunctionCallExpr("like", visit(context.expression(), Expr.class));
        } else if (context.MINUTE() != null) {
            return new FunctionCallExpr("minute", visit(context.expression(), Expr.class));
        } else if (context.MOD() != null) {
            return new FunctionCallExpr("mod", visit(context.expression(), Expr.class));
        } else if (context.MONTH() != null) {
            return new FunctionCallExpr("month", visit(context.expression(), Expr.class));
        } else if (context.REGEXP() != null) {
            return new FunctionCallExpr("regexp", visit(context.expression(), Expr.class));
        } else if (context.RIGHT() != null) {
            return new FunctionCallExpr("right", visit(context.expression(), Expr.class));
        } else if (context.RLIKE() != null) {
            return new FunctionCallExpr("regexp", visit(context.expression(), Expr.class));
        } else if (context.SECOND() != null) {
            return new FunctionCallExpr("second", visit(context.expression(), Expr.class));
        } else if (context.YEAR() != null) {
            return new FunctionCallExpr("year", visit(context.expression(), Expr.class));
        } else if (context.PASSWORD() != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.string());
            return new StringLiteral(new String(MysqlPassword.makeScrambledPassword(stringLiteral.getValue())));
        }

        if (context.TIMESTAMPADD() != null || context.TIMESTAMPDIFF() != null) {
            String functionName = context.TIMESTAMPADD() != null ? "TIMESTAMPADD" : "TIMESTAMPDIFF";
            UnitIdentifier e1 = (UnitIdentifier) visit(context.unitIdentifier());
            Expr e2 = (Expr) visit(context.expression(0));
            Expr e3 = (Expr) visit(context.expression(1));

            return new TimestampArithmeticExpr(functionName, e3, e2, e1.getDescription());
        }

        throw new ParsingException("No matching function with signature: %s(%s).", context.getText(),
                visit(context.expression(), Expr.class));
    }

    @Override
    public ParseNode visitConcat(StarRocksParser.ConcatContext context) {
        Expr left = (Expr) visit(context.left);
        Expr right = (Expr) visit(context.right);
        return new FunctionCallExpr("concat", new FunctionParams(Lists.newArrayList(left, right)));
    }

    // ------------------------------------------- Literal -------------------------------------------

    @Override
    public ParseNode visitNullLiteral(StarRocksParser.NullLiteralContext context) {
        return new NullLiteral();
    }

    @Override
    public ParseNode visitBooleanLiteral(StarRocksParser.BooleanLiteralContext context) {
        try {
            return new BoolLiteral(context.getText());
        } catch (AnalysisException e) {
            throw new ParsingException("Invalid boolean literal: " + context.getText());
        }
    }

    @Override
    public ParseNode visitNumericLiteral(StarRocksParser.NumericLiteralContext context) {
        return visit(context.number());
    }

    private static final BigInteger LONG_MAX = new BigInteger("9223372036854775807"); // 2^63 - 1

    private static final BigInteger LARGEINT_MAX_ABS =
            new BigInteger("170141183460469231731687303715884105728"); // 2^127

    @Override
    public ParseNode visitIntegerValue(StarRocksParser.IntegerValueContext context) {
        try {
            BigInteger intLiteral = new BigInteger(context.getText());
            // Note: val is positive, because we do not recognize minus character in 'IntegerLiteral'
            // -2^63 will be recognized as large int(__int128)
            if (intLiteral.compareTo(LONG_MAX) <= 0) {
                return new IntLiteral(intLiteral.longValue());
            } else if (intLiteral.compareTo(LARGEINT_MAX_ABS) <= 0) {
                return new LargeIntLiteral(intLiteral.toString());
            } else {
                throw new ParsingException("Numeric overflow " + intLiteral);
            }
        } catch (NumberFormatException | AnalysisException e) {
            throw new ParsingException("Invalid numeric literal: " + context.getText());
        }
    }

    @Override
    public ParseNode visitDoubleValue(StarRocksParser.DoubleValueContext context) {
        try {
            BigDecimal decimal = new BigDecimal(context.getText());
            int precision = DecimalLiteral.getRealPrecision(decimal);
            int scale = DecimalLiteral.getRealScale(decimal);
            int integerPartWidth = precision - scale;
            if (integerPartWidth > 38) {
                return new FloatLiteral(context.getText());
            }
            return new DecimalLiteral(decimal);
        } catch (AnalysisException | NumberFormatException e) {
            throw new ParsingException(e.getMessage());
        }
    }

    @Override
    public ParseNode visitDecimalValue(StarRocksParser.DecimalValueContext context) {
        try {
            return new DecimalLiteral(context.getText());
        } catch (AnalysisException e) {
            throw new ParsingException(e.getMessage());
        }
    }

    @Override
    public ParseNode visitDateLiteral(StarRocksParser.DateLiteralContext context) {
        String value = ((StringLiteral) visit(context.string())).getValue();
        try {
            if (context.DATE() != null) {
                return new DateLiteral(value, Type.DATE);
            }
            if (context.DATETIME() != null) {
                return new DateLiteral(value, Type.DATETIME);
            }
        } catch (AnalysisException e) {
            throw new ParsingException(e.getMessage());
        }
        throw new ParsingException("Parse Error : unknown type " + context.getText());
    }

    @Override
    public ParseNode visitString(StarRocksParser.StringContext context) {
        String quotedString;
        if (context.SINGLE_QUOTED_TEXT() != null) {
            quotedString = context.SINGLE_QUOTED_TEXT().getText();
            // For support mysql embedded quotation
            // In a single-quoted string, two single-quotes are combined into one single-quote
            quotedString = quotedString.substring(1, quotedString.length() - 1).replace("''", "'");
        } else {
            quotedString = context.DOUBLE_QUOTED_TEXT().getText();
            // For support mysql embedded quotation
            // In a double-quoted string, two double-quotes are combined into one double-quote
            quotedString = quotedString.substring(1, quotedString.length() - 1).replace("\"\"", "\"");
        }
        return new StringLiteral(escapeBackSlash(quotedString));
    }

    private static String escapeBackSlash(String str) {
        StringWriter writer = new StringWriter();
        int strLen = str.length();
        for (int i = 0; i < strLen; ++i) {
            char c = str.charAt(i);
            if (c == '\\' && (i + 1) < strLen) {
                switch (str.charAt(i + 1)) {
                    case 'n':
                        writer.append('\n');
                        break;
                    case 't':
                        writer.append('\t');
                        break;
                    case 'r':
                        writer.append('\r');
                        break;
                    case 'b':
                        writer.append('\b');
                        break;
                    case '0':
                        writer.append('\0'); // Ascii null
                        break;
                    case 'Z': // ^Z must be escaped on Win32
                        writer.append('\032');
                        break;
                    case '_':
                    case '%':
                        writer.append('\\'); // remember prefix for wildcard
                        /* Fall through */
                    default:
                        writer.append(str.charAt(i + 1));
                        break;
                }
                i++;
            } else {
                writer.append(c);
            }
        }

        return writer.toString();
    }

    @Override
    public ParseNode visitArrayConstructor(StarRocksParser.ArrayConstructorContext context) {
        if (context.arrayType() != null) {
            return new ArrayExpr(
                    new ArrayType(getType(context.arrayType().type())),
                    visit(context.expression(), Expr.class));
        }

        return new ArrayExpr(null, visit(context.expression(), Expr.class));
    }

    @Override
    public ParseNode visitArraySubscript(StarRocksParser.ArraySubscriptContext context) {
        Expr value = (Expr) visit(context.value);
        Expr index = (Expr) visit(context.index);
        return new ArrayElementExpr(value, index);
    }

    @Override
    public ParseNode visitArraySlice(StarRocksParser.ArraySliceContext context) {
        throw new ParsingException("Array slice is not currently supported");
        //TODO: support array slice in BE
        /*
        Expr expr = (Expr) visit(context.primaryExpression());

        IntLiteral lowerBound;
        if (context.start != null) {
            lowerBound = new IntLiteral(Long.parseLong(context.start.getText()));
        } else {
            lowerBound = new IntLiteral(0);
        }
        IntLiteral upperBound;
        if (context.end != null) {
            upperBound = new IntLiteral(Long.parseLong(context.end.getText()));
        } else {
            upperBound = new IntLiteral(-1);
        }

        return new ArraySliceExpr(expr, lowerBound, upperBound);
         */
    }

    @Override
    public ParseNode visitInterval(StarRocksParser.IntervalContext context) {
        return new IntervalLiteral((Expr) visit(context.value), (UnitIdentifier) visit(context.from));
    }

    @Override
    public ParseNode visitUnitIdentifier(StarRocksParser.UnitIdentifierContext context) {
        return new UnitIdentifier(context.getText());
    }

    // ------------------------------------------- Primary Expression -------------------------------------------

    @Override
    public ParseNode visitColumnReference(StarRocksParser.ColumnReferenceContext context) {
        if (context.identifier() != null) {
            Identifier identifier = (Identifier) visit(context.identifier());
            return new SlotRef(null, identifier.getValue(), identifier.getValue());
        } else {
            QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
            if (qualifiedName.getParts().size() == 3) {
                return new SlotRef(new TableName(qualifiedName.getParts().get(0), qualifiedName.getParts().get(1)),
                        qualifiedName.getParts().get(2),
                        qualifiedName.getParts().get(2));
            } else if (qualifiedName.getParts().size() == 2) {
                return new SlotRef(new TableName(null, qualifiedName.getParts().get(0)),
                        qualifiedName.getParts().get(1),
                        qualifiedName.getParts().get(1));
            } else {
                throw new ParsingException("Unqualified column reference " + qualifiedName);
            }
        }
    }

    @Override
    public ParseNode visitArrowExpression(StarRocksParser.ArrowExpressionContext context) {
        Expr expr = (Expr) visit(context.primaryExpression());
        StringLiteral stringLiteral = (StringLiteral) visit(context.string());

        return new ArrowExpr(expr, stringLiteral);
    }

    @Override
    public ParseNode visitVariable(StarRocksParser.VariableContext context) {
        SetType setType = SetType.DEFAULT;
        if (context.GLOBAL() != null) {
            setType = SetType.GLOBAL;
        } else if (context.LOCAL() != null || context.SESSION() != null) {
            setType = SetType.SESSION;
        }

        return new SysVariableDesc(((Identifier) visit(context.identifier())).getValue(), setType);
    }

    @Override
    public ParseNode visitCollate(StarRocksParser.CollateContext context) {
        return visit(context.primaryExpression());
    }

    @Override
    public ParseNode visitParenthesizedExpression(StarRocksParser.ParenthesizedExpressionContext context) {
        return visit(context.expression());
    }

    @Override
    public ParseNode visitUnquotedIdentifier(StarRocksParser.UnquotedIdentifierContext context) {
        return new Identifier(context.getText());
    }

    @Override
    public ParseNode visitBackQuotedIdentifier(StarRocksParser.BackQuotedIdentifierContext context) {
        return new Identifier(context.getText().replace("`", ""));
    }

    @Override
    public ParseNode visitDigitIdentifier(StarRocksParser.DigitIdentifierContext context) {
        return new Identifier(context.getText());
    }

    // ------------------------------------------- Util Functions -------------------------------------------

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
        return contexts.stream()
                .map(this::visit)
                .map(clazz::cast)
                .collect(toList());
    }

    private <T> List<T> visitIfPresent(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
        if (contexts != null && contexts.size() != 0) {
            return contexts.stream()
                    .map(this::visit)
                    .map(clazz::cast)
                    .collect(toList());
        } else {
            return null;
        }
    }

    private ParseNode visitIfPresent(ParserRuleContext context) {
        if (context != null) {
            return visit(context);
        } else {
            return null;
        }
    }

    private QualifiedName getQualifiedName(StarRocksParser.QualifiedNameContext context) {
        List<String> parts = visit(context.identifier(), Identifier.class).stream()
                .map(Identifier::getValue)
                .collect(Collectors.toList());

        return QualifiedName.of(parts);
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

    private Type getType(StarRocksParser.TypeContext type) {
        if (type.baseType() != null) {
            if (type.baseType().BOOLEAN() != null) {
                return Type.BOOLEAN;
            } else if (type.baseType().TINYINT() != null) {
                return Type.TINYINT;
            } else if (type.baseType().SMALLINT() != null) {
                return Type.SMALLINT;
            } else if (type.baseType().INT() != null || type.baseType().INTEGER() != null) {
                return Type.INT;
            } else if (type.baseType().BIGINT() != null) {
                return Type.BIGINT;
            } else if (type.baseType().LARGEINT() != null) {
                return Type.LARGEINT;
            } else if (type.baseType().FLOAT() != null) {
                return Type.FLOAT;
            } else if (type.baseType().DOUBLE() != null) {
                return Type.DOUBLE;
            } else if (type.baseType().DATE() != null) {
                return Type.DATE;
            } else if (type.baseType().DATETIME() != null) {
                return Type.DATETIME;
            } else if (type.baseType().TIME() != null) {
                return Type.TIME;
            } else if (type.baseType().VARCHAR() != null) {
                if (type.baseType().typeParameter() != null) {
                    return ScalarType.createVarcharType(
                            Integer.parseInt(type.baseType().typeParameter().INTEGER_VALUE().toString()));
                } else {
                    return Type.VARCHAR;
                }
            } else if (type.baseType().CHAR() != null) {
                if (type.baseType().typeParameter() != null) {
                    return ScalarType.createCharType(
                            Integer.parseInt(type.baseType().typeParameter().INTEGER_VALUE().toString()));
                } else {
                    return Type.CHAR;
                }
            } else if (type.baseType().STRING() != null) {
                ScalarType stringType = ScalarType.createVarcharType(ScalarType.DEFAULT_STRING_LENGTH);
                stringType.setAssignedStrLenInColDefinition();
                return stringType;
            } else if (type.baseType().BITMAP() != null) {
                return Type.BITMAP;
            } else if (type.baseType().HLL() != null) {
                return Type.HLL;
            } else if (type.baseType().PERCENTILE() != null) {
                return Type.PERCENTILE;
            } else if (type.baseType().JSON() != null) {
                return Type.JSON;
            }

            return Type.INVALID;
        } else if (type.decimalType() != null) {
            if (type.precision == null) {
                if (type.decimalType().DECIMAL() != null) {
                    return ScalarType.createUnifiedDecimalType(10, 0);
                } else if (type.decimalType().DECIMALV2() != null) {
                    return ScalarType.createDecimalV2Type();
                } else if (type.decimalType().DECIMAL32() != null) {
                    return ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32);
                } else if (type.decimalType().DECIMAL64() != null) {
                    return ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64);
                } else if (type.decimalType().DECIMAL128() != null) {
                    return ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128);
                }
            }

            int precision = Integer.parseInt(type.precision.getText());
            int scale = ScalarType.DEFAULT_SCALE;

            if (type.scale != null) {
                scale = Integer.parseInt(type.scale.getText());
            }

            if (type.decimalType().DECIMAL() != null) {
                return ScalarType.createUnifiedDecimalType(precision, scale);
            } else if (type.decimalType().DECIMALV2() != null) {
                return ScalarType.createDecimalV2Type(precision, scale);
            } else if (type.decimalType().DECIMAL32() != null) {
                return ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, precision, scale);
            } else if (type.decimalType().DECIMAL64() != null) {
                return ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, precision, scale);
            } else if (type.decimalType().DECIMAL128() != null) {
                return ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, precision, scale);
            }
        } else if (type.arrayType() != null) {
            StarRocksParser.ArrayTypeContext arrayTypeContext = type.arrayType();
            return new ArrayType(getType(arrayTypeContext.type()));
        }

        throw new IllegalArgumentException("Unsupported type specification: " + type.getText());
    }
}
