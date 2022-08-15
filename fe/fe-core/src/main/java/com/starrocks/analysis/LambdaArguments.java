package com.starrocks.analysis;
import com.google.common.base.Preconditions;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.thrift.TExprNode;

import java.util.List;
import java.util.stream.Collectors;

public class LambdaArguments extends Expr {
    public List<String> getNames() {
        return names;
    }

    List<String> names;
    List<PlaceHolderExpr> arguments;

    public void putArguments(List<PlaceHolderExpr> arguments) {
        this.arguments = arguments;
    }

    public List<PlaceHolderExpr> getArguments() {
        return arguments;
    }

    public LambdaArguments(List<String> name) {
        this.names = name;
    }

    public LambdaArguments(LambdaArguments rhs) {
        super(rhs);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        Preconditions.checkState(false, "unreachable");
    }

    @Override
    protected String toSqlImpl() {
        Preconditions.checkState(names.size() > 0);
        String name = names.get(0);
        if (children.size() > 2) {
            name = "(" + names.stream().map(String::valueOf).collect(Collectors.joining(",")) + ")";
        }
        return String.format("%s", name);
    }

    @Override
    protected void toThrift(TExprNode msg) {
        throw new StarRocksPlannerException("not support", ErrorType.INTERNAL_ERROR);
    }

    @Override
    public Expr clone() {
        return new LambdaArguments(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLambdaArguments(this, context);
    }
}
