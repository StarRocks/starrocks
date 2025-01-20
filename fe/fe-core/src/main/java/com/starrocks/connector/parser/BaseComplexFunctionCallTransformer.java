package com.starrocks.connector.parser;

import com.starrocks.analysis.Expr;


public abstract class BaseComplexFunctionCallTransformer {

    protected abstract Expr transform(String functionName, Expr... args);
}
