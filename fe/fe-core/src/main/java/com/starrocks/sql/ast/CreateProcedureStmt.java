// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CreateProcedureStmt extends DdlStmt {
    private static final String IN = "IN";
    private static final String OUT = "OUT";
    private static final String INOUT = "INOUT";

    public static class ProcArgType {
        public ProcArgType(TypeDef type, String inputType) {
            this.type = type;
            this.inputType = inputType;
        }

        private final TypeDef type;
        private final String inputType;

        public TypeDef getType() {
            return type;
        }

        public String getInputType() {
            return inputType;
        }
    }

    private final String name;
    private final List<ProcArgType> args;
    private final Map<String, String> properties;

    private CreateFunctionStmt proc;
    public CreateProcedureStmt(String name, List<ProcArgType> args, Map<String, String> properties) {
        this.name = name;
        this.args = args;
        this.properties = properties;
    }

    public void analyze(ConnectContext context) throws AnalysisException {
        boolean hasOutput = false;
        for (ProcArgType arg : args) {
            if (hasOutput && arg.getInputType().equals(IN)) {
                throw new AnalysisException("IN parameter should before OUT parameters");
            }
            if (arg.getInputType().equals(OUT)) {
                hasOutput = true;
            }
            if (arg.getInputType().equals(INOUT)) {
                throw new AnalysisException("Unsupported INOUT parameter");
            }
        }
        if (!hasOutput) {
            throw new AnalysisException("Unsupported no OUT parameter");
        }
        List<TypeDef> typedefs =
                args.subList(1, args.size()).stream().map(ProcArgType::getType).collect(Collectors.toList());
        FunctionArgsDef funcArgs = new FunctionArgsDef(typedefs, false);
        proc =
                new CreateFunctionStmt("SCALAR", new FunctionName(name), funcArgs,
                        TypeDef.createVarchar(ScalarType.MAX_VARCHAR_LENGTH),
                        null, properties);
        proc.analyze(context);
    }

    public CreateFunctionStmt getProc() {
        return proc;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateProcedure(this, context);
    }

    @Override
    public String toString() {
        return "CreateProcedureStatement{" +
                "name='" + name + '\'' +
                ", properties=" + properties +
                '}';
    }
}
