// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;

import java.util.List;

public class PartitionNamesAnalyzer {
    public static void analyze(ParseNode parseNode, ConnectContext session) {
        new PartitionNamesAnalyzerVisitor().analyze(parseNode, session);
    }

    static class PartitionNamesAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(ParseNode parseNode, ConnectContext session) {
            visit(parseNode, session);
        }

        @Override
        public Void visitPartitionNames(PartitionNames statement, ConnectContext context) {
            List<String> partitionNames = statement.getPartitionNames();
            if (partitionNames.isEmpty()) {
                throw new SemanticException("No partition specifed in partition lists");
            }
            // check if partition name is not empty string
            if (partitionNames.stream().anyMatch(entity -> Strings.isNullOrEmpty(entity))) {
                throw new SemanticException("there are empty partition name");
            }
            return null;
        }
    }
}
