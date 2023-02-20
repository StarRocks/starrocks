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

import com.google.common.collect.ImmutableSet;
import com.starrocks.common.util.LoadPriority;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterLoadStmt;
import com.starrocks.sql.ast.LoadStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Optional;

public class AlterLoadAnalyzer {

    private static final Logger LOG = LogManager.getLogger(AlterLoadAnalyzer.class);

    private static final String NAME_TYPE = "ROUTINE LOAD NAME";

    public static final ImmutableSet<String> CONFIGURABLE_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(LoadStmt.PRIORITY)
            .build();

    private AlterLoadAnalyzer() {
        throw new IllegalStateException("creating an instance is illegal");
    }

    public static void analyze(AlterLoadStmt statement, ConnectContext context) {
        statement.setDbName(AnalyzerUtils.getOrDefaultDatabase(statement.getDbName(), context));
        FeNameFormat.checkLabel(statement.getLabel());
        FeNameFormat.checkCommonName(NAME_TYPE, statement.getLabel());


        Map<String, String> jobProperties = statement.getJobProperties();
        Optional<String> optional = jobProperties.keySet().stream().filter(
                entity -> !CONFIGURABLE_PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new SemanticException(optional.get() + " is invalid property");
        }

        if (jobProperties.containsKey(LoadStmt.PRIORITY)) {
            final String priorityProperty = jobProperties.get(LoadStmt.PRIORITY);
            if (priorityProperty != null) {
                if (LoadPriority.priorityByName(priorityProperty) == null) {
                    throw new SemanticException(LoadStmt.PRIORITY + " should in HIGHEST/HIGH/NORMAL/LOW/LOWEST");
                }
            }
            statement.getAnalyzedJobProperties().put(LoadStmt.PRIORITY, priorityProperty);
        }

        if (statement.getAnalyzedJobProperties().isEmpty()) {
            throw new SemanticException("No properties are specified");
        }
    }
}

