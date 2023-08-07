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

import com.google.common.base.Strings;
import com.starrocks.analysis.LabelName;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterRoutineLoadStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

public class AlterRoutineLoadAnalyzer {

    private static final Logger LOG = LogManager.getLogger(AlterRoutineLoadAnalyzer.class);

    private AlterRoutineLoadAnalyzer() {
        throw new IllegalStateException("creating an instance is illegal");
    }

    private static final String NAME_TYPE = "ROUTINE LOAD NAME";

    public static void analyze(AlterRoutineLoadStmt statement, ConnectContext context) {
        LabelName label = statement.getLabelName();
        String dbName = label.getDbName();
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = context.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                throw new SemanticException(PARSER_ERROR_MSG.noDbSelected());
            }
        }

        if (!(context.getDatabase().equalsIgnoreCase(dbName)
                || context.getDatabase().equalsIgnoreCase(""))) {
            throw new SemanticException(PARSER_ERROR_MSG.dbNameNotMatch(context.getDatabase(), dbName));
        }
        LabelName labelName = new LabelName(dbName, statement.getLabel());
        statement.setLabelName(labelName);
        try {
            FeNameFormat.checkCommonName(NAME_TYPE, labelName.getLabelName());
            FeNameFormat.checkLabel(label.getLabelName());
            statement.setRoutineLoadDesc(CreateRoutineLoadStmt.buildLoadDesc(statement.getLoadPropertyList()));
            statement.checkJobProperties();
            statement.checkDataSourceProperties();
        } catch (UserException e) {
            LOG.error(e);
            throw new SemanticException(e.getMessage());
        }
    }
}

