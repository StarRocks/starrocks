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
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.StarRocksException;
import com.starrocks.load.Load;
import com.starrocks.load.RoutineLoadDesc;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.LabelName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class CreateRoutineLoadAnalyzer {

    private static final Logger LOG = LogManager.getLogger(CreateRoutineLoadAnalyzer.class);
    private static final String NAME_TYPE = "ROUTINE LOAD NAME";

    private CreateRoutineLoadAnalyzer() {
        throw new IllegalStateException("creating an instance is illegal");
    }

    public static void analyze(CreateRoutineLoadStmt statement, ConnectContext context) {
        LabelName label = statement.getLabelName();
        String dbName = label.getDbName();
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = context.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        if (Strings.isNullOrEmpty(statement.getTableName())) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR);
        }
        statement.setDBName(dbName);
        statement.setName(label.getLabelName());
        try {
            FeNameFormat.checkCommonName(NAME_TYPE, label.getLabelName());
            FeNameFormat.checkLabel(label.getLabelName());
            statement.setRoutineLoadDesc(CreateRoutineLoadStmt.buildLoadDesc(statement.getLoadPropertyList()));
            statement.checkJobProperties();
            statement.checkDataSourceProperties();
            RoutineLoadDesc routineLoadDesc = statement.getRoutineLoadDesc();
            List<ImportColumnDesc> metaColumnDescs =
                    (routineLoadDesc != null && routineLoadDesc.getColumnsInfo() != null)
                            ? routineLoadDesc.getColumnsInfo().getColumns()
                            : null;
            Load.validateStreamMetaFunctions(metaColumnDescs, statement.getTypeName(), statement.getFormat(),
                    statement.getUseNativeAvroReader());
            // Schema evolution maps payload fields to columns by name. A plain column list in COLUMNS
            // pins the loaded column set instead, so an added column would never start loading and the
            // BE would re-escalate the same schema forever. Expression mappings pin nothing: the
            // payload columns still come from the live table schema.
            if (Boolean.TRUE.equals(statement.getEnableAvroSchemaEvolution()) && metaColumnDescs != null
                    && metaColumnDescs.stream().anyMatch(ImportColumnDesc::isColumn)) {
                throw new StarRocksException(CreateRoutineLoadStmt.AVRO_ENABLE_SCHEMA_EVOLUTION
                        + " maps payload fields to columns by name and cannot be combined with an explicit"
                        + " column list in COLUMNS; use one or the other (expression mappings in COLUMNS"
                        + " are still allowed)");
            }
        } catch (StarRocksException e) {
            LOG.error(e.getMessage(), e);
            throw new SemanticException(e.getMessage());
        }
    }
}
