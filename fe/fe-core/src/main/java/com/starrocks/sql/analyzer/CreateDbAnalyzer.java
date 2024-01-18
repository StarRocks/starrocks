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
import com.starrocks.cloudnative.storagevolume.StorageVolumeMgr;
import com.starrocks.common.error.ErrorCode;
import com.starrocks.common.error.ErrorReport;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.common.MetaUtils;

import java.util.Map;

public class CreateDbAnalyzer {
    public static void analyze(CreateDbStmt statement, ConnectContext context) {
        String dbName = statement.getFullDbName();
        FeNameFormat.checkDbName(dbName);

        String catalogName = statement.getCatalogName();
        if (Strings.isNullOrEmpty(catalogName)) {
            catalogName = context.getCurrentCatalog();
            statement.setCatalogName(catalogName);
        }

        try {
            MetaUtils.checkCatalogExistAndReport(catalogName);
        } catch (AnalysisException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalogName);
        }

        Map<String, String> properties = statement.getProperties();
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME)) {
            String volume = properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME);
            if (RunMode.isSharedNothingMode() && !StorageVolumeMgr.LOCAL.equalsIgnoreCase(volume)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Storage volume can only be 'local' in shared nothing mode");
            }
        }
    }
}
