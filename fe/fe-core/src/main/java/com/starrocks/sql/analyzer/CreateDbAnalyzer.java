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

<<<<<<< HEAD
import com.starrocks.common.AnalysisException;
=======
import com.google.common.base.Strings;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.common.MetaUtils;
<<<<<<< HEAD
import org.apache.parquet.Strings;
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

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

<<<<<<< HEAD
        try {
            MetaUtils.checkCatalogExistAndReport(catalogName);
        } catch (AnalysisException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalogName);
        }
=======
        MetaUtils.checkCatalogExistAndReport(catalogName);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

        Map<String, String> properties = statement.getProperties();
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME)) {
            String volume = properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME);
<<<<<<< HEAD
            if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING && !StorageVolumeMgr.LOCAL.equalsIgnoreCase(volume)) {
=======
            if (RunMode.isSharedNothingMode() && !StorageVolumeMgr.LOCAL.equalsIgnoreCase(volume)) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Storage volume can only be 'local' in shared nothing mode");
            }
        }
    }
}
