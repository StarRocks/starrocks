// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/AdminCheckTabletsStmt.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;

// ADDMIN CHECK TABLET (id1, id2, ...) PROPERTIES ("type" = "check_consistency");
public class AdminCheckTabletsStmt extends DdlStmt {

    private List<Long> tabletIds;
    private Map<String, String> properties;

    public enum CheckType {
        CONSISTENCY; // check the consistency of replicas of tablet

        public static CheckType getTypeFromString(String str) throws AnalysisException {
            try {
                return CheckType.valueOf(str.toUpperCase());
            } catch (Exception e) {
                throw new AnalysisException("Unknown check type: " + str);
            }
        }
    }

    private CheckType type;

    public AdminCheckTabletsStmt(List<Long> tabletIds, Map<String, String> properties) {
        this.tabletIds = tabletIds;
        this.properties = properties;
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    public CheckType getType() {
        return type;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        // check auth
        if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        if (CollectionUtils.isEmpty(tabletIds)) {
            throw new AnalysisException("Tablet id list is empty");
        }

        String typeStr = PropertyAnalyzer.analyzeType(properties);
        if (typeStr == null) {
            throw new AnalysisException("Should specify 'type' property");
        }
        type = CheckType.getTypeFromString(typeStr);

        if (properties != null && !properties.isEmpty()) {
            throw new AnalysisException("Unknown properties: " + properties.keySet());
        }
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
