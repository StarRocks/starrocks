// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.UserException;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;

import java.util.Map;

public class AlterResourceStmt extends DdlStmt {
    private static final String TYPE = "type";

    private final String resourceName;
    private final Map<String, String> properties;

    public AlterResourceStmt(String resourceName, Map<String, String> properties) {
        this.resourceName = resourceName;
        this.properties = properties;
    }

    public String getResourceName() {
        return resourceName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // check auth
        if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        // check name
        FeNameFormat.checkResourceName(resourceName);

        // check properties
        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Resource properties can't be null");
        }

        // not allow to modify the resource type
        if (properties.get(TYPE) != null) {
            throw new AnalysisException("Not allow to modify the resource type");
        }

    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER ");
        sb.append("RESOURCE '").append(resourceName).append("' ");
        sb.append("SET PROPERTIES(").append(new PrintableMap<>(properties, "=", true, false)).append(")");
        return sb.toString();
    }
}

