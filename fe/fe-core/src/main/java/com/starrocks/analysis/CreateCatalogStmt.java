package com.starrocks.analysis;

import com.starrocks.catalog.Resource;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;

import java.util.Arrays;
import java.util.Map;

public class CreateCatalogStmt extends DdlStmt {
    private static final String TYPE = "type";

    private final String catalogName;
    private final Map<String, String> properties;
    private String catalogType;
    
    public CreateCatalogStmt(String catalogName, Map<String, String> properties) {
        this.catalogName = catalogName;
        this.properties = properties;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getCatalogType() {
        return catalogType;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // check auth
        if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        // check name
        FeNameFormat.checkResourceName(catalogName);

        // check type in properties
        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("catalog properties can't be null");
        }

        String type = properties.get(TYPE);
        if (type == null) {
            throw new AnalysisException("catalog type can't be null");
        }
        catalogType = type;
    }
}
