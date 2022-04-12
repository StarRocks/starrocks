// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.RefreshType;
import com.starrocks.common.AnalysisException;
import org.apache.commons.lang.NotImplementedException;

public class RefreshSchemeDesc implements ParseNode {
    protected RefreshType type;

    public void analyze() throws AnalysisException {
        throw new NotImplementedException();
    }

    public RefreshType getType() {
        return type;
    }

    public String toSql() {
        throw new NotImplementedException();
    }


}
