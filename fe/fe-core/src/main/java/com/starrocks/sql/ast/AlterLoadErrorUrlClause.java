// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;
import com.starrocks.load.LoadErrorHub;

import java.util.Map;

// FORMAT:
//   ALTER SYSTEM SET LOAD ERRORS HUB properties("type" = "xxx");

@Deprecated
public class AlterLoadErrorUrlClause extends AlterClause {
    private final Map<String, String> properties;
    private LoadErrorHub.Param param;

    public AlterLoadErrorUrlClause(Map<String, String> properties) {
        super(AlterOpType.ALTER_OTHER);
        this.properties = properties;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }
}
