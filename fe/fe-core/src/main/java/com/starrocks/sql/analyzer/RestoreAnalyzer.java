// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.RestoreStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.common.MetaUtils;

public class RestoreAnalyzer {
    // I don't think permission checks are needed
    public static void analyze(RestoreStmt restoreStmt, ConnectContext session) {
    }
}

