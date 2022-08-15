// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.execution;

import com.starrocks.analysis.StatementBase;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;

public interface DataDefinitionExecutor {

    ShowResultSet execute(StatementBase stmt, ConnectContext context) throws Exception;

}
