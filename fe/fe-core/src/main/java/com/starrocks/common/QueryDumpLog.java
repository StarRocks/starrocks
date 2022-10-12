// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryDumpLog {
    public static final QueryDumpLog QUERY_DUMP = new QueryDumpLog("dump.query");
    private Logger logger;

    public QueryDumpLog(String dumpName) {
        logger = LogManager.getLogger(dumpName);
    }

    public static QueryDumpLog getQueryDump() {
        return QUERY_DUMP;
    }

    public void log(String message) {
        logger.info(message);
    }
}
