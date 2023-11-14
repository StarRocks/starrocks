package com.starrocks.connector.odps;

import com.starrocks.connector.hive.HiveTableName;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class OdpsTableName extends HiveTableName {
    public OdpsTableName(String databaseName, String tableName) {
        super(databaseName, tableName);
    }

    public static OdpsTableName of(String databaseName, String tableName) {
        return new OdpsTableName(databaseName, tableName);
    }
}
