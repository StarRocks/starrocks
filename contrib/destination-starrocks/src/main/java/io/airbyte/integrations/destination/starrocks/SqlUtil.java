package io.airbyte.integrations.destination.starrocks;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.base.JavaBaseConstants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class SqlUtil {
    public static Connection createJDBCConnection(JsonNode config) throws ClassNotFoundException, SQLException {
        String dbUrl = String.format(StarRocksConstants.PATTERN_JDBC_URL
                , config.get(StarRocksConstants.KEY_FE_HOST).asText()
                , config.get(StarRocksConstants.KEY_FE_QUERY_PORT).asInt(StarRocksConstants.DEFAULT_FE_QUERY_PORT)
                , config.get(StarRocksConstants.KEY_DB).asText());

        try {
            Class.forName(StarRocksConstants.CJ_JDBC_DRIVER);
        }catch (ClassNotFoundException e){
            Class.forName(StarRocksConstants.JDBC_DRIVER);
        }

        String user = config.get(StarRocksConstants.KEY_USER)==null ?
                StarRocksConstants.DEFAULT_USER :
                config.get(StarRocksConstants.KEY_USER).asText();
        String pwd = config.get(StarRocksConstants.KEY_PWD)==null ?
                StarRocksConstants.DEFAULT_PWD :
                config.get(StarRocksConstants.KEY_PWD).asText();

        return DriverManager.getConnection(dbUrl, user, pwd);
    }

    public static void execute(Connection conn, String sql) throws SQLException {
        try(Statement statement = conn.createStatement()) {
            statement.execute(sql);
        }
    }


    public static void createDatabaseIfNotExist(Connection conn, String db) throws SQLException {
        String sql = String.format("CREATE DATABASE IF NOT EXISTS %s;", db);
        execute(conn, sql);
    }

    public static void truncateTable(Connection conn, String tableName) throws SQLException {
        String sql = String.format("TRUNCATE TABLE %s;", tableName);
        execute(conn, sql);
    }

    public static void insertFromTable(Connection conn,String srcTableName, String dstTableName) throws SQLException {
        String sql = String.format("INSERT INTO %s SELECT * FROM %s;",  dstTableName, srcTableName);
        execute(conn, sql);
    }

    public static void renameTable(Connection conn, String srcTableName, String dstTableName) throws SQLException {
        String sql = String.format("ALTER TABLE %s RENAME %s;", srcTableName, dstTableName);
        execute(conn, sql);
    }

    public static void createTableIfNotExist(Connection conn, String tableName) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS " + tableName + " ( \n"
                + "`" + JavaBaseConstants.COLUMN_NAME_AB_ID + "` varchar(40),\n"
                + "`" + JavaBaseConstants.COLUMN_NAME_EMITTED_AT + "` BIGINT,\n"
                + "`" + JavaBaseConstants.COLUMN_NAME_DATA + "` String)\n"
                + "DUPLICATE KEY(`" + JavaBaseConstants.COLUMN_NAME_AB_ID + "`,`" + JavaBaseConstants.COLUMN_NAME_EMITTED_AT + "`) \n"
                + "DISTRIBUTED BY HASH(`" + JavaBaseConstants.COLUMN_NAME_AB_ID + "`) BUCKETS 16 \n"
                + "PROPERTIES ( \n"
                + "\"replication_num\" = \"1\" \n"
                + ");";
        execute(conn, sql);
    }

    public static void dropTableIfExists(Connection conn, String tableName) throws SQLException {
        String sql = String.format("DROP TABLE IF EXISTS `%s`;", tableName);
        execute(conn, sql);
    }

}
