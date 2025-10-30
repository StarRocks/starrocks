package com.starrocks.service.arrow.flight.sql;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class JavaTest {
    public static void main(String[] args) {

        String url = "jdbc:arrow-flight-sql://127.0.0.1:9408/testns?useEncryption=false&useSSL=false";
        String user = "";
        String password = "";

        String query = "SELECT * FROM INFORMATION_SCHEMA.TABLES;"; // Or any query you like
        try {
            // Load Arrow Flight SQL JDBC driver
            Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
        } catch (Exception e) {
            e.printStackTrace();
        }


        try (Connection con = DriverManager.getConnection(url, user, password);
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {

            if (rs.next()) {
                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();
                System.out.println("First row:");

                for (int i = 1; i <= colCount; i++) {
                    String colName = meta.getColumnName(i);
                    Object value = rs.getObject(i);
                    System.out.println(colName + ": " + value);
                }
            } else {
                System.out.println("No results found.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}