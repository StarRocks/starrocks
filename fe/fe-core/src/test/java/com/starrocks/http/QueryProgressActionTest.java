// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.starrocks.http;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.pseudocluster.PseudoCluster;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;

public class QueryProgressActionTest {

    @Test
    public void testQueryProgressAction() throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 1);
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();

        try {
            //1.init env: create table、enable profile
            stmt.execute("create database IF NOT EXISTS test_db");
            stmt.execute("use test_db");
            stmt.execute("CREATE TABLE IF NOT EXISTS test_table " +
                    "(`col1` varchar(65533),`col2` varchar(65533),`ds` date) ENGINE=OLAP " +
                    "DUPLICATE KEY(`col1`) " +
                    "DISTRIBUTED BY HASH(`col1`) BUCKETS 1 " +
                    "PROPERTIES (\"replication_num\" = \"1\")");
            stmt.execute("set global enable_profile=true");
            System.out.println("1.init env done!");

            //2.check query progress
            //note: the part contains two thread:
            // a.querySqlThread: run a SQL query for 10 seconds
            // b.checkProgressThread: get above query's progress via /api/query/progress api every 3 seconds
            //    if state=Running, then progress_percent should be in [0.00%, 100.00%)
            //    if state=Finished/Error, then progress_percent should be 100.00%
            String querySql = "select count(*) from test_table union all select sleep(10)"; //select sleep(10) for 10 seconds
            Thread querySqlThread = new Thread(() -> {
                try {
                    stmt.execute(querySql);
                } catch (Exception throwables) {
                    throwables.printStackTrace();
                } finally {
                    System.out.println("2.1 check query progress querySqlThread done!");
                }
            });

            Thread checkProgressThread = new Thread(() -> {
                try {
                    int flag = 0;
                    while (true) {
                        stmt.execute("show profilelist limit 1;");
                        if (stmt.getResultSet().next() &&
                                stmt.getResultSet().getString(5).equals(querySql)) {
                            String queryId = stmt.getResultSet().getString(1);
                            System.out.println("testQueryProgressAction: " + queryId);
                            String info = getQueryProgress(stmt, queryId);
                            JsonObject infoJson = JsonParser.parseString(info).getAsJsonObject();
                            String state = infoJson.get("state").getAsString();
                            Double progress = Double.parseDouble(
                                    infoJson.getAsJsonObject("progress_info")
                                            .get("progress_percent").getAsString()
                                            .replace("%", ""));
                            if (state.equals("Running")) {
                                Assert.assertTrue("Progress percent should be between 0.00% and 100.00%",
                                        progress >= 0.00 && progress < 100.00);
                            }
                            if (state.equals("Finished") || state.equals("Error")) {
                                Assert.assertTrue("Progress percent should be equals 100.00%",
                                        progress == 100.00);
                                break;
                            }
                        }
                        Thread.sleep(3000);
                        flag++;
                        if (flag == 10) { //if not completed in 30s(10 * 3s), it is considered a failure.
                            System.out.println("2.2 check query progress CheckProgressThread failed!");
                            Assert.assertTrue("2.2 check query progress CheckProgressThread failed!", false);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    System.out.println("2.2 check query progress CheckProgressThread done!");
                }
            });

            querySqlThread.start();
            checkProgressThread.start();

            try {
                querySqlThread.join();
                checkProgressThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("2.check query progress done!");

            //3.check special case
            //3.1 check not valid parameter
            OkHttpClient client = new OkHttpClient();
            Request request = new Request.Builder()
                    .url(String.format("http://%s:8030/api/query/progress?query_id_111=123", getFeIp(stmt)))
                    .build();
            Response response = client.newCall(request).execute();
            String info1 = response.body().string();
            Assert.assertTrue(info1.contains("not valid parameter"));
            System.out.println("3.1 check not valid parameter done!");

            //3.2 check query id not found
            String info2 = getQueryProgress(stmt, "123");
            Assert.assertTrue(info2.contains("query id 123 not found"));
            System.out.println("3.2 check query id not found done!");

            //3.3 check short circuit query
            //init env: create PK table、enable short circuit query
            stmt.execute("CREATE TABLE IF NOT EXISTS test_short_circuit_query " +
                    "(`col1` int,`col2` varchar(65533),`ds` date) ENGINE=OLAP " +
                    "PRIMARY KEY(`col1`) " +
                    "DISTRIBUTED BY HASH(`col1`) BUCKETS 1 " +
                    "PROPERTIES (\"replication_num\" = \"1\"," +
                    "\"storage_type\" = \"COLUMN_WITH_ROW\")");
            stmt.execute("ADMIN SET FRONTEND CONFIG (\"enable_experimental_rowstore\" = \"true\");");
            stmt.execute("set enable_short_circuit = true");

            stmt.execute("SELECT * FROM test_short_circuit_query WHERE col1=1;");
            stmt.execute("show profilelist limit 1;");
            if (stmt.getResultSet().next()) {
                String queryId = stmt.getResultSet().getString(1);
                String info3 = getQueryProgress(stmt, queryId);
                Assert.assertTrue(info3.contains("short circuit point query doesn't suppot get query progress"));
            }
            System.out.println("3.3 check short circuit query done!");
            System.out.println("3.check special case done!");
        } finally {
            stmt.close();
            connection.close();
            PseudoCluster.getInstance().shutdown(true);
        }
    }

    private String getQueryProgress(Statement stmt, String queryId) throws Exception {
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(String.format("http://%s:8030/api/query/progress?query_id=%s", getFeIp(stmt), queryId))
                .build();
        Response response = client.newCall(request).execute();
        String info = response.body().string();
        return info;
    }

    private String getFeIp(Statement stmt) throws Exception {
        String feIp = "";
        stmt.execute("show frontends;");
        if (stmt.getResultSet().next()) {
            //feIp = stmt.getResultSet().getString(2);
            String col1 = stmt.getResultSet().getString(1);
            String col2 = stmt.getResultSet().getString(2);
            String col3 = stmt.getResultSet().getString(3);
            String col4 = stmt.getResultSet().getString(4);
            String col5 = stmt.getResultSet().getString(5);
            String col6 = stmt.getResultSet().getString(6);
            String col7 = stmt.getResultSet().getString(7);
            String col8 = stmt.getResultSet().getString(8);
            System.out.println("testQueryProgressAction: " + col1 + "_" + col2 + "_" + col3 + "_" +
                    col4 + "_" + col5 + "_" + col6 + "_" + col7 + "_" + col8);
        }
        //return feIp;
        return "127.0.0.1";
    }
}
