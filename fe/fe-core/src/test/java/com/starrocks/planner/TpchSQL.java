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

package com.starrocks.planner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class TpchSQL {
    private static String from(String name) {
        String path = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();
        File file = new File(path + "/tpchsql/q" + name + ".sql");
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String str;
            while ((str = reader.readLine()) != null) {
                sb.append(str).append("\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        SQL_MAP.put("q" + name, sb.toString());
        return sb.toString();
    }

    private static final Map<String, String> SQL_MAP = new LinkedHashMap<>();
    public static final String Q01 = from("1");
    public static final String Q02 = from("2");
    public static final String Q03 = from("3");
    public static final String Q04 = from("4");
    public static final String Q05 = from("5");
    public static final String Q06 = from("6");
    public static final String Q07 = from("7");
    public static final String Q08 = from("8");
    public static final String Q09 = from("9");
    public static final String Q10 = from("10");
    public static final String Q11 = from("11");
    public static final String Q12 = from("12");
    public static final String Q13 = from("13");
    public static final String Q14 = from("14");
    public static final String Q15 = from("15");
    public static final String Q16 = from("16");
    public static final String Q17 = from("17");
    public static final String Q18 = from("18");
    public static final String Q19 = from("19");
    public static final String Q20 = from("20");
    public static final String Q21 = from("21");
    public static final String Q22 = from("22");

    public static String getSQL(String name) {
        return SQL_MAP.get(name);
    }

    public static boolean contains(String name) {
        return SQL_MAP.containsKey(name);
    }

    public static Map<String, String> getAllSQL() {
        return SQL_MAP;
    }

    public static final String REGION = "CREATE TABLE region ( R_REGIONKEY  INTEGER NOT NULL,\n" +
            "                            R_NAME       CHAR(25) NOT NULL,\n" +
            "                            R_COMMENT    VARCHAR(152),\n" +
            "                            PAD char(1) NOT NULL)\n" +
            "ENGINE=OLAP\n" +
            "DUPLICATE KEY(`r_regionkey`)\n" +
            "COMMENT \"OLAP\"\n" +
            "DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\"\n" +
            ");";

    public static final String SUPPLIER = "CREATE TABLE supplier ( S_SUPPKEY     INTEGER NOT NULL,\n" +
            "                             S_NAME        CHAR(25) NOT NULL,\n" +
            "                             S_ADDRESS     VARCHAR(40) NOT NULL, \n" +
            "                             S_NATIONKEY   INTEGER NOT NULL,\n" +
            "                             S_PHONE       CHAR(15) NOT NULL,\n" +
            "                             S_ACCTBAL     double NOT NULL,\n" +
            "                             S_COMMENT     VARCHAR(101) NOT NULL,\n" +
            "                             PAD char(1) NOT NULL)\n" +
            "ENGINE=OLAP\n" +
            "DUPLICATE KEY(`s_suppkey`)\n" +
            "COMMENT \"OLAP\"\n" +
            "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 1\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\"\n" +
            ");";

    public static final String PARTSUPP = "CREATE TABLE partsupp ( PS_PARTKEY     INTEGER NOT NULL,\n" +
            "                             PS_SUPPKEY     INTEGER NOT NULL,\n" +
            "                             PS_AVAILQTY    INTEGER NOT NULL,\n" +
            "                             PS_SUPPLYCOST  double  NOT NULL,\n" +
            "                             PS_COMMENT     VARCHAR(199) NOT NULL,\n" +
            "                             PAD char(1) NOT NULL)\n" +
            "ENGINE=OLAP\n" +
            "DUPLICATE KEY(`ps_partkey`)\n" +
            "COMMENT \"OLAP\"\n" +
            "DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 10\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\"\n" +
            ");";

    public static final String ORDERS = "CREATE TABLE orders  ( O_ORDERKEY       INTEGER NOT NULL,\n" +
            "                           O_CUSTKEY        INTEGER NOT NULL,\n" +
            "                           O_ORDERSTATUS    CHAR(1) NOT NULL,\n" +
            "                           O_TOTALPRICE     double NOT NULL,\n" +
            "                           O_ORDERDATE      DATE NOT NULL,\n" +
            "                           O_ORDERPRIORITY  CHAR(15) NOT NULL,  \n" +
            "                           O_CLERK          CHAR(15) NOT NULL, \n" +
            "                           O_SHIPPRIORITY   INTEGER NOT NULL,\n" +
            "                           O_COMMENT        VARCHAR(79) NOT NULL,\n" +
            "                           PAD char(1) NOT NULL)\n" +
            "ENGINE=OLAP\n" +
            "DUPLICATE KEY(`o_orderkey`)\n" +
            "COMMENT \"OLAP\"\n" +
            "DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 10\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\"\n" +
            ");";

    public static final String CUSTOMER ="CREATE TABLE customer ( C_CUSTKEY     INTEGER NOT NULL,\n" +
            "                             C_NAME        VARCHAR(25) NOT NULL,\n" +
            "                             C_ADDRESS     VARCHAR(40) NOT NULL,\n" +
            "                             C_NATIONKEY   INTEGER NOT NULL,\n" +
            "                             C_PHONE       CHAR(15) NOT NULL,\n" +
            "                             C_ACCTBAL     double   NOT NULL,\n" +
            "                             C_MKTSEGMENT  CHAR(10) NOT NULL,\n" +
            "                             C_COMMENT     VARCHAR(117) NOT NULL,\n" +
            "                             PAD char(1) NOT NULL)\n" +
            "ENGINE=OLAP\n" +
            "DUPLICATE KEY(`c_custkey`)\n" +
            "COMMENT \"OLAP\"\n" +
            "DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 10\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\"\n" +
            ");";

    public static final String NATION = "CREATE TABLE `nation` (\n" +
            "  `N_NATIONKEY` int(11) NOT NULL COMMENT \"\",\n" +
            "  `N_NAME` char(25) NOT NULL COMMENT \"\",\n" +
            "  `N_REGIONKEY` int(11) NOT NULL COMMENT \"\",\n" +
            "  `N_COMMENT` varchar(152) NULL COMMENT \"\",\n" +
            "  `PAD` char(1) NOT NULL COMMENT \"\"\n" +
            ") ENGINE=OLAP\n" +
            "DUPLICATE KEY(`N_NATIONKEY`)\n" +
            "COMMENT \"OLAP\"\n" +
            "DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\"\n" +
            ");";

    public static final String PART = "CREATE TABLE part  ( P_PARTKEY     INTEGER NOT NULL,\n" +
            "                          P_NAME        VARCHAR(55) NOT NULL,\n" +
            "                          P_MFGR        CHAR(25) NOT NULL,\n" +
            "                          P_BRAND       CHAR(10) NOT NULL,\n" +
            "                          P_TYPE        VARCHAR(25) NOT NULL,\n" +
            "                          P_SIZE        INTEGER NOT NULL,\n" +
            "                          P_CONTAINER   CHAR(10) NOT NULL,\n" +
            "                          P_RETAILPRICE double NOT NULL,\n" +
            "                          P_COMMENT     VARCHAR(23) NOT NULL,\n" +
            "                          PAD char(1) NOT NULL)\n" +
            "ENGINE=OLAP\n" +
            "DUPLICATE KEY(`p_partkey`)\n" +
            "COMMENT \"OLAP\"\n" +
            "DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 10\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\"\n" +
            ");";

    public static final String LINEITEM = "CREATE TABLE lineitem ( L_ORDERKEY    INTEGER NOT NULL,\n" +
            "                             L_PARTKEY     INTEGER NOT NULL,\n" +
            "                             L_SUPPKEY     INTEGER NOT NULL,\n" +
            "                             L_LINENUMBER  INTEGER NOT NULL,\n" +
            "                             L_QUANTITY    double NOT NULL,\n" +
            "                             L_EXTENDEDPRICE  double NOT NULL,\n" +
            "                             L_DISCOUNT    double NOT NULL,\n" +
            "                             L_TAX         double NOT NULL,\n" +
            "                             L_RETURNFLAG  CHAR(1) NOT NULL,\n" +
            "                             L_LINESTATUS  CHAR(1) NOT NULL,\n" +
            "                             L_SHIPDATE    DATE NOT NULL,\n" +
            "                             L_COMMITDATE  DATE NOT NULL,\n" +
            "                             L_RECEIPTDATE DATE NOT NULL,\n" +
            "                             L_SHIPINSTRUCT CHAR(25) NOT NULL,\n" +
            "                             L_SHIPMODE     CHAR(10) NOT NULL,\n" +
            "                             L_COMMENT      VARCHAR(44) NOT NULL,\n" +
            "                             PAD char(1) NOT NULL)\n" +
            "ENGINE=OLAP\n" +
            "DUPLICATE KEY(`l_orderkey`)\n" +
            "COMMENT \"OLAP\"\n" +
            "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 20\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\"\n" +
            ");";
}
