// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Resource;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.ast.CreateResourceStmt;
import org.junit.BeforeClass;

import java.util.Map;

public class HivePlanTestBase extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        mockHiveResource();

        String dbName = "hive_test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        starRocksAssert.withTable("CREATE EXTERNAL TABLE IF NOT EXISTS region  ( r_regionkey  INTEGER,\n" +
                "                            r_name       STRING,\n" +
                "                            r_comment    STRING)\n" +
                "\n" +
                "    ENGINE=hive\n" +
                "properties (\n" +
                "    \"resource\" = \"hive0\",\n" +
                "    \"table\" = \"region\",\n" +
                "    \"database\" = \"tpch_100g\"\n" +
                ");");

        starRocksAssert.withTable("CREATE EXTERNAL TABLE IF NOT EXISTS nation (\n" +
                "  n_nationkey int,\n" +
                "  n_name STRING,\n" +
                "  n_regionkey int,\n" +
                "  n_comment STRING\n" +
                ")\n" +
                "\n" +
                "    ENGINE=hive\n" +
                "properties (\n" +
                "    \"resource\" = \"hive0\",\n" +
                "    \"table\" = \"nation\",\n" +
                "    \"database\" = \"tpch_100g\"\n" +
                ");");

        starRocksAssert.withTable("CREATE EXTERNAL TABLE IF NOT EXISTS customer ( c_custkey     INTEGER,\n" +
                "                             c_name        STRING,\n" +
                "                             c_address     STRING,\n" +
                "                             c_nationkey   INTEGER,\n" +
                "                             c_phone       STRING,\n" +
                "                             c_acctbal     decimal(15,2)  ,\n" +
                "                             c_mktsegment  STRING,\n" +
                "                             c_comment     STRING)\n" +
                "\n" +
                "    ENGINE=hive\n" +
                "properties (\n" +
                "    \"resource\" = \"hive0\",\n" +
                "    \"table\" = \"customer\",\n" +
                "    \"database\" = \"tpch_100g\"\n" +
                ");");

        starRocksAssert.withTable("CREATE EXTERNAL TABLE IF NOT EXISTS lineitem ( l_orderkey    INTEGER,\n" +
                "                             l_partkey     INTEGER,\n" +
                "                             l_suppkey     INTEGER,\n" +
                "                             l_linenumber  INTEGER,\n" +
                "                             l_quantity    decimal(15,2),\n" +
                "                             l_extendedprice  decimal(15,2),\n" +
                "                             l_discount    decimal(15,2),\n" +
                "                             l_tax         decimal(15,2),\n" +
                "                             l_returnflag  STRING,\n" +
                "                             l_linestatus  STRING,\n" +
                "                             l_shipdate    DATE,\n" +
                "                             l_commitdate  DATE,\n" +
                "                             l_receiptdate DATE,\n" +
                "                             l_shipinstruct STRING,\n" +
                "                             l_shipmode     STRING,\n" +
                "                             l_comment      STRING)\n" +
                "\n" +
                "    ENGINE=hive\n" +
                "properties (\n" +
                "    \"resource\" = \"hive0\",\n" +
                "    \"table\" = \"lineitem\",\n" +
                "    \"database\" = \"tpch_100g\"\n" +
                ");");

        starRocksAssert.withTable("CREATE EXTERNAL TABLE IF NOT EXISTS orders  ( o_orderkey       INTEGER,\n" +
                "                           o_custkey        INTEGER,\n" +
                "                           o_orderstatus    STRING,\n" +
                "                           o_totalprice     decimal(15,2),\n" +
                "                           o_orderdate      DATE,\n" +
                "                           o_orderpriority  STRING,\n" +
                "                           o_clerk          STRING,\n" +
                "                           o_shippriority   INTEGER,\n" +
                "                           o_comment        STRING)\n" +
                "\n" +
                "    ENGINE=hive\n" +
                "properties (\n" +
                "    \"resource\" = \"hive0\",\n" +
                "    \"table\" = \"orders\",\n" +
                "    \"database\" = \"tpch_100g\"\n" +
                ");");

        starRocksAssert.withTable("CREATE EXTERNAL TABLE IF NOT EXISTS part  ( p_partkey     INTEGER,\n" +
                "                          p_name        STRING,\n" +
                "                          p_mfgr        STRING,\n" +
                "                          p_brand       STRING,\n" +
                "                          p_type        STRING,\n" +
                "                          p_size        INTEGER,\n" +
                "                          p_container   STRING,\n" +
                "                          p_retailprice decimal(15,2),\n" +
                "                          p_comment     STRING)\n" +
                "\n" +
                "    ENGINE=hive\n" +
                "properties (\n" +
                "    \"resource\" = \"hive0\",\n" +
                "    \"table\" = \"part\",\n" +
                "    \"database\" = \"tpch_100g\"\n" +
                ");");

        starRocksAssert.withTable("CREATE EXTERNAL TABLE IF NOT EXISTS partsupp ( ps_partkey     INTEGER,\n" +
                "                             ps_suppkey     INTEGER,\n" +
                "                             ps_availqty    INTEGER,\n" +
                "                             ps_supplycost  decimal(15,2) ,\n" +
                "                             ps_comment     STRING)\n" +
                "\n" +
                "    ENGINE=hive\n" +
                "properties (\n" +
                "    \"resource\" = \"hive0\",\n" +
                "    \"table\" = \"partsupp\",\n" +
                "    \"database\" = \"tpch_100g\"\n" +
                ");");

        starRocksAssert.withTable("CREATE EXTERNAL TABLE IF NOT EXISTS supplier ( s_suppkey     INTEGER,\n" +
                "                             s_name        STRING,\n" +
                "                             s_address     STRING,\n" +
                "                             s_nationkey   INTEGER,\n" +
                "                             s_phone       STRING,\n" +
                "                             s_acctbal     decimal(15,2),\n" +
                "                             s_comment     STRING)\n" +
                "\n" +
                "    ENGINE=hive\n" +
                "properties (\n" +
                "    \"resource\" = \"hive0\",\n" +
                "    \"table\" = \"supplier\",\n" +
                "    \"database\" = \"tpch_100g\"\n" +
                ");");

        starRocksAssert.withTable("CREATE EXTERNAL TABLE IF NOT EXISTS t1  ( c1  INTEGER,\n" +
                "                            c2       STRING,\n" +
                "                            c3    STRING," +
                "                            par_col   STRING)\n" +
                "\n" +
                "    ENGINE=hive\n" +
                "properties (\n" +
                "    \"resource\" = \"hive0\",\n" +
                "    \"table\" = \"t1\",\n" +
                "    \"database\" = \"partitioned_db\"\n" +
                ");");

        starRocksAssert.withTable("CREATE EXTERNAL TABLE IF NOT EXISTS lineitem_par ( l_orderkey    INTEGER,\n" +
                "                             l_partkey     INTEGER,\n" +
                "                             l_suppkey     INTEGER,\n" +
                "                             l_linenumber  INTEGER,\n" +
                "                             l_quantity    decimal(15,2),\n" +
                "                             l_extendedprice  decimal(15,2),\n" +
                "                             l_discount    decimal(15,2),\n" +
                "                             l_tax         decimal(15,2),\n" +
                "                             l_returnflag  STRING,\n" +
                "                             l_linestatus  STRING,\n" +
                "                             l_shipdate    DATE,\n" +
                "                             l_commitdate  DATE,\n" +
                "                             l_receiptdate DATE,\n" +
                "                             l_shipinstruct STRING,\n" +
                "                             l_shipmode     STRING,\n" +
                "                             l_comment      STRING)\n" +
                "\n" +
                "    ENGINE=hive\n" +
                "properties (\n" +
                "    \"resource\" = \"hive0\",\n" +
                "    \"table\" = \"lineitem_par\",\n" +
                "    \"database\" = \"partitioned_db\"\n" +
                ");");
    }

    private static void mockHiveResource() throws Exception {
        // use mock hive repository
        connectContext.getGlobalStateMgr().setHiveRepository(new MockHiveRepository());

        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", "hive");
        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        CreateResourceStmt createResourceStmt = new CreateResourceStmt(true, "hive0", properties);
        createResourceStmt.setResourceType(Resource.ResourceType.HIVE);
        connectContext.getGlobalStateMgr().getResourceMgr().createResource(createResourceStmt);
    }
}
