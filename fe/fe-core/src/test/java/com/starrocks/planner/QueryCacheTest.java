// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.DEFAULT_CREATE_TABLE_TEMPLATE;

public class QueryCacheTest {
    private static ConnectContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        String createTbl0StmtStr = "" +
                "CREATE TABLE if not exists t0(\n" +
                "dt DATE NOT NULL,\n" +
                "c1 VARCHAR NOT NULL,\n" +
                "c2 CHAR  NOT NULL,\n" +
                "c3 INT NOT NULL,\n" +
                "c4 BIGINT NOT NULL,\n" +
                "v1 INT NOT NULL,\n" +
                "v2 DECIMAL(7,2) NOT NULL,\n" +
                "v3 DECIMAL(15,3) NOT NULL,\n" +
                "v4 DECIMAL(33,4) NOT NULL,\n" +
                "v5 DOUBLE NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dt`, `c1`, `c2`, `c3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(dt) (\n" +
                "  START (\"2022-01-01\") END (\"2022-03-01\") EVERY (INTERVAL 1 day))\n" +
                "DISTRIBUTED BY HASH(`c1`, `c2`, `c3`, `c4`) BUCKETS 10\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"default\"\n" +
                ");";

        String createTbl1StmtStr = "" +
                "CREATE TABLE if not exists t1(\n" +
                "ts DATETIME NOT NULL,\n" +
                "c1 VARCHAR NOT NULL,\n" +
                "c2 CHAR  NOT NULL,\n" +
                "c3 INT NOT NULL,\n" +
                "c4 BIGINT NOT NULL,\n" +
                "v1 INT NOT NULL,\n" +
                "v2 DECIMAL(7,2) NOT NULL,\n" +
                "v3 DECIMAL(15,3) NOT NULL,\n" +
                "v4 DECIMAL(33,4) NOT NULL,\n" +
                "v5 DOUBLE NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`ts`, `c1`, `c2`, `c3`, `c4`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(ts) (\n" +
                "  START (\"2022-01-01\") END (\"2022-03-01\") EVERY (INTERVAL 1 day))\n" +
                "DISTRIBUTED BY HASH(`c1`, `c2`, `c3`, `c4`) BUCKETS 10\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"colocate_with\" = \"cg0\",\n" +
                "\"storage_format\" = \"default\"\n" +
                ");";

        String createTbl2StmtStr = "" +
                "CREATE TABLE if not exists t2(\n" +
                "c1 INT NOT NULL,\n" +
                "c2 BIGINT NOT NULL,\n" +
                "v1 DECIMAL(7, 2) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c1`, `c2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(c1) (\n" +
                "  START (\"1\") END (\"100\") EVERY (10))\n" +
                "DISTRIBUTED BY HASH(`c1`, `c2`) BUCKETS 10\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"default\"\n" +
                ");";

        String createTbl3StmtStr = "" +
                "CREATE TABLE if not exists t3(\n" +
                "c1 INT NOT NULL,\n" +
                "c2 BIGINT NOT NULL,\n" +
                "v1 DECIMAL(7, 2) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c1`, `c2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(c1, c2) (\n" +
                "  partition p1 values [('0', '0'), ('10', '10')),\n" +
                "  partition p2 values [('10', '10'), ('20', '20')),\n" +
                "  partition p3 values [('20', '20'), ('30', '30'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`c1`, `c2`) BUCKETS 10\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"default\"\n" +
                ");";

        String createTbl4StmtStr = "" +
                "CREATE TABLE if not exists t4(\n" +
                "ts DATETIME NOT NULL,\n" +
                "c1 VARCHAR NOT NULL,\n" +
                "v1 INT NOT NULL,\n" +
                "v2 DECIMAL(7,2) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`ts`, `c1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(ts) (\n" +
                "  START (\"2022-01-01\") END (\"2022-03-01\") EVERY (INTERVAL 1 day))\n" +
                "DISTRIBUTED BY HASH(`c1`) BUCKETS 10\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"default\"\n" +
                ");";

        String createTbl5StmtStr = "" +
                "CREATE TABLE if not exists t5(\n" +
                "ts DATETIME NOT NULL,\n" +
                "c1 VARCHAR NOT NULL,\n" +
                "v1 INT NOT NULL,\n" +
                "v2 DECIMAL(7,2) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "UNIQUE KEY(`ts`, `c1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(ts) (\n" +
                "  START (\"2022-01-01\") END (\"2022-03-01\") EVERY (INTERVAL 1 day))\n" +
                "DISTRIBUTED BY HASH(`c1`) BUCKETS 10\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"default\"\n" +
                ");";
        String createTbl6StmtStr = "" +
                "CREATE TABLE if not exists t6(\n" +
                "ts DATETIME NOT NULL,\n" +
                "c1 VARCHAR NOT NULL,\n" +
                "v1 INT REPLACE NOT NULL ,\n" +
                "v2 DECIMAL(7,2) SUM NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`ts`, `c1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(ts) (\n" +
                "  START (\"2022-01-01\") END (\"2022-03-01\") EVERY (INTERVAL 1 day))\n" +
                "DISTRIBUTED BY HASH(`c1`) BUCKETS 10\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"default\"\n" +
                ");";
        String createTbl7StmtStr = "" +
                "CREATE TABLE if not exists t7(\n" +
                "ts DATETIME NOT NULL,\n" +
                "c1 VARCHAR NOT NULL,\n" +
                "v1 INT SUM NOT NULL,\n" +
                "v2 HLL HLL_UNION NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`ts`, `c1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(ts) (\n" +
                "  START (\"2022-01-01\") END (\"2022-03-01\") EVERY (INTERVAL 1 day))\n" +
                "DISTRIBUTED BY HASH(`c1`) BUCKETS 10\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"default\"\n" +
                ");";

        String createTbl8StmtStr = "" +
                "CREATE TABLE `t8` (\n" +
                "  `dt` datetime NOT NULL COMMENT \"\",\n" +
                "  `id` int(11) NOT NULL COMMENT \"\",\n" +
                "  `name` varchar(32) NOT NULL COMMENT \"\",\n" +
                "  `quantity` int(11) NOT NULL COMMENT \"\",\n" +
                "  `price` decimal64(7, 2) NOT NULL COMMENT \"\",\n" +
                "  `tax` decimal64(7, 2) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`dt`, `id`, `name`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`dt`, `id`, `name`) BUCKETS 100 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");";

        String hits = "" +
                " CREATE TABLE `hits` (\n" +
                "  `CounterID` int(11) NULL COMMENT \"\",\n" +
                "  `EventDate` date NOT NULL COMMENT \"\",\n" +
                "  `UserID` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `EventTime` datetime NOT NULL COMMENT \"\",\n" +
                "  `WatchID` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `JavaEnable` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `Title` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `GoodEvent` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `ClientIP` int(11) NOT NULL COMMENT \"\",\n" +
                "  `RegionID` int(11) NOT NULL COMMENT \"\",\n" +
                "  `CounterClass` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `OS` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `UserAgent` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `URL` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `Referer` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `IsRefresh` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `RefererCategoryID` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `RefererRegionID` int(11) NOT NULL COMMENT \"\",\n" +
                "  `URLCategoryID` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `URLRegionID` int(11) NOT NULL COMMENT \"\",\n" +
                "  `ResolutionWidth` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `ResolutionHeight` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `ResolutionDepth` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `FlashMajor` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `FlashMinor` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `FlashMinor2` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `NetMajor` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `NetMinor` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `UserAgentMajor` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `UserAgentMinor` varchar(255) NOT NULL COMMENT \"\",\n" +
                "  `CookieEnable` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `JavascriptEnable` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `IsMobile` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `MobilePhone` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `MobilePhoneModel` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `Params` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `IPNetworkID` int(11) NOT NULL COMMENT \"\",\n" +
                "  `TraficSourceID` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `SearchEngineID` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `SearchPhrase` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `AdvEngineID` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `IsArtifical` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `WindowClientWidth` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `WindowClientHeight` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `ClientTimeZone` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `ClientEventTime` datetime NOT NULL COMMENT \"\",\n" +
                "  `SilverlightVersion1` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `SilverlightVersion2` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `SilverlightVersion3` int(11) NOT NULL COMMENT \"\",\n" +
                "  `SilverlightVersion4` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `PageCharset` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `CodeVersion` int(11) NOT NULL COMMENT \"\",\n" +
                "  `IsLink` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `IsDownload` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `IsNotBounce` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `FUniqID` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `OriginalURL` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `HID` int(11) NOT NULL COMMENT \"\",\n" +
                "  `IsOldCounter` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `IsEvent` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `IsParameter` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `DontCountHits` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `WithHash` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `HitColor` char(1) NOT NULL COMMENT \"\",\n" +
                "  `LocalEventTime` datetime NOT NULL COMMENT \"\",\n" +
                "  `Age` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `Sex` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `Income` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `Interests` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `Robotness` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `RemoteIP` int(11) NOT NULL COMMENT \"\",\n" +
                "  `WindowName` int(11) NOT NULL COMMENT \"\",\n" +
                "  `OpenerName` int(11) NOT NULL COMMENT \"\",\n" +
                "  `HistoryLength` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `BrowserLanguage` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `BrowserCountry` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `SocialNetwork` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `SocialAction` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `HTTPError` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `SendTiming` int(11) NOT NULL COMMENT \"\",\n" +
                "  `DNSTiming` int(11) NOT NULL COMMENT \"\",\n" +
                "  `ConnectTiming` int(11) NOT NULL COMMENT \"\",\n" +
                "  `ResponseStartTiming` int(11) NOT NULL COMMENT \"\",\n" +
                "  `ResponseEndTiming` int(11) NOT NULL COMMENT \"\",\n" +
                "  `FetchTiming` int(11) NOT NULL COMMENT \"\",\n" +
                "  `SocialSourceNetworkID` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `SocialSourcePage` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `ParamPrice` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `ParamOrderID` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `ParamCurrency` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `ParamCurrencyID` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `OpenstatServiceName` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `OpenstatCampaignID` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `OpenstatAdID` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `OpenstatSourceID` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `UTMSource` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `UTMMedium` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `UTMCampaign` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `UTMContent` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `UTMTerm` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `FromTag` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `HasGCLID` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `RefererHash` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `URLHash` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `CLID` int(11) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`CounterID`, `EventDate`, `UserID`, `EventTime`, `WatchID`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`EventTime`)\n" +
                "(PARTITION p20130701 VALUES [('2013-07-01 00:00:00'), ('2013-07-02 00:00:00')),\n" +
                "PARTITION p20130702 VALUES [('2013-07-02 00:00:00'), ('2013-07-03 00:00:00')),\n" +
                "PARTITION p20130703 VALUES [('2013-07-03 00:00:00'), ('2013-07-04 00:00:00')),\n" +
                "PARTITION p20130704 VALUES [('2013-07-04 00:00:00'), ('2013-07-05 00:00:00')),\n" +
                "PARTITION p20130705 VALUES [('2013-07-05 00:00:00'), ('2013-07-06 00:00:00')),\n" +
                "PARTITION p20130706 VALUES [('2013-07-06 00:00:00'), ('2013-07-07 00:00:00')),\n" +
                "PARTITION p20130707 VALUES [('2013-07-07 00:00:00'), ('2013-07-08 00:00:00')),\n" +
                "PARTITION p20130708 VALUES [('2013-07-08 00:00:00'), ('2013-07-09 00:00:00')),\n" +
                "PARTITION p20130709 VALUES [('2013-07-09 00:00:00'), ('2013-07-10 00:00:00')),\n" +
                "PARTITION p20130710 VALUES [('2013-07-10 00:00:00'), ('2013-07-11 00:00:00')),\n" +
                "PARTITION p20130711 VALUES [('2013-07-11 00:00:00'), ('2013-07-12 00:00:00')),\n" +
                "PARTITION p20130712 VALUES [('2013-07-12 00:00:00'), ('2013-07-13 00:00:00')),\n" +
                "PARTITION p20130713 VALUES [('2013-07-13 00:00:00'), ('2013-07-14 00:00:00')),\n" +
                "PARTITION p20130714 VALUES [('2013-07-14 00:00:00'), ('2013-07-15 00:00:00')),\n" +
                "PARTITION p20130715 VALUES [('2013-07-15 00:00:00'), ('2013-07-16 00:00:00')),\n" +
                "PARTITION p20130716 VALUES [('2013-07-16 00:00:00'), ('2013-07-17 00:00:00')),\n" +
                "PARTITION p20130717 VALUES [('2013-07-17 00:00:00'), ('2013-07-18 00:00:00')),\n" +
                "PARTITION p20130718 VALUES [('2013-07-18 00:00:00'), ('2013-07-19 00:00:00')),\n" +
                "PARTITION p20130719 VALUES [('2013-07-19 00:00:00'), ('2013-07-20 00:00:00')),\n" +
                "PARTITION p20130720 VALUES [('2013-07-20 00:00:00'), ('2013-07-21 00:00:00')),\n" +
                "PARTITION p20130721 VALUES [('2013-07-21 00:00:00'), ('2013-07-22 00:00:00')),\n" +
                "PARTITION p20130722 VALUES [('2013-07-22 00:00:00'), ('2013-07-23 00:00:00')),\n" +
                "PARTITION p20130723 VALUES [('2013-07-23 00:00:00'), ('2013-07-24 00:00:00')),\n" +
                "PARTITION p20130724 VALUES [('2013-07-24 00:00:00'), ('2013-07-25 00:00:00')),\n" +
                "PARTITION p20130725 VALUES [('2013-07-25 00:00:00'), ('2013-07-26 00:00:00')),\n" +
                "PARTITION p20130726 VALUES [('2013-07-26 00:00:00'), ('2013-07-27 00:00:00')),\n" +
                "PARTITION p20130727 VALUES [('2013-07-27 00:00:00'), ('2013-07-28 00:00:00')),\n" +
                "PARTITION p20130728 VALUES [('2013-07-28 00:00:00'), ('2013-07-29 00:00:00')),\n" +
                "PARTITION p20130729 VALUES [('2013-07-29 00:00:00'), ('2013-07-30 00:00:00')),\n" +
                "PARTITION p20130730 VALUES [('2013-07-30 00:00:00'), ('2013-07-31 00:00:00')),\n" +
                "PARTITION p20130731 VALUES [('2013-07-31 00:00:00'), ('2013-08-01 00:00:00')))\n" +
                "DISTRIBUTED BY HASH(`UserID`) BUCKETS 48\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"false\"\n" +
                ");";

        String createTbl9StmtStr = "" +
                "CREATE TABLE if not exists t9(\n" +
                "REGION_CODE VARCHAR NOT NULL,\n" +
                "REGION_NAME VARCHAR NOT NULL,\n" +
                "v1 INT NOT NULL,\n" +
                "v2 DECIMAL(7,2) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`REGION_CODE`, `REGION_NAME`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`REGION_CODE`, `REGION_NAME`) BUCKETS 10\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"default\"\n" +
                ");";

        ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setEnablePipelineEngine(true);
        ctx.getSessionVariable().setEnableQueryCache(true);
        FeConstants.runningUnitTest = true;
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase(StatsConstants.STATISTICS_DB_NAME)
                .useDatabase(StatsConstants.STATISTICS_DB_NAME)
                .withTable(DEFAULT_CREATE_TABLE_TEMPLATE);
        starRocksAssert.withDatabase("qc_db").useDatabase("qc_db");
        starRocksAssert.withTable(createTbl0StmtStr);
        starRocksAssert.withTable(createTbl1StmtStr);
        starRocksAssert.withTable(createTbl2StmtStr);
        starRocksAssert.withTable(createTbl3StmtStr);
        starRocksAssert.withTable(createTbl4StmtStr);
        starRocksAssert.withTable(createTbl5StmtStr);
        starRocksAssert.withTable(createTbl6StmtStr);
        starRocksAssert.withTable(createTbl7StmtStr);
        starRocksAssert.withTable(createTbl8StmtStr);
        starRocksAssert.withTable(createTbl9StmtStr);
        starRocksAssert.withTable(hits);
    }

    Optional<PlanFragment> getCachedFragment(String sql) {
        ExecPlan plan = null;
        try {
            plan = UtFrameUtils.getPlanAndFragment(ctx, sql).second;
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        Optional<PlanFragment> optFragment = plan.getFragments().stream()
                .filter(f -> f.getCacheParam() != null).findFirst();
        if (!optFragment.isPresent()) {
            System.out.println("wrong query:" + sql);
            try {
                System.out.println("plan:" + UtFrameUtils.getVerboseFragmentPlan(ctx, sql));
            } catch (Throwable ignored) {

            }
        }
        return optFragment;
    }

    private void testHelper(List<String> queryList) {
        List<PlanFragment> frags = queryList.stream()
                .map(q -> getCachedFragment(q).get()).collect(Collectors.toList());
        List<ByteBuffer> digests =
                frags.stream().map(f -> ByteBuffer.wrap(f.getCacheParam().getDigest())).collect(Collectors.toList());
        List<Set<Integer>> slotRemappings =
                frags.stream().map(f -> new HashSet<>(f.getCacheParam().getSlot_remapping().values()))
                        .collect(Collectors.toList());
        ByteBuffer digest = digests.get(0);
        Set<Integer> slotRemapping = slotRemappings.get(0);
        Assert.assertTrue(digest != null && digest.array().length > 0);
        Assert.assertTrue(digests.stream().allMatch(d -> d.equals(digest)));
        Assert.assertTrue(slotRemapping != null && !slotRemapping.isEmpty());
        Assert.assertTrue(slotRemappings.stream().allMatch(s -> s.equals(slotRemapping)));
    }

    private void testNoGroupBy(String aggFunction, List<String> whereClauses) throws Exception {
        List<String> queryList = whereClauses.stream()
                .map(w -> String.format("select %s from t0 %s", aggFunction, w)).collect(Collectors.toList());
        testHelper(queryList);
    }

    private void testGroupBy(String aggFunction, List<String> whereClauses, String groupBy) throws Exception {
        List<String> queryList = whereClauses.stream()
                .map(w -> String.format("select %s, %s from t0 %s group by %s", groupBy, aggFunction, w, groupBy))
                .collect(Collectors.toList());
        testHelper(queryList);
    }

    private List<String> permuteList(List<String> lst) {
        List<List<String>> lstList = Lists.newArrayList(lst, Lists.reverse(lst));
        List<String> shuffleList0 = Lists.newArrayList(lst);
        Collections.shuffle(shuffleList0);
        List<String> shuffleList1 = Lists.newArrayList(lst);
        Collections.shuffle(shuffleList1);
        lstList.add(shuffleList0);
        lstList.add(shuffleList1);
        return lstList.stream().map(l -> String.join(", ", l)).collect(Collectors.toList());
    }

    private void testNoGroupByPermuation(List<String> aggFunctions, List<String> whereClauses) throws Exception {
        List<String> selections = permuteList(aggFunctions);
        List<String> queryList = selections.stream()
                .flatMap(s -> whereClauses.stream().map(w -> String.format("select %s from t0 %s", s, w))).collect(
                        Collectors.toList());
        testHelper(queryList);
    }

    private void testGroupByPermutation(List<String> aggFunctions, List<String> whereClauses,
                                        List<String> groupColumns) throws Exception {
        List<String> selections = permuteList(aggFunctions);
        List<String> groupBys = permuteList(groupColumns);
        List<String> queryList = selections.stream()
                .flatMap(s -> whereClauses.stream().flatMap(
                        w -> groupBys.stream()
                                .map(g -> String.format("select %s, %s from t0 %s group by %s", g, s, w, g))))
                .collect(Collectors.toList());
        testHelper(queryList);
    }

    @Test
    public void testNoGroupBy() throws Exception {
        ctx.getSessionVariable().setNewPlanerAggStage(2);
        List<String> aggrFunctions =
                Lists.newArrayList("count(v1)", "sum(v1)", "avg(v1)", "max(v1)", "min(v1)", "count(distinct v1)",
                        "variance(v1)", "stddev(v1)", "ndv(v1)", "hll_raw_agg(hll_hash(v1))",
                        "bitmap_union(bitmap_hash(v1))", "hll_union_agg(hll_hash(v1))",
                        "bitmap_union_count(bitmap_hash(v1))");
        List<String> whereClauses = Lists.newArrayList("where dt between '2022-01-02' and '2022-01-03'",
                "where dt between '2022-01-01' and '2022-01-31'", "where dt between '2022-01-04' and '2022-01-06'", "");
        for (String agg : aggrFunctions) {
            testNoGroupBy(agg, whereClauses);
        }
    }

    @Test
    public void testNoGroupByAggregationsAppliedToComplexExprs() throws Exception {
        ctx.getSessionVariable().setNewPlanerAggStage(2);
        List<String> aggrFunctions =
                Lists.newArrayList(
                        "sum(if(v1>0, v1, 0))",
                        "max(if(v1>0, v1, 0))",
                        "sum(case when substr(cast(v2 as varchar), 1, 3) = '99' then 1 else 0 end)",
                        "avg(case when substr(cast(v2 as varchar), 1, 3) = '99' then 1 else 0 end)"
                );
        List<String> whereClauses = Lists.newArrayList("where dt between '2022-01-02' and '2022-01-03'",
                "where dt between '2022-01-01' and '2022-01-31'", "where dt between '2022-01-04' and '2022-01-06'", "");
        for (String agg : aggrFunctions) {
            testNoGroupBy(agg, whereClauses);
        }
        testNoGroupByPermuation(aggrFunctions, whereClauses);
    }

    @Test
    public void testNoGroupByDistinct() throws Exception {
        ctx.getSessionVariable().setNewPlanerAggStage(2);
        List<String> aggrFunctions = Lists.newArrayList("count(distinct v1)", "sum(distinct v1)");
        List<String> whereClauses = Lists.newArrayList("where dt between '2022-01-02' and '2022-01-03'",
                "where dt between '2022-01-01' and '2022-01-31'", "where dt between '2022-01-04' and '2022-01-06'", "");
        for (String agg : aggrFunctions) {
            testNoGroupBy(agg, whereClauses);
        }
        ctx.getSessionVariable().setNewPlanerAggStage(4);
        for (String agg : aggrFunctions) {
            testNoGroupBy(agg, whereClauses);
        }
    }

    @Test
    public void testNoGroupByAvgDistinct() throws Exception {
        String agg = "avg(distinct v1)";
        List<String> whereClauses = Lists.newArrayList("where dt between '2022-01-02' and '2022-01-03'",
                "where dt between '2022-01-01' and '2022-01-31'", "where dt between '2022-01-04' and '2022-01-06'", "");
        boolean oldCboCteReuse = ctx.getSessionVariable().isCboCteReuse();
        try {
            ctx.getSessionVariable().setCboCteReuse(false);
            ctx.getSessionVariable().setNewPlanerAggStage(2);
            testNoGroupBy(agg, whereClauses);
            ctx.getSessionVariable().setNewPlanerAggStage(3);
            testNoGroupBy(agg, whereClauses);
        } finally {
            ctx.getSessionVariable().setCboCteReuse(oldCboCteReuse);
        }
    }

    @Test
    public void testNoGroupByMultiAggregations() throws Exception {
        List<String> aggrFunctions =
                Lists.newArrayList("count(v1), sum(v1), avg(v1), max(v1)", "min(v1), count(distinct v1), variance(v1)",
                        "stddev(v1), ndv(v1), hll_raw_agg(hll_hash(v1))",
                        "bitmap_union(bitmap_hash(v1)), hll_union_agg(hll_hash(v1)), bitmap_union_count(bitmap_hash(v1))");
        List<String> whereClauses = Lists.newArrayList("where dt between '2022-01-02' and '2022-01-03'",
                "where dt between '2022-01-01' and '2022-01-31'", "where dt between '2022-01-04' and '2022-01-06'", "");
        ctx.getSessionVariable().setNewPlanerAggStage(2);
        for (String agg : aggrFunctions) {
            testNoGroupBy(agg, whereClauses);
        }
    }

    @Test
    public void testNoGroupByMultiAggregationsPermutation() throws Exception {
        List<List<String>> aggrFunctionsList = Lists.newArrayList(
                Lists.newArrayList("count(v1)", "sum(v2)", "avg(v3)"),
                Lists.newArrayList("max(v3)", "min(v4)", "count(distinct v5)"),
                Lists.newArrayList("ndv(v5)", "hll_raw_agg(hll_hash(v1))"),
                Lists.newArrayList("bitmap_union(bitmap_hash(v2))", "hll_union_agg(hll_hash(v3))"));
        List<String> whereClauses = Lists.newArrayList("where dt between '2022-01-02' and '2022-01-03'",
                "where dt between '2022-01-01' and '2022-01-31'", "where dt between '2022-01-04' and '2022-01-06'", "");
        ctx.getSessionVariable().setNewPlanerAggStage(2);
        for (List<String> aggs : aggrFunctionsList) {
            testNoGroupByPermuation(aggs, whereClauses);
        }
    }

    @Test
    public void testNoGroupByDistinctMultiAggregationsPermutation() throws Exception {
        List<String> aggrFunctions =
                Lists.newArrayList("count(distinct v1)", "sum(distinct v1)", "avg(distinct v1)");

        List<String> whereClauses = Lists.newArrayList("where dt between '2022-01-02' and '2022-01-03'",
                "where dt between '2022-01-01' and '2022-01-31'", "where dt between '2022-01-04' and '2022-01-06'", "");
        boolean oldCboCteReuse = ctx.getSessionVariable().isCboCteReuse();
        try {
            ctx.getSessionVariable().setCboCteReuse(false);
            ctx.getSessionVariable().setNewPlanerAggStage(2);
            testNoGroupByPermuation(aggrFunctions, whereClauses);
            ctx.getSessionVariable().setNewPlanerAggStage(3);
            testNoGroupByPermuation(aggrFunctions, whereClauses);
        } finally {
            ctx.getSessionVariable().setCboCteReuse(oldCboCteReuse);
        }
    }

    @Test
    public void testNoGroupByWithComplexPredicates() throws Exception {
        List<String> complexPredicates = Lists.newArrayList(
                "c1 like 'abc%' and c2 in ('A', 'B', 'C', 'DEF') and c3 % 2 = 1 and c4 < 100",
                "c3 % 2 = 1 and c2 in ('A', 'B', 'C', 'DEF') and c4 < 100 and c1 like 'abc%'");

        List<String> aggrFunctions = Lists.newArrayList("count(v1)", "sum(v2)", "avg(v3)");
        List<String> whereClauses = Lists.newArrayList("where dt between '2022-01-02' and '2022-01-03'",
                "where dt between '2022-01-01' and '2022-01-31'", "where dt between '2022-01-04' and '2022-01-06'",
                "where true");
        whereClauses = whereClauses.stream()
                .flatMap(w -> complexPredicates.stream().map(cp -> String.format("%s and %s", w, cp)))
                .collect(Collectors.toList());
        ctx.getSessionVariable().setNewPlanerAggStage(2);
        testNoGroupByPermuation(aggrFunctions, whereClauses);
    }

    @Test
    public void testGroupByOneColumn() throws Exception {
        ctx.getSessionVariable().setNewPlanerAggStage(2);
        List<String> aggrFunctions =
                Lists.newArrayList("count(v1)", "sum(v1)", "avg(v1)", "max(v1)", "min(v1)", "count(distinct v1)",
                        "variance(v1)", "stddev(v1)", "ndv(v1)", "hll_raw_agg(hll_hash(v1))",
                        "bitmap_union(bitmap_hash(v1))", "hll_union_agg(hll_hash(v1))",
                        "bitmap_union_count(bitmap_hash(v1))");
        List<String> whereClauses = Lists.newArrayList("where dt between '2022-01-02' and '2022-01-03'",
                "where dt between '2022-01-01' and '2022-01-31'", "where dt between '2022-01-04' and '2022-01-06'", "");
        for (String agg : aggrFunctions) {
            testGroupBy(agg, whereClauses, "dt");
        }
    }

    @Test
    public void testGroupByMultiColumns() throws Exception {
        ctx.getSessionVariable().setNewPlanerAggStage(2);
        List<String> aggrFunctions =
                Lists.newArrayList("count(v1)", "sum(v1)", "avg(v1)", "max(v1)", "min(v1)");
        List<String> whereClauses = Lists.newArrayList("where dt between '2022-01-02' and '2022-01-03'",
                "where dt between '2022-01-01' and '2022-01-31'", "where dt between '2022-01-04' and '2022-01-06'", "");
        for (String agg : aggrFunctions) {
            testGroupBy(agg, whereClauses, "dt,c1");
            testGroupBy(agg, whereClauses, "dt,c1,c2");
            testGroupBy(agg, whereClauses, "dt,c1,c2,c3");
            testGroupBy(agg, whereClauses, "dt,c1,c2,c3,c4");
        }
    }

    @Test
    public void testGroupByMultiColumnsDistinct() throws Exception {
        List<String> aggrFunctions =
                Lists.newArrayList("count(distinct v1)", "sum(distinct v1)");
        List<String> whereClauses = Lists.newArrayList("where dt between '2022-01-02' and '2022-01-03'",
                "where dt between '2022-01-01' and '2022-01-31'", "where dt between '2022-01-04' and '2022-01-06'", "");

        ctx.getSessionVariable().setNewPlanerAggStage(2);
        for (String agg : aggrFunctions) {
            testGroupBy(agg, whereClauses, "dt,c1");
            testGroupBy(agg, whereClauses, "dt,c1,c2");
            testGroupBy(agg, whereClauses, "dt,c1,c2,c3");
            testGroupBy(agg, whereClauses, "dt,c1,c2,c3,c4");
        }

        ctx.getSessionVariable().setNewPlanerAggStage(3);
        for (String agg : aggrFunctions) {
            testGroupBy(agg, whereClauses, "dt,c1");
            testGroupBy(agg, whereClauses, "dt,c1,c2");
            testGroupBy(agg, whereClauses, "dt,c1,c2,c3");
            testGroupBy(agg, whereClauses, "dt,c1,c2,c3,c4");
        }
    }

    @Test
    public void testGroupByMultiColumnsAvgDistinct() throws Exception {
        String agg = "avg(distinct v1)";
        List<String> whereClauses = Lists.newArrayList("where dt between '2022-01-02' and '2022-01-03'",
                "where dt between '2022-01-01' and '2022-01-31'", "where dt between '2022-01-04' and '2022-01-06'", "");

        boolean oldCboCteReuse = ctx.getSessionVariable().isCboCteReuse();
        try {
            ctx.getSessionVariable().setCboCteReuse(false);
            ctx.getSessionVariable().setNewPlanerAggStage(2);
            testGroupBy(agg, whereClauses, "dt,c1");
            testGroupBy(agg, whereClauses, "dt,c1,c2");
            testGroupBy(agg, whereClauses, "dt,c1,c2,c3");
            testGroupBy(agg, whereClauses, "dt,c1,c2,c3,c4");

            ctx.getSessionVariable().setNewPlanerAggStage(3);
            testGroupBy(agg, whereClauses, "dt,c1");
            testGroupBy(agg, whereClauses, "dt,c1,c2");
            testGroupBy(agg, whereClauses, "dt,c1,c2,c3");
            testGroupBy(agg, whereClauses, "dt,c1,c2,c3,c4");
        } finally {
            ctx.getSessionVariable().setCboCteReuse(oldCboCteReuse);
        }
    }

    @Test
    public void testGroupByMultiColumnsMultiDistinct() throws Exception {
        String agg = "avg(distinct v1), sum(distinct v2), count(distinct v3)";
        List<String> whereClauses = Lists.newArrayList("where dt between '2022-01-02' and '2022-01-03'",
                "where dt between '2022-01-01' and '2022-01-31'", "where dt between '2022-01-04' and '2022-01-06'", "");

        boolean oldCboCteReuse = ctx.getSessionVariable().isCboCteReuse();
        try {
            ctx.getSessionVariable().setCboCteReuse(false);
            ctx.getSessionVariable().setNewPlanerAggStage(2);
            testGroupBy(agg, whereClauses, "dt,c1");
            testGroupBy(agg, whereClauses, "dt,c1,c2");
            testGroupBy(agg, whereClauses, "dt,c1,c2,c3");
            testGroupBy(agg, whereClauses, "dt,c1,c2,c3,c4");

            ctx.getSessionVariable().setNewPlanerAggStage(3);
            testGroupBy(agg, whereClauses, "dt,c1");
            testGroupBy(agg, whereClauses, "dt,c1,c2");
            testGroupBy(agg, whereClauses, "dt,c1,c2,c3");
            testGroupBy(agg, whereClauses, "dt,c1,c2,c3,c4");
        } finally {
            ctx.getSessionVariable().setCboCteReuse(oldCboCteReuse);
        }
    }

    @Test
    public void testGroupByPermutation() throws Exception {
        List<String> complexPredicates = Lists.newArrayList(
                "c1 like 'abc%' and c2 in ('A', 'B', 'C', 'DEF') and c3 % 2 = 1 and c4 < 100",
                "c3 % 2 = 1 and c2 in ('A', 'B', 'C', 'DEF') and c4 < 100 and c1 like 'abc%'");

        List<String> aggrFunctions = Lists.newArrayList("count(v1)", "sum(v2)", "avg(v3)");
        List<String> whereClauses = Lists.newArrayList("where dt between '2022-01-02' and '2022-01-03'",
                "where dt between '2022-01-01' and '2022-01-31'", "where dt between '2022-01-04' and '2022-01-06'",
                "where true");
        whereClauses = whereClauses.stream()
                .flatMap(w -> complexPredicates.stream().map(cp -> String.format("%s and %s", w, cp)))
                .collect(Collectors.toList());
        ctx.getSessionVariable().setNewPlanerAggStage(2);
        testGroupByPermutation(aggrFunctions, whereClauses, Lists.newArrayList("dt", "c1", "c2", "c3", "c4"));
    }

    @Test
    public void testGroupByPermutationWithComplexExprs() throws Exception {
        List<String> complexPredicates = Lists.newArrayList(
                "c1 like 'abc%' and c2 in ('A', 'B', 'C', 'DEF') and c3 % 2 = 1 and c4 < 100",
                "c3 % 2 = 1 and c2 in ('A', 'B', 'C', 'DEF') and c4 < 100 and c1 like 'abc%'");

        List<String> aggrFunctions =
                Lists.newArrayList(
                        "sum(if(v1>0, v1, 0))",
                        "max(if(v1>0, v1, 0))",
                        "sum(case when substr(cast(v2 as varchar), 1, 3) = '99' then 1 else 0 end)",
                        "avg(case when substr(cast(v2 as varchar), 1, 3) = '99' then 1 else 0 end)");

        List<String> whereClauses = Lists.newArrayList("where dt between '2022-01-02' and '2022-01-03'",
                "where dt between '2022-01-01' and '2022-01-31'", "where dt between '2022-01-04' and '2022-01-06'",
                "where true");
        List<String> groupByColumns = Lists.newArrayList(
                "if(c1='Y', 'Y', 'N')",
                "case when left(c2, 2) = 'AB' then 1 else  0 end",
                "c3",
                "c4%10");

        whereClauses = whereClauses.stream()
                .flatMap(w -> complexPredicates.stream().map(cp -> String.format("%s and %s", w, cp)))
                .collect(Collectors.toList());
        ctx.getSessionVariable().setNewPlanerAggStage(2);
        testGroupByPermutation(aggrFunctions, whereClauses, groupByColumns);
    }

    @Test
    public void testPhase1GroupBy() {
        List<String> selections = Lists.newArrayList("sum(v1), sum(v2), sum(v3)");
        List<String> groupBys = permuteList(Lists.newArrayList("c1", "c2", "c3", "c4"));
        List<String> whereClauses = Lists.newArrayList("where ts between '2022-01-02' and '2022-01-03'",
                "where ts between '2022-01-01' and '2022-01-31'", "where ts between '2022-01-04' and '2022-01-06'",
                "where true");
        List<String> queryList = selections.stream()
                .flatMap(s -> whereClauses.stream().flatMap(
                        w -> groupBys.stream()
                                .map(g -> String.format("select %s, %s from t1 %s group by %s", g, s, w, g))))
                .collect(Collectors.toList());
        testHelper(queryList);
    }

    @Test
    public void testGroupByDatetimeTrunc() {
        String selections = "sum(v1), sum(v2), sum(v3)";
        String groupBys = "date_trunc('minute', ts), c2, c3, c4";
        List<String> whereClauses = Lists.newArrayList("where ts between '2022-01-02' and '2022-01-03'",
                "where ts between '2022-01-01' and '2022-01-31'", "where ts between '2022-01-04' and '2022-01-06'",
                "where true");
        List<String> queryList = whereClauses.stream().map(w ->
                        String.format("select %s, %s from t1 %s group by %s", groupBys, selections, w, groupBys))
                .collect(Collectors.toList());
        testHelper(queryList);
    }

    @Test
    public void testTablePartitionByIntColumn() {
        String selections = "sum(v1)";
        String groupBys = "c1, c2";
        List<String> whereClauses = Lists.newArrayList("where c1 between 10 and 20",
                "where c1 between 20 and 21", "where c1 between 30 and 43",
                "where true");
        List<String> queryList = whereClauses.stream().map(w ->
                        String.format("select %s, %s from t2 %s group by %s", groupBys, selections, w, groupBys))
                .collect(Collectors.toList());
        testHelper(queryList);
    }

    @Test
    public void testRandomFunctions() {
        List<String> queryList = Lists.newArrayList(
                "select sum(v1) from t0 where uuid() like '%s'",
                "select right(cast(random() as varchar), 2), sum(v1) from t0 where dt between '2022-02-01' and '2022-02-04' group by right(cast(random() as varchar), 2);",
                "select sum(case when random()>0.5 then 1 else 0 end) from t0");
        Assert.assertTrue(queryList.stream().noneMatch(q -> getCachedFragment(q).isPresent()));
    }

    @Test
    public void testPartitionByMultiColumns() {
        List<String> queryList = Lists.newArrayList("select sum(v1) from t3");
        Assert.assertTrue(queryList.stream().allMatch(q -> getCachedFragment(q).isPresent()));
    }

    @Test
    public void testDifferentDataModels() {
        List<String> NonMultiVersionCacheableQueryList = Lists.newArrayList(
                "select sum(v1) from t4",
                "select sum(v1) from t5",
                "select sum(v1) from t6");
        Assert.assertTrue(NonMultiVersionCacheableQueryList.stream().map(this::getCachedFragment)
                .allMatch(f -> f.isPresent() && !f.get().getCacheParam().isCan_use_multiversion()));

        List<String> cachableQueryList = Lists.newArrayList(
                "select sum(v1), count(distinct v2) from t7",
                "select sum(v1), count(distinct v2) from t1"
        );
        Assert.assertTrue(cachableQueryList.stream().map(this::getCachedFragment)
                .allMatch(f -> f.isPresent() && f.get().getCacheParam().isCan_use_multiversion()));
    }

    @Test
    public void testPredicateDecompositionFailure() {
        List<String> unCacheableQueryList = Lists.newArrayList(
                "select sum(v1) from t1 where date_trunc('day', ts)='2022-01-02' and rand() > 0.5",
                "select sum(v1) from t1 where ts not in ('2022-01-03 00:00:00') and sleep(4)>10",
                "select sum(v1) from t1 where ts < '2022-01-03 00:00:00' or ts > '2022-01-10 00:00:00' and uuid()>10"
        );
        Assert.assertTrue(unCacheableQueryList.stream().noneMatch(q -> getCachedFragment(q).isPresent()));
    }

    @Test
    public void testBetweenPredicateDecomposition() throws AnalysisException {
        String q1 = "select sum(v1) from t1 where ts between '2022-01-02 12:55:00' and '2022-01-08 01:30:00'";
        Optional<PlanFragment> optFrag = getCachedFragment(q1);
        Assert.assertTrue(optFrag.isPresent());
        Map<Long, String> rangeMap = optFrag.get().getCacheParam().getRegion_map();
        Assert.assertTrue(!rangeMap.isEmpty());
        List<String> expectRanges = Lists.newArrayList();
        PartitionKey startKey;
        PartitionKey endKey;
        List<List<String>> rangeValues = Lists.newArrayList(
                Lists.newArrayList("2022-01-02 12:55:00", "2022-01-03 00:00:00"),
                Lists.newArrayList("2022-01-03 00:00:00", "2022-01-04 00:00:00"),
                Lists.newArrayList("2022-01-04 00:00:00", "2022-01-05 00:00:00"),
                Lists.newArrayList("2022-01-05 00:00:00", "2022-01-06 00:00:00"),
                Lists.newArrayList("2022-01-06 00:00:00", "2022-01-07 00:00:00"),
                Lists.newArrayList("2022-01-07 00:00:00", "2022-01-08 00:00:00"),
                Lists.newArrayList("2022-01-08 00:00:00", "2022-01-08 01:30:01")
        );
        for (List<String> rangeValue : rangeValues) {
            startKey = new PartitionKey();
            endKey = new PartitionKey();
            startKey.pushColumn(new DateLiteral(rangeValue.get(0), Type.DATETIME), PrimitiveType.DATETIME);
            endKey.pushColumn(new DateLiteral(rangeValue.get(1), Type.DATETIME), PrimitiveType.DATETIME);
            expectRanges.add(Range.closedOpen(startKey, endKey).toString());
        }
        Set<String> rangeSet = rangeMap.values().stream().collect(Collectors.toSet());
        for (String expectRange : expectRanges) {
            Assert.assertTrue(rangeSet.contains(expectRange));
        }
    }

    @Test
    public void testClosedOpenBinaryPredicateDecomposition() throws AnalysisException {
        String q1 = "select sum(v1) from t1 where ts >= '2022-01-02 12:55:00' and ts < '2022-01-08 01:30:00'";
        Optional<PlanFragment> optFrag = getCachedFragment(q1);
        Assert.assertTrue(optFrag.isPresent());
        Map<Long, String> rangeMap = optFrag.get().getCacheParam().getRegion_map();
        Assert.assertTrue(!rangeMap.isEmpty());
        List<String> expectRanges = Lists.newArrayList();
        PartitionKey startKey;
        PartitionKey endKey;
        List<List<String>> rangeValues = Lists.newArrayList(
                Lists.newArrayList("2022-01-02 12:55:00", "2022-01-03 00:00:00"),
                Lists.newArrayList("2022-01-03 00:00:00", "2022-01-04 00:00:00"),
                Lists.newArrayList("2022-01-04 00:00:00", "2022-01-05 00:00:00"),
                Lists.newArrayList("2022-01-05 00:00:00", "2022-01-06 00:00:00"),
                Lists.newArrayList("2022-01-06 00:00:00", "2022-01-07 00:00:00"),
                Lists.newArrayList("2022-01-07 00:00:00", "2022-01-08 00:00:00"),
                Lists.newArrayList("2022-01-08 00:00:00", "2022-01-08 01:30:00")
        );
        for (List<String> rangeValue : rangeValues) {
            startKey = new PartitionKey();
            endKey = new PartitionKey();
            startKey.pushColumn(new DateLiteral(rangeValue.get(0), Type.DATETIME), PrimitiveType.DATETIME);
            endKey.pushColumn(new DateLiteral(rangeValue.get(1), Type.DATETIME), PrimitiveType.DATETIME);
            expectRanges.add(Range.closedOpen(startKey, endKey).toString());
        }
        Set<String> rangeSet = rangeMap.values().stream().collect(Collectors.toSet());
        for (String expectRange : expectRanges) {
            Assert.assertTrue(rangeSet.contains(expectRange));
        }
    }

    @Test
    public void testClosedOpenIntegerBinaryPredicateDecomposition() throws AnalysisException {
        String q1 = "select sum(v1) from t2 where c1 >= 13 and c1 <= 69";
        Optional<PlanFragment> optFrag = getCachedFragment(q1);
        Assert.assertTrue(optFrag.isPresent());
        Map<Long, String> rangeMap = optFrag.get().getCacheParam().getRegion_map();
        Assert.assertTrue(!rangeMap.isEmpty());
        List<String> expectRanges = Lists.newArrayList();
        PartitionKey startKey;
        PartitionKey endKey;
        List<List<String>> rangeValues = Lists.newArrayList(
                Lists.newArrayList("13", "21"),
                Lists.newArrayList("21", "31"),
                Lists.newArrayList("31", "41"),
                Lists.newArrayList("41", "51"),
                Lists.newArrayList("51", "61"),
                Lists.newArrayList("61", "70")
        );
        for (List<String> rangeValue : rangeValues) {
            startKey = new PartitionKey();
            endKey = new PartitionKey();
            startKey.pushColumn(new IntLiteral(rangeValue.get(0), Type.INT), PrimitiveType.INT);
            endKey.pushColumn(new IntLiteral(rangeValue.get(1), Type.INT), PrimitiveType.INT);
            expectRanges.add(Range.closedOpen(startKey, endKey).toString());
        }
        Set<String> rangeSet = rangeMap.values().stream().collect(Collectors.toSet());
        for (String expectRange : expectRanges) {
            Assert.assertTrue(rangeSet.contains(expectRange));
        }
    }

    @Test
    public void testClosedClosedBetweenPredicateDecomposition() throws AnalysisException {
        String q1 = "select sum(v1) from t1 where ts >= '2022-01-02 12:55:00' and ts <= '2022-01-08 01:30:00'";
        Optional<PlanFragment> optFrag = getCachedFragment(q1);
        Assert.assertTrue(optFrag.isPresent());
        Map<Long, String> rangeMap = optFrag.get().getCacheParam().getRegion_map();
        Assert.assertTrue(!rangeMap.isEmpty());
        List<String> expectRanges = Lists.newArrayList();
        PartitionKey startKey;
        PartitionKey endKey;
        List<List<String>> rangeValues = Lists.newArrayList(
                Lists.newArrayList("2022-01-02 12:55:00", "2022-01-03 00:00:00"),
                Lists.newArrayList("2022-01-03 00:00:00", "2022-01-04 00:00:00"),
                Lists.newArrayList("2022-01-04 00:00:00", "2022-01-05 00:00:00"),
                Lists.newArrayList("2022-01-05 00:00:00", "2022-01-06 00:00:00"),
                Lists.newArrayList("2022-01-06 00:00:00", "2022-01-07 00:00:00"),
                Lists.newArrayList("2022-01-07 00:00:00", "2022-01-08 00:00:00"),
                Lists.newArrayList("2022-01-08 00:00:00", "2022-01-08 01:30:01")
        );
        for (List<String> rangeValue : rangeValues) {
            startKey = new PartitionKey();
            endKey = new PartitionKey();
            startKey.pushColumn(new DateLiteral(rangeValue.get(0), Type.DATETIME), PrimitiveType.DATETIME);
            endKey.pushColumn(new DateLiteral(rangeValue.get(1), Type.DATETIME), PrimitiveType.DATETIME);
            expectRanges.add(Range.closedOpen(startKey, endKey).toString());
        }
        Set<String> rangeSet = rangeMap.values().stream().collect(Collectors.toSet());
        for (String expectRange : expectRanges) {
            Assert.assertTrue(rangeSet.contains(expectRange));
        }
    }

    @Test
    public void testOpenClosedBetweenPredicateDecomposition() throws AnalysisException {
        String q1 = "select sum(v1) from t1 where ts > '2022-01-02 12:55:00' and  ts <= '2022-01-08 01:30:00'";
        Optional<PlanFragment> optFrag = getCachedFragment(q1);
        Assert.assertTrue(optFrag.isPresent());
        Map<Long, String> rangeMap = optFrag.get().getCacheParam().getRegion_map();
        Assert.assertTrue(!rangeMap.isEmpty());
        List<String> expectRanges = Lists.newArrayList();
        PartitionKey startKey;
        PartitionKey endKey;
        List<List<String>> rangeValues = Lists.newArrayList(
                Lists.newArrayList("2022-01-02 12:55:01", "2022-01-03 00:00:00"),
                Lists.newArrayList("2022-01-03 00:00:00", "2022-01-04 00:00:00"),
                Lists.newArrayList("2022-01-04 00:00:00", "2022-01-05 00:00:00"),
                Lists.newArrayList("2022-01-05 00:00:00", "2022-01-06 00:00:00"),
                Lists.newArrayList("2022-01-06 00:00:00", "2022-01-07 00:00:00"),
                Lists.newArrayList("2022-01-07 00:00:00", "2022-01-08 00:00:00"),
                Lists.newArrayList("2022-01-08 00:00:00", "2022-01-08 01:30:01")
        );
        for (List<String> rangeValue : rangeValues) {
            startKey = new PartitionKey();
            endKey = new PartitionKey();
            startKey.pushColumn(new DateLiteral(rangeValue.get(0), Type.DATETIME), PrimitiveType.DATETIME);
            endKey.pushColumn(new DateLiteral(rangeValue.get(1), Type.DATETIME), PrimitiveType.DATETIME);
            expectRanges.add(Range.closedOpen(startKey, endKey).toString());
        }
        Set<String> rangeSet = rangeMap.values().stream().collect(Collectors.toSet());
        for (String expectRange : expectRanges) {
            Assert.assertTrue(rangeSet.contains(expectRange));
        }
    }

    @Test
    public void testOpenOpenBetweenPredicateDecomposition() throws AnalysisException {
        String q1 = "select sum(v1) from t1 where ts > '2022-01-02 12:55:00' and ts < '2022-01-08 01:30:00'";
        Optional<PlanFragment> optFrag = getCachedFragment(q1);
        Assert.assertTrue(optFrag.isPresent());
        Map<Long, String> rangeMap = optFrag.get().getCacheParam().getRegion_map();
        Assert.assertTrue(!rangeMap.isEmpty());
        List<String> expectRanges = Lists.newArrayList();
        PartitionKey startKey;
        PartitionKey endKey;
        List<List<String>> rangeValues = Lists.newArrayList(
                Lists.newArrayList("2022-01-02 12:55:01", "2022-01-03 00:00:00"),
                Lists.newArrayList("2022-01-03 00:00:00", "2022-01-04 00:00:00"),
                Lists.newArrayList("2022-01-04 00:00:00", "2022-01-05 00:00:00"),
                Lists.newArrayList("2022-01-05 00:00:00", "2022-01-06 00:00:00"),
                Lists.newArrayList("2022-01-06 00:00:00", "2022-01-07 00:00:00"),
                Lists.newArrayList("2022-01-07 00:00:00", "2022-01-08 00:00:00"),
                Lists.newArrayList("2022-01-08 00:00:00", "2022-01-08 01:30:00")
        );
        for (List<String> rangeValue : rangeValues) {
            startKey = new PartitionKey();
            endKey = new PartitionKey();
            startKey.pushColumn(new DateLiteral(rangeValue.get(0), Type.DATETIME), PrimitiveType.DATETIME);
            endKey.pushColumn(new DateLiteral(rangeValue.get(1), Type.DATETIME), PrimitiveType.DATETIME);
            expectRanges.add(Range.closedOpen(startKey, endKey).toString());
        }
        Set<String> rangeSet = rangeMap.values().stream().collect(Collectors.toSet());
        for (String expectRange : expectRanges) {
            if (!rangeSet.contains(expectRange)) {
                System.out.println(expectRange);
            }
            Assert.assertTrue(rangeSet.contains(expectRange));
        }
    }

    @Test
    public void testInPredicateDecomposition()
            throws AnalysisException {
        String q1 = "select sum(v1) from t1 where ts in ('2022-01-03 00:00:00')";
        Optional<PlanFragment> optFrag = getCachedFragment(q1);
        Assert.assertTrue(optFrag.isPresent());
        Map<Long, String> rangeMap = optFrag.get().getCacheParam().getRegion_map();
        Assert.assertTrue(!rangeMap.isEmpty());
        PartitionKey startKey = new PartitionKey();
        startKey.pushColumn(new DateLiteral("2022-01-03 00:00:00", Type.DATETIME), PrimitiveType.DATETIME);
        PartitionKey endKey = new PartitionKey();
        endKey.pushColumn(new DateLiteral("2022-01-03 00:00:01", Type.DATETIME), PrimitiveType.DATETIME);
        Range<PartitionKey> expectRange = Range.closedOpen(startKey, endKey);
        rangeMap.values().stream().collect(Collectors.toSet()).contains(expectRange.toString());
    }

    @Test
    public void testUnpartitionedTable() {
        String q1 = "select  distinct(tax) from t8 where dt between '2021-01-01' and '2021-01-31' and id=23 ;";
        Optional<PlanFragment> optFrag = getCachedFragment(q1);
        Assert.assertTrue(optFrag.isPresent());
    }

    private static String toHexString(byte[] bytes) {
        StringBuffer s = new StringBuffer(bytes.length * 2);
        char[] d = "0123456789abcdef".toCharArray();
        for (byte a : bytes) {
            s.append(d[(a >>> 4) & 0xf]);
            s.append(d[a & 0xf]);
        }
        return s.toString();
    }

    @Test
    public void testDigest() {
        String queries[] = {
                "/*Q01*/ SELECT COUNT(*) FROM hits",
                "/*Q02*/ SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0",
                "/*Q03*/ SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits",
                "/*Q04*/ SELECT AVG(UserID) FROM hits",
                "/*Q05*/ SELECT COUNT(DISTINCT UserID) FROM hits",
                "/*Q06*/ SELECT COUNT(DISTINCT SearchPhrase) FROM hits",
                "/*Q07*/ SELECT MIN(EventDate), MAX(EventDate) FROM hits",
                "/*Q08*/ SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC",
                "/*Q09*/ SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
                "/*Q10*/ SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10",
                "/*Q11*/ SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10",
                "/*Q12*/ SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10",
                "/*Q13*/ SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
                "/*Q14*/ SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC",
        };

        Map<String, String> digests = new HashMap<String, String>();
        for (String q : queries) {
            Optional<PlanFragment> optFrag = getCachedFragment(q);
            Assert.assertTrue(optFrag.isPresent());
            String s = toHexString(optFrag.get().getCacheParam().getDigest());
            if (digests.containsKey(s)) {
                System.out.println(String.format("Conflicting digest:'%s',\nq1=%s\nq2=%s", s, q, digests.get(s)));
                Assert.fail();
            }
            digests.put(s, q);
        }
    }

    @Test
    public void testHotPartitions() {
        ctx.getSessionVariable().setQueryCacheHotPartitionNum(3);
        String queriesWithHotPartitions[] = {
                "/*PRIMARY_KEYS*/ SELECT COUNT(*) FROM t4 where ts between '2022-02-01 00:00:00' and '2022-03-31 00:00:00'",
                "/*UNIQUE_KEYS*/ SELECT COUNT(*) FROM t5 where ts between '2022-02-01 00:00:00' and '2022-03-31 00:00:00'",
                "/*AGGREGATE_KEYS with replace*/ SELECT COUNT(*) FROM t6 where ts between '2022-02-01 00:00:00' and '2022-03-31 00:00:00'",
        };

        String queriesWithoutHotPartitions[] = {
                "/*DUP_KEYS*/ SELECT COUNT(*) FROM t1 where ts between '2022-02-01 00:00:00' and '2022-03-31 00:00:00'",
                "/*AGGREGATE_KEYS*/ SELECT COUNT(*) FROM t7 where ts between '2022-02-01 00:00:00' and '2022-03-31 00:00:00'",
        };
        Set<String> expectRanges = Sets.newHashSet(
                "[types: [DATETIME]; keys: [2022-02-26 00:00:00]; ..types: [DATETIME]; keys: [2022-02-27 00:00:00]; )",
                "[types: [DATETIME]; keys: [2022-02-27 00:00:00]; ..types: [DATETIME]; keys: [2022-02-28 00:00:00]; )",
                "[types: [DATETIME]; keys: [2022-02-28 00:00:00]; ..types: [DATETIME]; keys: [2022-03-01 00:00:00]; )"
        );

        for (String q : queriesWithHotPartitions) {
            Optional<PlanFragment> optFrag = getCachedFragment(q);
            Assert.assertTrue(optFrag.isPresent());
            Map<Long, String> rangeMap = optFrag.get().getCacheParam().getRegion_map();
            Assert.assertFalse(rangeMap.values().stream().anyMatch(expectRanges::contains));
        }
        for (String q : queriesWithoutHotPartitions) {
            Optional<PlanFragment> optFrag = getCachedFragment(q);
            Assert.assertTrue(optFrag.isPresent());
            Map<Long, String> rangeMap = optFrag.get().getCacheParam().getRegion_map();
            List<String> matchedRanges =
                    rangeMap.values().stream().filter(expectRanges::contains).collect(Collectors.toList());
            Assert.assertEquals(matchedRanges.size(), expectRanges.size());
        }
    }

    @Test
    public void testQueryOnAggTableWithGroupByOrHavingClauseDependsOnAggColumn() {
        String[] queries = new String[]{
                "select v1, count(v2) from t7 group by v1",
                "select count(v2) from t7 where concat('abc', cast(v1 + 10 as varchar)) like '%bc10'",
                "select ts, count(v2) from t7 where v1 > 3 group by ts",
                "select ts, count(v2) from t7 " +
                        "where (case v1 % 3 when 0 then 'E' when 1 then 'A' else NULL end) is NOT NULL " +
                        "group by ts",
                "select count(v2) from t7 " +
                        "where (case v1 % 3 when 0 then 'E' when 1 then 'A' else NULL end) is NOT NULL",
                "select ts, count(v2) from t7 where v1 > 3 group by ts",
                "select /*+ SET_VAR(new_planner_agg_stage='1')*/ " +
                        "date_trunc('day', ts) as day, sum(v1) as sum_v1 " +
                        "from t7 " +
                        "where  " +
                        "   ts >= '2022-01-01 00:00:00' and ts <= '2022-01-01 23:59:59' " +
                        "   and c1 = 'abc' " +
                        "group by day having sum_v1 > 10",
        };
        for (String query : queries) {
            System.out.println(query);
            Optional<PlanFragment> optFrag = getCachedFragment(query);
            Assert.assertTrue(optFrag.isPresent());
            Assert.assertFalse(optFrag.get().getCacheParam().isCan_use_multiversion());
        }

        String[] negativeQueries = new String[]{
                "select ts, count(v2) from t7 group by ts",
                "select count(v2) from t7 where date_trunc('day', ts) = '2022-01-13'",
                "select ts, count(v2) from t7 where c1 like 'abc%' group by ts",
                "select ts, count(v2) from t7 " +
                        "where (case length(c1) % 3 when 0 then 'E' when 1 then 'A' else NULL end) is NOT NULL " +
                        "group by ts",
                "select count(v2) from t7 " +
                        "where (case length(c1) % 3 when 0 then 'E' when 1 then 'A' else NULL end) is NOT NULL",
                "select ts, count(v2) from t7 where length(c1) > 3 group by ts",
                "select /*+ SET_VAR(new_planner_agg_stage='2')*/ " +
                        "date_trunc('day', ts) as day, sum(v1) as sum_v1 " +
                        "from t7 " +
                        "where  " +
                        "   ts >= '2022-01-01 00:00:00' and ts <= '2022-01-01 23:59:59' " +
                        "   and c1 = 'abc' " +
                        "group by day having sum_v1 > 10",
                "select /*+ SET_VAR(new_planner_agg_stage='1')*/ " +
                        "sqrt(cast(sum(v1) as double)) > 3.1415926 " +
                        "from t7 " +
                        "where  " +
                        "   ts >= '2022-01-01 00:00:00' and ts <= '2022-01-01 23:59:59' " +
                        "   and c1 = 'abc' ",
        };
        for (String query : negativeQueries) {
            System.out.println(query);
            Optional<PlanFragment> optFrag = getCachedFragment(query);
            Assert.assertTrue(optFrag.isPresent());
            Assert.assertTrue(optFrag.get().getCacheParam().isCan_use_multiversion());
        }
    }

    @Test
    public void testGroupByDifferentColumnsOnUnpartitionedTable() {
        String sql0 = "SELECT REGION_CODE, count(*) from t9 group by 1;";
        String sql1 = "SELECT REGION_NAME, count(*) from t9 group by 1;";
        Optional<PlanFragment> frag0 = getCachedFragment(sql0);
        Optional<PlanFragment> frag1 = getCachedFragment(sql1);
        Assert.assertTrue(frag0.isPresent() && frag1.isPresent());
        Assert.assertNotEquals(frag0.get().getCacheParam().digest, frag1.get().getCacheParam().digest);
    }

    @Test
    public void testDisableCacheWhenFragmentWithLocalShuffle() throws Exception {
        String sql = "/*Q01*/ SELECT COUNT(*) FROM hits";
        Pair<String, ExecPlan> planAndFragment = UtFrameUtils.getPlanAndFragment(ctx, sql);
        ExecPlan execPlan = planAndFragment.second;
        Assert.assertTrue(execPlan.getFragments().stream().anyMatch(frag -> frag.getCacheParam() != null));
        List<PlanFragment> fragments = execPlan.getFragments().stream().map(frag -> {
            frag.setCacheParam(null);
            frag.setWithLocalShuffleIfTrue(true);
            FragmentNormalizer normalizer = new FragmentNormalizer(execPlan, frag);
            normalizer.normalize();
            return frag;
        }).collect(Collectors.toList());
        Assert.assertTrue(fragments.stream().noneMatch(frag -> frag.getCacheParam() != null));
    }
}
