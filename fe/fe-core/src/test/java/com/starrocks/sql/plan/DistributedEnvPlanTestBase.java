package com.starrocks.sql.plan;

import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;

public class DistributedEnvPlanTestBase extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        connectContext.getSessionVariable().disableNewPlanner();
        starRocksAssert.withTable("CREATE TABLE `lineorder_new_l` (\n" +
                "  `LO_ORDERKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `LO_ORDERDATE` date NOT NULL COMMENT \"\",\n" +
                "  `LO_LINENUMBER` tinyint(4) NOT NULL COMMENT \"\",\n" +
                "  `LO_CUSTKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `LO_PARTKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `LO_SUPPKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `LO_ORDERPRIORITY` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `LO_SHIPPRIORITY` tinyint(4) NOT NULL COMMENT \"\",\n" +
                "  `LO_QUANTITY` tinyint(4) NOT NULL COMMENT \"\",\n" +
                "  `LO_EXTENDEDPRICE` int(11) NOT NULL COMMENT \"\",\n" +
                "  `LO_ORDTOTALPRICE` int(11) NOT NULL COMMENT \"\",\n" +
                "  `LO_DISCOUNT` tinyint(4) NOT NULL COMMENT \"\",\n" +
                "  `LO_REVENUE` int(11) NOT NULL COMMENT \"\",\n" +
                "  `LO_SUPPLYCOST` int(11) NOT NULL COMMENT \"\",\n" +
                "  `LO_TAX` tinyint(4) NOT NULL COMMENT \"\",\n" +
                "  `LO_COMMITDATE` date NOT NULL COMMENT \"\",\n" +
                "  `LO_SHIPMODE` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `C_NAME` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `C_ADDRESS` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `C_CITY` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `C_NATION` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `C_REGION` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `C_PHONE` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `C_MKTSEGMENT` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `S_NAME` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `S_ADDRESS` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `S_CITY` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `S_NATION` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `S_REGION` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `S_PHONE` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `P_NAME` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `P_MFGR` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `P_CATEGORY` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `P_BRAND` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `P_COLOR` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `P_TYPE` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `P_SIZE` tinyint(4) NOT NULL COMMENT \"\",\n" +
                "  `P_CONTAINER` varchar(100) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`LO_ORDERKEY`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`LO_ORDERKEY`) BUCKETS 192\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `dates_n` (\n" +
                "  `d_datekey` int(11) NULL COMMENT \"\",\n" +
                "  `d_date` varchar(20) NULL COMMENT \"\",\n" +
                "  `d_dayofweek` varchar(10) NULL COMMENT \"\",\n" +
                "  `d_month` varchar(11) NULL COMMENT \"\",\n" +
                "  `d_year` int(11) NULL COMMENT \"\",\n" +
                "  `d_yearmonthnum` int(11) NULL COMMENT \"\",\n" +
                "  `d_yearmonth` varchar(9) NULL COMMENT \"\",\n" +
                "  `d_daynuminweek` int(11) NULL COMMENT \"\",\n" +
                "  `d_daynuminmonth` int(11) NULL COMMENT \"\",\n" +
                "  `d_daynuminyear` int(11) NULL COMMENT \"\",\n" +
                "  `d_monthnuminyear` int(11) NULL COMMENT \"\",\n" +
                "  `d_weeknuminyear` int(11) NULL COMMENT \"\",\n" +
                "  `d_sellingseason` varchar(14) NULL COMMENT \"\",\n" +
                "  `d_lastdayinweekfl` int(11) NULL COMMENT \"\",\n" +
                "  `d_lastdayinmonthfl` int(11) NULL COMMENT \"\",\n" +
                "  `d_holidayfl` int(11) NULL COMMENT \"\",\n" +
                "  `d_weekdayfl` int(11) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`d_datekey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`d_datekey`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        Catalog catalog = connectContext.getCatalog();
        connectContext.getSessionVariable().setTpchScale(100);
        long scale = connectContext.getSessionVariable().getTpchScale();
        OlapTable t0 = (OlapTable) catalog.getDb("default_cluster:test").getTable("region");
        setTableStatistics(t0, 5);

        OlapTable t5 = (OlapTable) catalog.getDb("default_cluster:test").getTable("nation");
        setTableStatistics(t5, 25);

        OlapTable t1 = (OlapTable) catalog.getDb("default_cluster:test").getTable("supplier");
        setTableStatistics(t1, 10000 * scale);

        OlapTable t4 = (OlapTable) catalog.getDb("default_cluster:test").getTable("customer");
        setTableStatistics(t4, 150000 * scale);

        OlapTable t6 = (OlapTable) catalog.getDb("default_cluster:test").getTable("part");
        setTableStatistics(t6, 200000 * scale);

        OlapTable t2 = (OlapTable) catalog.getDb("default_cluster:test").getTable("partsupp");
        setTableStatistics(t2, 800000 * scale);

        OlapTable t3 = (OlapTable) catalog.getDb("default_cluster:test").getTable("orders");
        setTableStatistics(t3, 1500000 * scale);

        OlapTable t7 = (OlapTable) catalog.getDb("default_cluster:test").getTable("lineitem");
        setTableStatistics(t7, 6000000 * scale);

        OlapTable test_all_type = (OlapTable) catalog.getDb("default_cluster:test").getTable("test_all_type");
        setTableStatistics(test_all_type, 6000000);

        OlapTable lineorder_new_l = (OlapTable) catalog.getDb("default_cluster:test").getTable("lineorder_new_l");
        setTableStatistics(lineorder_new_l, 1200018434);

        OlapTable dates_n = (OlapTable) catalog.getDb("default_cluster:test").getTable("dates_n");
        setTableStatistics(dates_n, 2556);

        String DB_NAME = "MNCT";
        starRocksAssert.withDatabase(DB_NAME);
        starRocksAssert.withTable("CREATE TABLE `MNCT`.`SQA_POC_K` (\n" +
                "    `CALC_DATE` date NULL,\n" +
                "    `STORE_CODE`varchar(50) NULL,\n" +
                "    `BRAND_CODE`decimal(4, 0) NULL,\n" +
                "    `START_BUSINESS_DATE` date NULL,\n" +
                "    `END_BUSINESS_DATE` date NULL,\n" +
                "    `REGIONALISM_CODE`varchar(50) NULL,\n" +
                "    `REGIONALISM_PRO_NAME`varchar(50) NULL,\n" +
                "    `REGIONALISM_CITY_NAME`varchar(50) NULL,\n" +
                "    `BOH_OPS_MARKET_CODE`varchar(50) NULL,\n" +
                "    `BOH_OPS_MARKET_NAME`varchar(50) NULL,\n" +
                "    `STORE_TYPE_CODE`varchar(50) NULL,\n" +
                "    `STORE_TYPE_NAME`varchar(50) NULL,\n" +
                "    `CALC_DATE_2`varchar(10) NULL,\n" +
                "    `STORE_CODE2`varchar(50) NULL,\n" +
                "    `PRICE_TYPE_CODE`varchar(100) NULL,\n" +
                "    `PRICE_TYPE_NAME`varchar(100) NULL,\n" +
                "    `EFFECT_DAY_WITH_START` date NULL,\n" +
                "    `STORE_CODE3`varchar(50) NULL,\n" +
                "    `a831F5D28_8205_403D_B9C1_4FC913AE3533`  tinyint NULL,\n" +
                "    `aF8EF0D68_B9D7_4BA1_98D3_F70F464A06FE`  tinyint NULL,\n" +
                "    `a913D7E69_B7B4_4F4B_AFB8_65C9ADDEF9C1`  tinyint NULL,\n" +
                "    `aD53DABBC_635C_4E5F_A961_7544B753F564`  tinyint NULL,\n" +
                "    `aD6C3E356_08E6_4F7F_853E_74C72F088639`  tinyint NULL,\n" +
                "    `aF94756DB_3481_4F07_9EE3_319D2E9AE389`  tinyint NULL,\n" +
                "    `CALC_DATE_6`varchar(10) NULL,\n" +
                "    `STORE_CODE6`varchar(50) NULL,\n" +
                "    `a103`  tinyint NULL,\n" +
                "    `a104`  tinyint NULL,\n" +
                "    `a105`  tinyint NULL,\n" +
                "    `a106`  tinyint NULL,\n" +
                "    `a107`  tinyint NULL,\n" +
                "    `a108`  tinyint NULL,\n" +
                "    `a109`  tinyint NULL,\n" +
                "    `a110`  tinyint NULL,\n" +
                "    `a111`  tinyint NULL,\n" +
                "    `a112`  tinyint NULL,\n" +
                "    `a31DC2494_0CD2_40B1_A93C_BB0846939CAE`  tinyint NULL,\n" +
                "    `aA7E37603_DA70_4C6A_B45F_9110CBFB8491`  tinyint NULL,\n" +
                "    `CALC_DATE_7`varchar(10) NULL,\n" +
                "    `STORE_CODE7`varchar(50) NULL,\n" +
                "    `STORE_LOCATION_CODE`varchar(50) NULL,\n" +
                "    `CALC_DATE_9`varchar(10) NULL,\n" +
                "    `STORE_CODE9`varchar(50) NULL,\n" +
                "    `a0099fd65_bc5d_4b9b_aeb4_7323935031af`  tinyint NULL,\n" +
                "    `a00f73c93_c80b_482d_936b_aece0e76a4af`  tinyint NULL,\n" +
                "    `a0480607e_1f52_400f_9b55_d18305a5a3d8`  tinyint NULL,\n" +
                "    `a054f3532_6223_4cae_9b31_413b94078b63`  tinyint NULL,\n" +
                "    `a0565910D_A270_4409_A04A_1D79942AC76A`  tinyint NULL,\n" +
                "    `a0cefc660_f2f1_4ec9_bfef_e51be874082c`  tinyint NULL,\n" +
                "    `a0f65ef13_978a_4907_92f4_e637363c5d02`  tinyint NULL,\n" +
                "    `a16274db5_18a9_4f52_9900_bea49167b18f`  tinyint NULL,\n" +
                "    `a167ef6ba_bacc_4106_a712_dbb019d22317`  tinyint NULL,\n" +
                "    `a19FCC357_9F8E_47E5_9D6E_08FC4D91F753`  tinyint NULL,\n" +
                "    `a1DB82793_8E2B_4525_8273_B6380B240450`  tinyint NULL,\n" +
                "    `a1FB29CB2_4B79_490B_B8F3_6FAD9B8EC02E`  tinyint NULL,\n" +
                "    `a247f5072_6b3e_4551_b667_ed121f7e8548`  tinyint NULL,\n" +
                "    `a2cacd1a8_59a3_4b78_9366_ff0012b35c4c`  tinyint NULL,\n" +
                "    `a2f053a69_689d_4c6a_b81c_64345cc97a30`  tinyint NULL,\n" +
                "    `a2FD017C0_F957_4E02_8AF5_4A8A142E25BF`  tinyint NULL,\n" +
                "    `a3dcec36f_7d66_4183_a99d_7345491d3691`  tinyint NULL,\n" +
                "    `a420c08ed_4619_4b43_a908_598dc7e83ec8`  tinyint NULL,\n" +
                "    `a4b4387be_47e9_43e7_becb_d294011d5083`  tinyint NULL,\n" +
                "    `a4B922B92_2370_45A3_AA5C_CFEF8C084A52`  tinyint NULL,\n" +
                "    `a4fc6dffb_b4fe_42ef_b25e_02783af84136`  tinyint NULL,\n" +
                "    `a530d68fe_58c9_4d3d_af06_40a555558e0e`  tinyint NULL,\n" +
                "    `a61e77b2c_8a34_4b54_9672_92415ecaeb54`  tinyint NULL,\n" +
                "    `a638705f5_f77f_4b4c_a153_7af65ba78cae`  tinyint NULL,\n" +
                "    `a64a247fb_f961_42d2_bc52_900eeb4f0261`  tinyint NULL,\n" +
                "    `a67893f26_82f5_4e5c_8693_88c467e8736c`  tinyint NULL,\n" +
                "    `a683272e8_94b1_4dcb_9dfd_eec0057341b4`  tinyint NULL,\n" +
                "    `a71c413cb_78b2_4c8b_9e5a_65559b0d7c36`  tinyint NULL,\n" +
                "    `a8051B908_51EC_4F30_A89B_2A44E59E7E1E`  tinyint NULL,\n" +
                "    `a85758fb9_b85f_4fe6_825c_7733f144047b`  tinyint NULL,\n" +
                "    `a85970d1c_8a65_41d3_b489_bd4ae4934f3b`  tinyint NULL,\n" +
                "    `a89EEC418_B612_4FCA_8C26_3722469D3D58`  tinyint NULL,\n" +
                "    `a9c4be392_6e0e_471f_bb67_690de85ea664`  tinyint NULL,\n" +
                "    `a9ea019ad_ef5e_4453_8e28_003ffbd64be5`  tinyint NULL,\n" +
                "    `aa8631188_a0b2_49ee_8625_acd548ac0b16`  tinyint NULL,\n" +
                "    `ab6808d0c_e1ef_47bc_9727_12e4ed7bafec`  tinyint NULL,\n" +
                "    `ab9912ae3_918f_4f3a_a92c_2afc8a699efa`  tinyint NULL,\n" +
                "    `aba30c165_3140_4436_8cbe_1d1c8e076840`  tinyint NULL,\n" +
                "    `abaa217e5_15ff_40d3_aabd_d0441143c4e5`  tinyint NULL,\n" +
                "    `ac7ee5c16_88a4_433d_be76_f9a1548be054`  tinyint NULL,\n" +
                "    `adcfd9dfc_4e0d_489b_bd8a_70b918c98dd1`  tinyint NULL,\n" +
                "    `ae34baa00_b33c_43be_a304_6b1e362932db`  tinyint NULL,\n" +
                "    `ae7eec89f_fe24_4018_bd19_77f5d0301823`  tinyint NULL,\n" +
                "    `aeac339f5_a347_47b1_8f7d_ea408e9bbec7`  tinyint NULL,\n" +
                "    `aeb010384_3349_4695_b0fa_4940548fe902`  tinyint NULL,\n" +
                "    `aeb1645f9_d0db_40ca_b01d_48e5a82e2617`  tinyint NULL,\n" +
                "    `aedddca9b_7839_4d49_a2f0_9456dc5b731d`  tinyint NULL,\n" +
                "    `aee0ce3ac_cfa5_4494_8b59_3b9dbe5fd101`  tinyint NULL,\n" +
                "    `af1a57fdf_4107_4e60_9ff2_7af64d82ed4a`  tinyint NULL,\n" +
                "    `af47f92de_1d51_4f1a_aba2_e56459c5aa4a`  tinyint NULL,\n" +
                "    `afe50f382_b15e_4519_a020_ba4dd984a6ac`  tinyint NULL,\n" +
                "    `CALC_DATE_8`varchar(10) NULL,\n" +
                "    `STORE_CODE8`varchar(50) NULL,\n" +
                "    `a03CA9197_5F43_4F82_A052_AF04F2EE300E`  tinyint NULL,\n" +
                "    `a0C47B129_A1D9_49C4_AFCA_80E1E0E3C611`  tinyint NULL,\n" +
                "    `a11E9BAC1_427B_4122_9FEB_08419B894F01`  tinyint NULL,\n" +
                "    `a154B8A95_61FB_449B_BD94_A774AECEC411`  tinyint NULL,\n" +
                "    `a1C141804_76B0_4A5C_B193_08878866E399`  tinyint NULL,\n" +
                "    `a1FE741CE_0BAC_4A82_A5C7_99D57EAA985B`  tinyint NULL,\n" +
                "    `a234562AC_C407_4465_B49C_2D5D22C292E7`  tinyint NULL,\n" +
                "    `a23937D40_A644_41FA_9649_18D4006DFF74`  tinyint NULL,\n" +
                "    `a289D930C_C423_45D6_8AB1_DF9949BA2D4E`  tinyint NULL,\n" +
                "    `a30C93CCD_7F86_4D35_9B44_D7990AB7568F`  tinyint NULL,\n" +
                "    `a37874A7F_AF0E_4CB2_95AB_413BE14EB1EA`  tinyint NULL,\n" +
                "    `a40D4B2DC_81C5_4B8A_A9EC_30F594727818`  tinyint NULL,\n" +
                "    `a4167C663_0568_4728_93D1_B503039916B3`  tinyint NULL,\n" +
                "    `a472FC4C2_7297_4C4D_B6C6_D92C177647EF`  tinyint NULL,\n" +
                "    `a48772C9F_2883_4262_BDF1_960D93646D94`  tinyint NULL,\n" +
                "    `a52EF8E44_3A3F_4FF6_9A13_936AE00F04A5`  tinyint NULL,\n" +
                "    `a5730D2CA_B590_4ABA_ACC8_A581B528CAB1`  tinyint NULL,\n" +
                "    `a5B23235E_3220_4A21_BA37_98D33BC7893B`  tinyint NULL,\n" +
                "    `a5F935915_5CDA_4351_AA83_2196ED5D464B`  tinyint NULL,\n" +
                "    `a60C73D49_05D6_4A76_9224_F440EEB89938`  tinyint NULL,\n" +
                "    `a6A21E2DF_44C1_45FB_B509_DD19438BADCE`  tinyint NULL,\n" +
                "    `a7294F5B8_2D90_4EDE_B403_D261BA062305`  tinyint NULL,\n" +
                "    `a738458A9_EAE6_41E7_98B3_BDDDA966DFAF`  tinyint NULL,\n" +
                "    `a7A1DE04F_E232_42BF_A613_28A5BBEA181E`  tinyint NULL,\n" +
                "    `a7E637BDB_0EC1_4064_911E_8FA7B0C8E036`  tinyint NULL,\n" +
                "    `a7E70AEEE_D66D_4312_ADE2_047635D47C8C`  tinyint NULL,\n" +
                "    `a81F66F75_C78D_4367_B493_3D89C78FBEBF`  tinyint NULL,\n" +
                "    `a8452B3DE_E608_4EBA_B025_D38138B90BD1`  tinyint NULL,\n" +
                "    `a9222A48D_D489_41BB_B90D_23E5AA015364`  tinyint NULL,\n" +
                "    `a999A5750_A6CC_4DAC_8A66_D853B07F9501`  tinyint NULL,\n" +
                "    `aA2546DBC_0B02_4C47_938E_524BE8EE7AE0`  tinyint NULL,\n" +
                "    `aA8A4F860_9240_4352_A7C2_0D2CEA2B40E6`  tinyint NULL,\n" +
                "    `aAF3A914E_15C0_4186_A8DB_F7489E8CD556`  tinyint NULL,\n" +
                "    `aB0BE4177_69EA_457B_81C9_54028A032050`  tinyint NULL,\n" +
                "    `aC3B4A097_E056_4A72_9AFC_96AC924D1AE7`  tinyint NULL,\n" +
                "    `aCD7B67BF_B479_454C_9592_43E24B1363F2`  tinyint NULL,\n" +
                "    `aD17A63BC_4FC1_4E40_81BF_F3C5B4623B74`  tinyint NULL,\n" +
                "    `aD5DB3666_6367_46BA_BDEC_AC82FD38C4D3`  tinyint NULL,\n" +
                "    `aDA4FD67F_CBD7_4B7E_9BB7_39DC7EDE4B6F`  tinyint NULL,\n" +
                "    `aE19B019D_5DE0_421C_9113_5B3EE28CAE6D`  tinyint NULL,\n" +
                "    `aE4A168DD_4BBD_4909_86F1_70167AA7D538`  tinyint NULL,\n" +
                "    `aE5015F48_1D96_461B_B92E_2711587B902B`  tinyint NULL,\n" +
                "    `aE92D8A31_DB3D_475D_877D_2C66EBF5B910`  tinyint NULL,\n" +
                "    `aEB6EC514_0B8D_4136_A01D_2C85793768AA`  tinyint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`CALC_DATE`,`STORE_CODE`, `BRAND_CODE`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`STORE_CODE`) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `MNCT`.`SQA_POC_K1` (\n" +
                "  `CALC_DATE` date NULL COMMENT \"\",\n" +
                "  `KEY_ID` char(36) NULL COMMENT \"\",\n" +
                "  `SCOPE_ID` char(36) NULL COMMENT \"\",\n" +
                "  `BRAND_CODE` tinyint NULL COMMENT \"\",\n" +
                "  `SUB_SCOPE_CODE` varchar(50) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`CALC_DATE`,`KEY_ID`, `SCOPE_ID`, `BRAND_CODE`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`KEY_ID`) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `MNCT`.`SQA_POC_K2` (\n" +
                "  `CALC_DATE` date NULL COMMENT \"\",\n" +
                "  `KEY_ID` char(36) NULL COMMENT \"\",\n" +
                "  `SCOPE_ID` char(36) NULL COMMENT \"\",\n" +
                "  `BRAND_CODE` tinyint NULL COMMENT \"\",\n" +
                "  `SUB_SCOPE_CODE` varchar(50) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`CALC_DATE`,`KEY_ID`, `SCOPE_ID`, `BRAND_CODE`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`KEY_ID`) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `MNCT`.`SQA_POC_K3` (\n" +
                "    `CALC_DATE` date NULL COMMENT \"\",\n" +
                "    `KEY_ID` char(36) NULL COMMENT \"\",\n" +
                "    `SCOPE_ID` char(36) NULL COMMENT \"\",\n" +
                "    `BRAND_CODE` tinyint NULL COMMENT \"\",\n" +
                "    `a831F5D28_8205_403D_B9C1_4FC913AE3533`  tinyint NULL,\n" +
                "    `aF8EF0D68_B9D7_4BA1_98D3_F70F464A06FE`  tinyint NULL,\n" +
                "    `a913D7E69_B7B4_4F4B_AFB8_65C9ADDEF9C1`  tinyint NULL,\n" +
                "    `aD53DABBC_635C_4E5F_A961_7544B753F564`  tinyint NULL,\n" +
                "    `aD6C3E356_08E6_4F7F_853E_74C72F088639`  tinyint NULL,\n" +
                "    `aF94756DB_3481_4F07_9EE3_319D2E9AE389`  tinyint NULL,\n" +
                "    `a103`  tinyint NULL,\n" +
                "    `a104`  tinyint NULL,\n" +
                "    `a105`  tinyint NULL,\n" +
                "    `a106`  tinyint NULL,\n" +
                "    `a107`  tinyint NULL,\n" +
                "    `a108`  tinyint NULL,\n" +
                "    `a109`  tinyint NULL,\n" +
                "    `a110`  tinyint NULL,\n" +
                "    `a111`  tinyint NULL,\n" +
                "    `a112`  tinyint NULL,\n" +
                "    `a31DC2494_0CD2_40B1_A93C_BB0846939CAE`  tinyint NULL,\n" +
                "    `aA7E37603_DA70_4C6A_B45F_9110CBFB8491`  tinyint NULL,\n" +
                "    `a0099fd65_bc5d_4b9b_aeb4_7323935031af`  tinyint NULL,\n" +
                "    `a00f73c93_c80b_482d_936b_aece0e76a4af`  tinyint NULL,\n" +
                "    `a0480607e_1f52_400f_9b55_d18305a5a3d8`  tinyint NULL,\n" +
                "    `a054f3532_6223_4cae_9b31_413b94078b63`  tinyint NULL,\n" +
                "    `a0565910D_A270_4409_A04A_1D79942AC76A`  tinyint NULL,\n" +
                "    `a0cefc660_f2f1_4ec9_bfef_e51be874082c`  tinyint NULL,\n" +
                "    `a0f65ef13_978a_4907_92f4_e637363c5d02`  tinyint NULL,\n" +
                "    `a16274db5_18a9_4f52_9900_bea49167b18f`  tinyint NULL,\n" +
                "    `a167ef6ba_bacc_4106_a712_dbb019d22317`  tinyint NULL,\n" +
                "    `a19FCC357_9F8E_47E5_9D6E_08FC4D91F753`  tinyint NULL,\n" +
                "    `a1DB82793_8E2B_4525_8273_B6380B240450`  tinyint NULL,\n" +
                "    `a1FB29CB2_4B79_490B_B8F3_6FAD9B8EC02E`  tinyint NULL,\n" +
                "    `a247f5072_6b3e_4551_b667_ed121f7e8548`  tinyint NULL,\n" +
                "    `a2cacd1a8_59a3_4b78_9366_ff0012b35c4c`  tinyint NULL,\n" +
                "    `a2f053a69_689d_4c6a_b81c_64345cc97a30`  tinyint NULL,\n" +
                "    `a2FD017C0_F957_4E02_8AF5_4A8A142E25BF`  tinyint NULL,\n" +
                "    `a3dcec36f_7d66_4183_a99d_7345491d3691`  tinyint NULL,\n" +
                "    `a420c08ed_4619_4b43_a908_598dc7e83ec8`  tinyint NULL,\n" +
                "    `a4b4387be_47e9_43e7_becb_d294011d5083`  tinyint NULL,\n" +
                "    `a4B922B92_2370_45A3_AA5C_CFEF8C084A52`  tinyint NULL,\n" +
                "    `a4fc6dffb_b4fe_42ef_b25e_02783af84136`  tinyint NULL,\n" +
                "    `a530d68fe_58c9_4d3d_af06_40a555558e0e`  tinyint NULL,\n" +
                "    `a61e77b2c_8a34_4b54_9672_92415ecaeb54`  tinyint NULL,\n" +
                "    `a638705f5_f77f_4b4c_a153_7af65ba78cae`  tinyint NULL,\n" +
                "    `a64a247fb_f961_42d2_bc52_900eeb4f0261`  tinyint NULL,\n" +
                "    `a67893f26_82f5_4e5c_8693_88c467e8736c`  tinyint NULL,\n" +
                "    `a683272e8_94b1_4dcb_9dfd_eec0057341b4`  tinyint NULL,\n" +
                "    `a71c413cb_78b2_4c8b_9e5a_65559b0d7c36`  tinyint NULL,\n" +
                "    `a8051B908_51EC_4F30_A89B_2A44E59E7E1E`  tinyint NULL,\n" +
                "    `a85758fb9_b85f_4fe6_825c_7733f144047b`  tinyint NULL,\n" +
                "    `a85970d1c_8a65_41d3_b489_bd4ae4934f3b`  tinyint NULL,\n" +
                "    `a89EEC418_B612_4FCA_8C26_3722469D3D58`  tinyint NULL,\n" +
                "    `a9c4be392_6e0e_471f_bb67_690de85ea664`  tinyint NULL,\n" +
                "    `a9ea019ad_ef5e_4453_8e28_003ffbd64be5`  tinyint NULL,\n" +
                "    `aa8631188_a0b2_49ee_8625_acd548ac0b16`  tinyint NULL,\n" +
                "    `ab6808d0c_e1ef_47bc_9727_12e4ed7bafec`  tinyint NULL,\n" +
                "    `ab9912ae3_918f_4f3a_a92c_2afc8a699efa`  tinyint NULL,\n" +
                "    `aba30c165_3140_4436_8cbe_1d1c8e076840`  tinyint NULL,\n" +
                "    `abaa217e5_15ff_40d3_aabd_d0441143c4e5`  tinyint NULL,\n" +
                "    `ac7ee5c16_88a4_433d_be76_f9a1548be054`  tinyint NULL,\n" +
                "    `adcfd9dfc_4e0d_489b_bd8a_70b918c98dd1`  tinyint NULL,\n" +
                "    `ae34baa00_b33c_43be_a304_6b1e362932db`  tinyint NULL,\n" +
                "    `ae7eec89f_fe24_4018_bd19_77f5d0301823`  tinyint NULL,\n" +
                "    `aeac339f5_a347_47b1_8f7d_ea408e9bbec7`  tinyint NULL,\n" +
                "    `aeb010384_3349_4695_b0fa_4940548fe902`  tinyint NULL,\n" +
                "    `aeb1645f9_d0db_40ca_b01d_48e5a82e2617`  tinyint NULL,\n" +
                "    `aedddca9b_7839_4d49_a2f0_9456dc5b731d`  tinyint NULL,\n" +
                "    `aee0ce3ac_cfa5_4494_8b59_3b9dbe5fd101`  tinyint NULL,\n" +
                "    `af1a57fdf_4107_4e60_9ff2_7af64d82ed4a`  tinyint NULL,\n" +
                "    `af47f92de_1d51_4f1a_aba2_e56459c5aa4a`  tinyint NULL,\n" +
                "    `afe50f382_b15e_4519_a020_ba4dd984a6ac`  tinyint NULL,\n" +
                "    `a03CA9197_5F43_4F82_A052_AF04F2EE300E`  tinyint NULL,\n" +
                "    `a0C47B129_A1D9_49C4_AFCA_80E1E0E3C611`  tinyint NULL,\n" +
                "    `a11E9BAC1_427B_4122_9FEB_08419B894F01`  tinyint NULL,\n" +
                "    `a154B8A95_61FB_449B_BD94_A774AECEC411`  tinyint NULL,\n" +
                "    `a1C141804_76B0_4A5C_B193_08878866E399`  tinyint NULL,\n" +
                "    `a1FE741CE_0BAC_4A82_A5C7_99D57EAA985B`  tinyint NULL,\n" +
                "    `a234562AC_C407_4465_B49C_2D5D22C292E7`  tinyint NULL,\n" +
                "    `a23937D40_A644_41FA_9649_18D4006DFF74`  tinyint NULL,\n" +
                "    `a289D930C_C423_45D6_8AB1_DF9949BA2D4E`  tinyint NULL,\n" +
                "    `a30C93CCD_7F86_4D35_9B44_D7990AB7568F`  tinyint NULL,\n" +
                "    `a37874A7F_AF0E_4CB2_95AB_413BE14EB1EA`  tinyint NULL,\n" +
                "    `a40D4B2DC_81C5_4B8A_A9EC_30F594727818`  tinyint NULL,\n" +
                "    `a4167C663_0568_4728_93D1_B503039916B3`  tinyint NULL,\n" +
                "    `a472FC4C2_7297_4C4D_B6C6_D92C177647EF`  tinyint NULL,\n" +
                "    `a48772C9F_2883_4262_BDF1_960D93646D94`  tinyint NULL,\n" +
                "    `a52EF8E44_3A3F_4FF6_9A13_936AE00F04A5`  tinyint NULL,\n" +
                "    `a5730D2CA_B590_4ABA_ACC8_A581B528CAB1`  tinyint NULL,\n" +
                "    `a5B23235E_3220_4A21_BA37_98D33BC7893B`  tinyint NULL,\n" +
                "    `a5F935915_5CDA_4351_AA83_2196ED5D464B`  tinyint NULL,\n" +
                "    `a60C73D49_05D6_4A76_9224_F440EEB89938`  tinyint NULL,\n" +
                "    `a6A21E2DF_44C1_45FB_B509_DD19438BADCE`  tinyint NULL,\n" +
                "    `a7294F5B8_2D90_4EDE_B403_D261BA062305`  tinyint NULL,\n" +
                "    `a738458A9_EAE6_41E7_98B3_BDDDA966DFAF`  tinyint NULL,\n" +
                "    `a7A1DE04F_E232_42BF_A613_28A5BBEA181E`  tinyint NULL,\n" +
                "    `a7E637BDB_0EC1_4064_911E_8FA7B0C8E036`  tinyint NULL,\n" +
                "    `a7E70AEEE_D66D_4312_ADE2_047635D47C8C`  tinyint NULL,\n" +
                "    `a81F66F75_C78D_4367_B493_3D89C78FBEBF`  tinyint NULL,\n" +
                "    `a8452B3DE_E608_4EBA_B025_D38138B90BD1`  tinyint NULL,\n" +
                "    `a9222A48D_D489_41BB_B90D_23E5AA015364`  tinyint NULL,\n" +
                "    `a999A5750_A6CC_4DAC_8A66_D853B07F9501`  tinyint NULL,\n" +
                "    `aA2546DBC_0B02_4C47_938E_524BE8EE7AE0`  tinyint NULL,\n" +
                "    `aA8A4F860_9240_4352_A7C2_0D2CEA2B40E6`  tinyint NULL,\n" +
                "    `aAF3A914E_15C0_4186_A8DB_F7489E8CD556`  tinyint NULL,\n" +
                "    `aB0BE4177_69EA_457B_81C9_54028A032050`  tinyint NULL,\n" +
                "    `aC3B4A097_E056_4A72_9AFC_96AC924D1AE7`  tinyint NULL,\n" +
                "    `aCD7B67BF_B479_454C_9592_43E24B1363F2`  tinyint NULL,\n" +
                "    `aD17A63BC_4FC1_4E40_81BF_F3C5B4623B74`  tinyint NULL,\n" +
                "    `aD5DB3666_6367_46BA_BDEC_AC82FD38C4D3`  tinyint NULL,\n" +
                "    `aDA4FD67F_CBD7_4B7E_9BB7_39DC7EDE4B6F`  tinyint NULL,\n" +
                "    `aE19B019D_5DE0_421C_9113_5B3EE28CAE6D`  tinyint NULL,\n" +
                "    `aE4A168DD_4BBD_4909_86F1_70167AA7D538`  tinyint NULL,\n" +
                "    `aE5015F48_1D96_461B_B92E_2711587B902B`  tinyint NULL,\n" +
                "    `aE92D8A31_DB3D_475D_877D_2C66EBF5B910`  tinyint NULL,\n" +
                "    `aEB6EC514_0B8D_4136_A01D_2C85793768AA`  tinyint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`CALC_DATE`,`KEY_ID`, `SCOPE_ID`, `BRAND_CODE`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`KEY_ID`) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `MNCT`.`SQA_POC_K4` (\n" +
                "    `CALC_DATE` date NULL COMMENT \"\",\n" +
                "    `KEY_ID` char(36) NULL COMMENT \"\",\n" +
                "    `SCOPE_ID` char(36) NULL COMMENT \"\",\n" +
                "    `BRAND_CODE` tinyint NULL COMMENT \"\",\n" +
                "    `a831F5D28_8205_403D_B9C1_4FC913AE3533`  tinyint NULL,\n" +
                "    `aF8EF0D68_B9D7_4BA1_98D3_F70F464A06FE`  tinyint NULL,\n" +
                "    `a913D7E69_B7B4_4F4B_AFB8_65C9ADDEF9C1`  tinyint NULL,\n" +
                "    `aD53DABBC_635C_4E5F_A961_7544B753F564`  tinyint NULL,\n" +
                "    `aD6C3E356_08E6_4F7F_853E_74C72F088639`  tinyint NULL,\n" +
                "    `aF94756DB_3481_4F07_9EE3_319D2E9AE389`  tinyint NULL,\n" +
                "    `a103`  tinyint NULL,\n" +
                "    `a104`  tinyint NULL,\n" +
                "    `a105`  tinyint NULL,\n" +
                "    `a106`  tinyint NULL,\n" +
                "    `a107`  tinyint NULL,\n" +
                "    `a108`  tinyint NULL,\n" +
                "    `a109`  tinyint NULL,\n" +
                "    `a110`  tinyint NULL,\n" +
                "    `a111`  tinyint NULL,\n" +
                "    `a112`  tinyint NULL,\n" +
                "    `a31DC2494_0CD2_40B1_A93C_BB0846939CAE`  tinyint NULL,\n" +
                "    `aA7E37603_DA70_4C6A_B45F_9110CBFB8491`  tinyint NULL,\n" +
                "    `a0099fd65_bc5d_4b9b_aeb4_7323935031af`  tinyint NULL,\n" +
                "    `a00f73c93_c80b_482d_936b_aece0e76a4af`  tinyint NULL,\n" +
                "    `a0480607e_1f52_400f_9b55_d18305a5a3d8`  tinyint NULL,\n" +
                "    `a054f3532_6223_4cae_9b31_413b94078b63`  tinyint NULL,\n" +
                "    `a0565910D_A270_4409_A04A_1D79942AC76A`  tinyint NULL,\n" +
                "    `a0cefc660_f2f1_4ec9_bfef_e51be874082c`  tinyint NULL,\n" +
                "    `a0f65ef13_978a_4907_92f4_e637363c5d02`  tinyint NULL,\n" +
                "    `a16274db5_18a9_4f52_9900_bea49167b18f`  tinyint NULL,\n" +
                "    `a167ef6ba_bacc_4106_a712_dbb019d22317`  tinyint NULL,\n" +
                "    `a19FCC357_9F8E_47E5_9D6E_08FC4D91F753`  tinyint NULL,\n" +
                "    `a1DB82793_8E2B_4525_8273_B6380B240450`  tinyint NULL,\n" +
                "    `a1FB29CB2_4B79_490B_B8F3_6FAD9B8EC02E`  tinyint NULL,\n" +
                "    `a247f5072_6b3e_4551_b667_ed121f7e8548`  tinyint NULL,\n" +
                "    `a2cacd1a8_59a3_4b78_9366_ff0012b35c4c`  tinyint NULL,\n" +
                "    `a2f053a69_689d_4c6a_b81c_64345cc97a30`  tinyint NULL,\n" +
                "    `a2FD017C0_F957_4E02_8AF5_4A8A142E25BF`  tinyint NULL,\n" +
                "    `a3dcec36f_7d66_4183_a99d_7345491d3691`  tinyint NULL,\n" +
                "    `a420c08ed_4619_4b43_a908_598dc7e83ec8`  tinyint NULL,\n" +
                "    `a4b4387be_47e9_43e7_becb_d294011d5083`  tinyint NULL,\n" +
                "    `a4B922B92_2370_45A3_AA5C_CFEF8C084A52`  tinyint NULL,\n" +
                "    `a4fc6dffb_b4fe_42ef_b25e_02783af84136`  tinyint NULL,\n" +
                "    `a530d68fe_58c9_4d3d_af06_40a555558e0e`  tinyint NULL,\n" +
                "    `a61e77b2c_8a34_4b54_9672_92415ecaeb54`  tinyint NULL,\n" +
                "    `a638705f5_f77f_4b4c_a153_7af65ba78cae`  tinyint NULL,\n" +
                "    `a64a247fb_f961_42d2_bc52_900eeb4f0261`  tinyint NULL,\n" +
                "    `a67893f26_82f5_4e5c_8693_88c467e8736c`  tinyint NULL,\n" +
                "    `a683272e8_94b1_4dcb_9dfd_eec0057341b4`  tinyint NULL,\n" +
                "    `a71c413cb_78b2_4c8b_9e5a_65559b0d7c36`  tinyint NULL,\n" +
                "    `a8051B908_51EC_4F30_A89B_2A44E59E7E1E`  tinyint NULL,\n" +
                "    `a85758fb9_b85f_4fe6_825c_7733f144047b`  tinyint NULL,\n" +
                "    `a85970d1c_8a65_41d3_b489_bd4ae4934f3b`  tinyint NULL,\n" +
                "    `a89EEC418_B612_4FCA_8C26_3722469D3D58`  tinyint NULL,\n" +
                "    `a9c4be392_6e0e_471f_bb67_690de85ea664`  tinyint NULL,\n" +
                "    `a9ea019ad_ef5e_4453_8e28_003ffbd64be5`  tinyint NULL,\n" +
                "    `aa8631188_a0b2_49ee_8625_acd548ac0b16`  tinyint NULL,\n" +
                "    `ab6808d0c_e1ef_47bc_9727_12e4ed7bafec`  tinyint NULL,\n" +
                "    `ab9912ae3_918f_4f3a_a92c_2afc8a699efa`  tinyint NULL,\n" +
                "    `aba30c165_3140_4436_8cbe_1d1c8e076840`  tinyint NULL,\n" +
                "    `abaa217e5_15ff_40d3_aabd_d0441143c4e5`  tinyint NULL,\n" +
                "    `ac7ee5c16_88a4_433d_be76_f9a1548be054`  tinyint NULL,\n" +
                "    `adcfd9dfc_4e0d_489b_bd8a_70b918c98dd1`  tinyint NULL,\n" +
                "    `ae34baa00_b33c_43be_a304_6b1e362932db`  tinyint NULL,\n" +
                "    `ae7eec89f_fe24_4018_bd19_77f5d0301823`  tinyint NULL,\n" +
                "    `aeac339f5_a347_47b1_8f7d_ea408e9bbec7`  tinyint NULL,\n" +
                "    `aeb010384_3349_4695_b0fa_4940548fe902`  tinyint NULL,\n" +
                "    `aeb1645f9_d0db_40ca_b01d_48e5a82e2617`  tinyint NULL,\n" +
                "    `aedddca9b_7839_4d49_a2f0_9456dc5b731d`  tinyint NULL,\n" +
                "    `aee0ce3ac_cfa5_4494_8b59_3b9dbe5fd101`  tinyint NULL,\n" +
                "    `af1a57fdf_4107_4e60_9ff2_7af64d82ed4a`  tinyint NULL,\n" +
                "    `af47f92de_1d51_4f1a_aba2_e56459c5aa4a`  tinyint NULL,\n" +
                "    `afe50f382_b15e_4519_a020_ba4dd984a6ac`  tinyint NULL,\n" +
                "    `a03CA9197_5F43_4F82_A052_AF04F2EE300E`  tinyint NULL,\n" +
                "    `a0C47B129_A1D9_49C4_AFCA_80E1E0E3C611`  tinyint NULL,\n" +
                "    `a11E9BAC1_427B_4122_9FEB_08419B894F01`  tinyint NULL,\n" +
                "    `a154B8A95_61FB_449B_BD94_A774AECEC411`  tinyint NULL,\n" +
                "    `a1C141804_76B0_4A5C_B193_08878866E399`  tinyint NULL,\n" +
                "    `a1FE741CE_0BAC_4A82_A5C7_99D57EAA985B`  tinyint NULL,\n" +
                "    `a234562AC_C407_4465_B49C_2D5D22C292E7`  tinyint NULL,\n" +
                "    `a23937D40_A644_41FA_9649_18D4006DFF74`  tinyint NULL,\n" +
                "    `a289D930C_C423_45D6_8AB1_DF9949BA2D4E`  tinyint NULL,\n" +
                "    `a30C93CCD_7F86_4D35_9B44_D7990AB7568F`  tinyint NULL,\n" +
                "    `a37874A7F_AF0E_4CB2_95AB_413BE14EB1EA`  tinyint NULL,\n" +
                "    `a40D4B2DC_81C5_4B8A_A9EC_30F594727818`  tinyint NULL,\n" +
                "    `a4167C663_0568_4728_93D1_B503039916B3`  tinyint NULL,\n" +
                "    `a472FC4C2_7297_4C4D_B6C6_D92C177647EF`  tinyint NULL,\n" +
                "    `a48772C9F_2883_4262_BDF1_960D93646D94`  tinyint NULL,\n" +
                "    `a52EF8E44_3A3F_4FF6_9A13_936AE00F04A5`  tinyint NULL,\n" +
                "    `a5730D2CA_B590_4ABA_ACC8_A581B528CAB1`  tinyint NULL,\n" +
                "    `a5B23235E_3220_4A21_BA37_98D33BC7893B`  tinyint NULL,\n" +
                "    `a5F935915_5CDA_4351_AA83_2196ED5D464B`  tinyint NULL,\n" +
                "    `a60C73D49_05D6_4A76_9224_F440EEB89938`  tinyint NULL,\n" +
                "    `a6A21E2DF_44C1_45FB_B509_DD19438BADCE`  tinyint NULL,\n" +
                "    `a7294F5B8_2D90_4EDE_B403_D261BA062305`  tinyint NULL,\n" +
                "    `a738458A9_EAE6_41E7_98B3_BDDDA966DFAF`  tinyint NULL,\n" +
                "    `a7A1DE04F_E232_42BF_A613_28A5BBEA181E`  tinyint NULL,\n" +
                "    `a7E637BDB_0EC1_4064_911E_8FA7B0C8E036`  tinyint NULL,\n" +
                "    `a7E70AEEE_D66D_4312_ADE2_047635D47C8C`  tinyint NULL,\n" +
                "    `a81F66F75_C78D_4367_B493_3D89C78FBEBF`  tinyint NULL,\n" +
                "    `a8452B3DE_E608_4EBA_B025_D38138B90BD1`  tinyint NULL,\n" +
                "    `a9222A48D_D489_41BB_B90D_23E5AA015364`  tinyint NULL,\n" +
                "    `a999A5750_A6CC_4DAC_8A66_D853B07F9501`  tinyint NULL,\n" +
                "    `aA2546DBC_0B02_4C47_938E_524BE8EE7AE0`  tinyint NULL,\n" +
                "    `aA8A4F860_9240_4352_A7C2_0D2CEA2B40E6`  tinyint NULL,\n" +
                "    `aAF3A914E_15C0_4186_A8DB_F7489E8CD556`  tinyint NULL,\n" +
                "    `aB0BE4177_69EA_457B_81C9_54028A032050`  tinyint NULL,\n" +
                "    `aC3B4A097_E056_4A72_9AFC_96AC924D1AE7`  tinyint NULL,\n" +
                "    `aCD7B67BF_B479_454C_9592_43E24B1363F2`  tinyint NULL,\n" +
                "    `aD17A63BC_4FC1_4E40_81BF_F3C5B4623B74`  tinyint NULL,\n" +
                "    `aD5DB3666_6367_46BA_BDEC_AC82FD38C4D3`  tinyint NULL,\n" +
                "    `aDA4FD67F_CBD7_4B7E_9BB7_39DC7EDE4B6F`  tinyint NULL,\n" +
                "    `aE19B019D_5DE0_421C_9113_5B3EE28CAE6D`  tinyint NULL,\n" +
                "    `aE4A168DD_4BBD_4909_86F1_70167AA7D538`  tinyint NULL,\n" +
                "    `aE5015F48_1D96_461B_B92E_2711587B902B`  tinyint NULL,\n" +
                "    `aE92D8A31_DB3D_475D_877D_2C66EBF5B910`  tinyint NULL,\n" +
                "    `aEB6EC514_0B8D_4136_A01D_2C85793768AA`  tinyint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`CALC_DATE`,`KEY_ID`, `SCOPE_ID`, `BRAND_CODE`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`KEY_ID`) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");\n");
        starRocksAssert.withTable("CREATE TABLE `MNCT`.`SQA_POC_KDF` (\n" +
                "\t`CALC_DATE` date NULL COMMENT \"\",\n" +
                "  \t`BRAND_CODE` tinyint NULL COMMENT \"\",\n" +
                "  \t`SCOPE_ID` char(36) NULL COMMENT \"\",\n" +
                "  \t`KEY_ID` char(36) NULL COMMENT \"\",\n" +
                "  \t`STATUS` varchar(2) NULL COMMENT \"\",\n" +
                "  \t`WEEKLY_SELL_TIME` varchar(30) NULL COMMENT \"\",\n" +
                "  \t`WEEKLY_SELL` varchar(2) NULL COMMENT \"\",\n" +
                "  \t`NATIONAL_HOLIDAY_IS_SELL` varchar(2) NULL COMMENT \"\",\n" +
                "  \t`BEGIN_TIME` datetime NULL COMMENT \"\",\n" +
                "  \t`END_TIME` datetime NULL COMMENT \"\",\n" +
                "  \t`NORMAL_DATE` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`CALC_DATE`, `BRAND_CODE`, `SCOPE_ID`, `KEY_ID`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`KEY_ID`) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        OlapTable SqaPocK = (OlapTable) catalog.getDb("default_cluster:MNCT").getTable("SQA_POC_K");
        setTableStatistics(SqaPocK, 84977);

        OlapTable sqaPocK1 = (OlapTable) catalog.getDb("default_cluster:MNCT").getTable("SQA_POC_K1");
        setTableStatistics(sqaPocK1, 398176);

        OlapTable sqaPocK2 = (OlapTable) catalog.getDb("default_cluster:MNCT").getTable("SQA_POC_K2");
        setTableStatistics(sqaPocK2, 392447);

        OlapTable sqaPocK3 = (OlapTable) catalog.getDb("default_cluster:MNCT").getTable("SQA_POC_K3");
        setTableStatistics(sqaPocK3, 316219);

        OlapTable sqaPocK4 = (OlapTable) catalog.getDb("default_cluster:MNCT").getTable("SQA_POC_K4");
        setTableStatistics(sqaPocK4, 316219);

        OlapTable SqaPocKdf = (OlapTable) catalog.getDb("default_cluster:MNCT").getTable("SQA_POC_KDF");
        setTableStatistics(SqaPocKdf, 306803);

        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
    }
}
