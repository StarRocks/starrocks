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


package com.starrocks.sql.parser;

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.StatementBase;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
public class ParserBench {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ParserBench.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    private String sql;

    @Setup
    public void setup() {
        // sql = generateSQL();
        sql = "with temp as (\n" +
                "    select\n" +
                "        '整体' as '分类',\n" +
                "        a.category as '品类',\n" +
                "        a.txall as '总合计',\n" +
                "        a.txall - coalesce(a.txxc, 0) as '前台',\n" +
                "        coalesce(a.txxc, 0) as '下沉',\n" +
                "        a.mrkt as '行业当日',\n" +
                "        a.txall / a.mrkt as '市占率',\n" +
                "case\n" +
                "            when b.txall_ago = 0 then 0\n" +
                "            else a.txall / b.txall_ago -1\n" +
                "        end as '总同比',\n" +
                "case\n" +
                "            when b.txqt_ago = 0 then 0\n" +
                "            else (a.txall - coalesce(a.txxc, 0)) / b.txqt_ago -1\n" +
                "        end as '前台同比',\n" +
                "        a.txxc / txxc_ago -1 as '下沉同比',\n" +
                "case\n" +
                "            when b.mrkt_ago = 0 then 0\n" +
                "            else a.mrkt / b.mrkt_ago -1\n" +
                "        end as '行业同比',\n" +
                "case\n" +
                "            when c.jp_all = 0 then 0\n" +
                "            else a.txall / c.jp_all\n" +
                "        end as '总控比',\n" +
                "case\n" +
                "            when c.jp_qt = 0 then 0\n" +
                "            else (a.txall - coalesce(a.txxc, 0)) / c.jp_qt\n" +
                "        end as '前台控比',\n" +
                "        a.txxc / if(c.jp_all - c.jp_qt < 0, '-', c.jp_all - c.jp_qt) as '下沉控比',\n" +
                "        ifnull(d.qt_plan * 10000, 0) as '前台目标',\n" +
                "case\n" +
                "            when ifnull(d.qt_plan * 10000, 0) = 0 then 1\n" +
                "            else (a.txall - coalesce(a.txxc, 0)) /(d.qt_plan * 10000)\n" +
                "        end as '前台达成',\n" +
                "        c.jp_all as '竞品合计',\n" +
                "        if(c.jp_all - c.jp_qt < 0, c.jp_all, c.jp_qt) as '竞品前台',\n" +
                "        if(c.jp_all - c.jp_qt < 0, '0', c.jp_all - c.jp_qt) as '竞品下沉',\n" +
                "        coalesce(e.jp_yc * 10000, 0) as '异常金额',\n" +
                "case\n" +
                "            when a.category in ('电热水器', '燃气热水器', '壁挂炉') then '海尔'\n" +
                "            when a.category = '油烟机' then '华帝'\n" +
                "            when a.category = '燃气灶' then '苏泊尔'\n" +
                "            when a.category = '洗碗机' then '西门子'\n" +
                "            when a.category = '消毒柜' then '康宝'\n" +
                "            when a.category = '集成家电' then '美大'\n" +
                "            when a.category = '饮水机' then '九阳'\n" +
                "            when a.category = '净水器' then '沁园'\n" +
                "        end as '竞品品牌'\n" +
                "    from\n" +
                "        (\n" +
                "            select\n" +
                "                tx.*\n" +
                "            from\n" +
                "(\n" +
                "                    select\n" +
                "                        tx.category,\n" +
                "                        sum(\n" +
                "                            case\n" +
                "                                when tx.brand = '美的'\n" +
                "                                and source_code = 'sycm_brand' then tx.sale_amt\n" +
                "                                else 0\n" +
                "                            end\n" +
                "                        ) as txall,\n" +
                "                        sum(\n" +
                "                            case\n" +
                "                                when plat = '淘系下沉'\n" +
                "                                and source_code = 'txxc' then sale_amt\n" +
                "                                else 0.0\n" +
                "                            end\n" +
                "                        ) as txxc,\n" +
                "                        sum(\n" +
                "                            case\n" +
                "                                when source_code = 'sycm_brand' then tx.sale_amt\n" +
                "                                else 0\n" +
                "                            end\n" +
                "                        ) as mrkt\n" +
                "                    from\n" +
                "                        ads_cr_act_industry_brand_now tx\n" +
                "                    where\n" +
                "                        tx.stat_date = '2023-11-02'\n" +
                "                    group by\n" +
                "                        tx.category\n" +
                "                    union\n" +
                "                    all\n" +
                "                    select\n" +
                "                        '合计' as category,\n" +
                "                        sum(\n" +
                "                            case\n" +
                "                                when tx.brand = '美的'\n" +
                "                                and source_code = 'sycm_brand' then tx.sale_amt\n" +
                "                                else 0\n" +
                "                            end\n" +
                "                        ) as txall,\n" +
                "                        sum(\n" +
                "                            case\n" +
                "                                when plat = '淘系下沉'\n" +
                "                                and source_code = 'txxc' then sale_amt\n" +
                "                                else 0.0\n" +
                "                            end\n" +
                "                        ) as txxc,\n" +
                "                        sum(\n" +
                "                            case\n" +
                "                                when source_code = 'sycm_brand' then tx.sale_amt\n" +
                "                                else 0\n" +
                "                            end\n" +
                "                        ) as mrkt\n" +
                "                    from\n" +
                "                        ads_cr_act_industry_brand_now tx\n" +
                "                    where\n" +
                "                        tx.stat_date = '2023-11-02'\n" +
                "                ) tx\n" +
                "        ) as a\n" +
                "        left join (\n" +
                "            select\n" +
                "                category,\n" +
                "                sum(\n" +
                "                    case\n" +
                "                        when brand = '美的'\n" +
                "                        and source_code = 'ods_mbi_sycm'\n" +
                "                        and plat = '淘系' then sale_amt\n" +
                "                        else 0\n" +
                "                    end\n" +
                "                ) as txall_ago,\n" +
                "                sum(\n" +
                "                    case\n" +
                "                        when brand = '美的'\n" +
                "                        and source_code = 'ods_mbi_sycm'\n" +
                "                        and plat = '淘系前台' then sale_amt\n" +
                "                        else 0\n" +
                "                    end\n" +
                "                ) as txqt_ago,\n" +
                "                sum(\n" +
                "                    case\n" +
                "                        when brand = '美的'\n" +
                "                        and source_code = 'ods_tmall_youpin_sale_store'\n" +
                "                        and plat = '淘系下沉离线' then sale_amt\n" +
                "                        else 0\n" +
                "                    end\n" +
                "                ) as txxc_ago,\n" +
                "                sum(\n" +
                "                    case\n" +
                "                        when brand = '行业'\n" +
                "                        and source_code = 'ods_mbi_sycm' then sale_amt\n" +
                "                        else 0\n" +
                "                    end\n" +
                "                ) as mrkt_ago\n" +
                "            from\n" +
                "                ads_cr_act_industry_brand_d\n" +
                "            where\n" +
                "                stat_date = date_sub('2023-11-02', interval 1 year)\n" +
                "                and category <> ''\n" +
                "            group by\n" +
                "                category\n" +
                "            union\n" +
                "            all\n" +
                "            select\n" +
                "                '合计' as category,\n" +
                "                sum(\n" +
                "                    case\n" +
                "                        when brand = '美的'\n" +
                "                        and source_code = 'ods_mbi_sycm'\n" +
                "                        and plat = '淘系' then sale_amt\n" +
                "                        else 0\n" +
                "                    end\n" +
                "                ) as txall_ago,\n" +
                "                sum(\n" +
                "                    case\n" +
                "                        when brand = '美的'\n" +
                "                        and source_code = 'ods_mbi_sycm'\n" +
                "                        and plat = '淘系前台' then sale_amt\n" +
                "                        else 0\n" +
                "                    end\n" +
                "                ) as txqt_ago,\n" +
                "                sum(\n" +
                "                    case\n" +
                "                        when brand = '美的'\n" +
                "                        and source_code = 'ods_tmall_youpin_sale_store'\n" +
                "                        and plat = '淘系下沉离线' then sale_amt\n" +
                "                        else 0\n" +
                "                    end\n" +
                "                ) as txxc_ago,\n" +
                "                sum(\n" +
                "                    case\n" +
                "                        when brand = '行业'\n" +
                "                        and source_code = 'ods_mbi_sycm' then sale_amt\n" +
                "                        else 0\n" +
                "                    end\n" +
                "                ) as mrkt_ago\n" +
                "            from\n" +
                "                ads_cr_act_industry_brand_d\n" +
                "            where\n" +
                "                stat_date = date_sub('2023-11-02', interval 1 year)\n" +
                "        ) as b on a.category = b.category\n" +
                "        left join [bucket] (\n" +
                "            select\n" +
                "                category,\n" +
                "                sum(\n" +
                "                    case\n" +
                "                        when source_code = 'sycm_brand'\n" +
                "                        and jp_flag not in ('美的', '非此品类竞对品牌') then sale_amt\n" +
                "                        else 0\n" +
                "                    end\n" +
                "                ) as jp_all,\n" +
                "                sum(\n" +
                "                    case\n" +
                "                        when source_code = 'txqt'\n" +
                "                        and jp_flag not in ('美的', '非此品类竞对品牌') then sale_amt\n" +
                "                        else 0\n" +
                "                    end\n" +
                "                ) as jp_qt\n" +
                "            from\n" +
                "                ads_cr_act_industry_brand_now\n" +
                "            where\n" +
                "                stat_date = '2023-11-02'\n" +
                "            group by\n" +
                "                category\n" +
                "            union\n" +
                "            all\n" +
                "            select\n" +
                "                '合计' as category,\n" +
                "                sum(\n" +
                "                    case\n" +
                "                        when source_code = 'sycm_brand'\n" +
                "                        and jp_flag not in ('美的', '非此品类竞对品牌') then sale_amt\n" +
                "                        else 0\n" +
                "                    end\n" +
                "                ) as jp_all,\n" +
                "                sum(\n" +
                "                    case\n" +
                "                        when source_code = 'txqt'\n" +
                "                        and jp_flag not in ('美的', '非此品类竞对品牌') then sale_amt\n" +
                "                        else 0\n" +
                "                    end\n" +
                "                ) as jp_qt\n" +
                "            from\n" +
                "                ads_cr_act_industry_brand_now\n" +
                "            where\n" +
                "                stat_date = '2023-11-02'\n" +
                "        ) as c on a.category = c.category\n" +
                "        left join [bucket] (\n" +
                "            select\n" +
                "                category,\n" +
                "                sum(\n" +
                "                    case\n" +
                "                        when plat = '淘系前台' then sale_plan\n" +
                "                        else 0.0\n" +
                "                    end\n" +
                "                ) as qt_plan\n" +
                "            from\n" +
                "                ads_cr_act_t_daily_sale_plan\n" +
                "            where\n" +
                "                stat_date = '2023-11-02'\n" +
                "            group by\n" +
                "                category\n" +
                "            union\n" +
                "            all\n" +
                "            select\n" +
                "                '合计' as category,\n" +
                "                sum(\n" +
                "                    case\n" +
                "                        when plat = '淘系前台' then sale_plan\n" +
                "                        else 0.0\n" +
                "                    end\n" +
                "                ) as qt_plan\n" +
                "            from\n" +
                "                ads_cr_act_t_daily_sale_plan\n" +
                "            where\n" +
                "                stat_date = '2023-11-02'\n" +
                "        ) as d on a.category = d.category\n" +
                "        left join [bucket] (\n" +
                "            SELECT\n" +
                "                category,\n" +
                "                sum(jp_yc) as jp_yc\n" +
                "            from\n" +
                "(\n" +
                "                    select\n" +
                "                        category,\n" +
                "                        sum(\n" +
                "                            case\n" +
                "                                when plat = '淘系前台' then sale_amt\n" +
                "                                else 0.0\n" +
                "                            end\n" +
                "                        ) as jp_yc\n" +
                "                    from\n" +
                "                        ads_cr_act_midea_er_plat_cate_sale\n" +
                "                    where\n" +
                "                        stat_date = '2023-11-02'\n" +
                "                    group by\n" +
                "                        category\n" +
                "                    union\n" +
                "                    all\n" +
                "                    select\n" +
                "                        '合计' as category,\n" +
                "                        sum(\n" +
                "                            case\n" +
                "                                when plat = '淘系前台' then sale_amt\n" +
                "                                else 0.0\n" +
                "                            end\n" +
                "                        ) as jp_yc\n" +
                "                    from\n" +
                "                        ads_cr_act_midea_er_plat_cate_sale\n" +
                "                    where\n" +
                "                        stat_date = '2023-11-02'\n" +
                "                    group by\n" +
                "                        category\n" +
                "                ) a\n" +
                "            group by\n" +
                "                category\n" +
                "        ) e on a.category = e.category\n" +
                ")\n" +
                "SELECT\n" +
                "    A27_T_1_.`品类` AS T_AFB_2_,\n" +
                "    A27_T_1_.`总合计` AS T_A87_3_,\n" +
                "    A27_T_1_.`市占率` AS T_A21_4_,\n" +
                "    A27_T_1_.`总同比` AS T_A7E_5_,\n" +
                "    A27_T_1_.`总控比` AS T_AEE_6_,\n" +
                "    A27_T_1_.`前台` AS T_A33_7_,\n" +
                "    A27_T_1_.`前台达成` AS T_AFA_8_,\n" +
                "    A27_T_1_.`前台同比` AS T_A15_9_,\n" +
                "    A27_T_1_.`前台控比` AS T_AFA_10_,\n" +
                "    A27_T_1_.`下沉` AS T_AD7_11_,\n" +
                "    A27_T_1_.`下沉同比` AS T_AAF_12_,\n" +
                "    A27_T_1_.`下沉控比` AS T_AC3_13_,\n" +
                "    A27_T_1_.`行业同比` AS T_A6B_14_,\n" +
                "    A27_T_1_.`竞品品牌` AS T_AA4_15_,\n" +
                "    A27_T_1_.`竞品合计` AS T_A30_16_,\n" +
                "    A27_T_1_.`竞品前台` AS T_A79_17_,\n" +
                "    CAST(A27_T_1_.`竞品下沉` AS decimal(15, 3)) AS T_AF7_18_,\n" +
                "    A27_T_1_.`异常金额` AS T_A90_19_\n" +
                "FROM\n" +
                "    (\n" +
                "        select\n" +
                "            *\n" +
                "        from\n" +
                "            temp\n" +
                "    ) AS A27_T_1_\n" +
                "WHERE\n" +
                "    A27_T_1_.`品类` = '合计'\n" +
                "ORDER BY\n" +
                "    T_A87_3_ DESC\n" +
                "LIMIT\n" +
                "    0, 1000;";
    }

    @Param({"SLL", "LL"})
    public String mode;

    @Param({"5000"})
    public int times;

    @Param({"true"})
    public boolean isRightSql;

    @Param({"true"})
    public boolean isLimit;

    @Benchmark
    public void parseInsertIntoValues() {
        parseSql(sql);
    }

    private String generateSQL() {
        List<String> wrongValues = Lists.newArrayList("K0.14044384266968246155471433667116798460483551025390625",
                "-1869445626", "K0.17698452552099786", "K127", "k-366217216");
        List<String> rightValues = Lists.newArrayList("0.14044384266968246155471433667116798460483551025390625",
                "-1869445626", "0.17698452552099786", "127", "-366217216");

        String joined;
        if (isRightSql) {
            joined = String.join(",", rightValues);
        } else {
            joined = String.join(",", wrongValues);
        }
        StringJoiner result = new StringJoiner(",", "(", ")");
        for (int i = 0; i < times; i++) {
            result.add(joined);
        }
        return "INSERT INTO test_load_decimal_1_0 VALUES " + result + ";";
    }

    private StatementBase parseSql(String sql) {
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        StarRocksParser parser = new StarRocksParser(tokenStream);
        parser.removeErrorListeners();
        parser.addErrorListener(new BaseErrorListener());
        parser.removeParseListeners();
        if (isLimit) {
            parser.addParseListener(new PostProcessListener(100000000, Config.expr_children_limit));
        }
        parser.getInterpreter().setPredictionMode(mode.equals("SLL") ? PredictionMode.SLL : PredictionMode.LL);
        StarRocksParser.SqlStatementsContext sqlStatements = parser.sqlStatements();
        return (StatementBase) new AstBuilder(SqlModeHelper.MODE_DEFAULT)
                .visitSingleStatement(sqlStatements.singleStatement(0));
    }

}
