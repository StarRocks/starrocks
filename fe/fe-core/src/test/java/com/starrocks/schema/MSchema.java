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

package com.starrocks.schema;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * MSchema represents Mocked Schema, it's a mocked table marketing which users can find
 * your needs' tables.
 *
 * NOTE:
 *  Now only support MTables directly because many fe uts can visit tables directly.
 *  We can support MDatabase with different MTables later.
 */
public class MSchema {
    public static final MTable EMPS = new MTable("emps", "empid",
            List.of(
                    "empid      INT         NOT NULL",
                    "deptno     INT         NOT NULL",
                    "locationid INT         NOT NULL",
                    "commission INT         NOT NULL",
                    "name       VARCHAR(20) NOT NULL",
                    "salary     DECIMAL(18, 2)"
            )
    )
            .withProperties("'foreign_key_constraints' = '(deptno) REFERENCES depts(deptno)'")
            .withValues("(1, 1, 1, 1, 'emp_name1', 100), " +
                    "(2, 1, 2, 2, 'emp_name1', 120), " +
                    "(3, 1, 2, 2, 'emp_name1', 150)");

    public static final MTable EMPS_NO_CONSTRAINT = new MTable("emps_no_constraint", "empid",
            List.of(
                    "empid      INT         NOT NULL",
                    "deptno     INT         NOT NULL",
                    "locationid INT         NOT NULL",
                    "commission INT         NOT NULL",
                    "name       VARCHAR(20) NOT NULL",
                    "salary     DECIMAL(18, 2)"
            )
    )
            .withValues("(1, 1, 1, 1, 'emp_name1', 100), " +
                    "(2, 1, 2, 2, 'emp_name1', 120), " +
                    "(3, 1, 2, 2, 'emp_name1', 150)");

    public static final MTable EMPS_BIGINT = new MTable("emps_bigint", "empid",
            List.of(
                    "empid      BIGINT         NOT NULL",
                    "deptno     BIGINT         NOT NULL",
                    "locationid BIGINT         NOT NULL",
                    "commission BIGINT         NOT NULL",
                    "name       VARCHAR(20) NOT NULL",
                    "salary     DECIMAL(18, 2)"
            )
    )
            .withValues("(1, 1, 1, 1, 'emp_name1', 100), " +
                    "(2, 1, 2, 2, 'emp_name1', 120), " +
                    "(3, 1, 2, 2, 'emp_name1', 150)");

    public static final MTable EMPS_NULL = new MTable("emps_null", "empid",
            List.of(
                    "empid      INT         NULL",
                    "deptno     INT         NULL",
                    "locationid INT         NULL",
                    "commission INT         NULL",
                    "name       VARCHAR(20) NULL",
                    "salary     DECIMAL(18, 2)"
            )
    )
            .withValues("(1, 1, 1, 1, 'emp_name1', 100), " +
                    "(2, 1, 2, 2, 'emp_name1', 120), " +
                    "(3, 1, 2, 2, 'emp_name1', 150)");
    public static final MTable EMPS_PAR = new MTable("emps_par", "empid",
            List.of(
                    "empid int not null",
                    "deptno int not null",
                    "name varchar(25) not null",
                    "salary double"
            ),
            "deptno",
            List.of(
                    "PARTITION p1 VALUES [(\"-2147483648\"), (\"2\"))",
                    "PARTITION p2 VALUES [(\"2\"), (\"3\"))",
                    "PARTITION p3 VALUES [(\"3\"), (\"4\"))"
            )
    );

    public static final MTable DEPTS = new MTable("depts", "deptno",
            List.of(
                    "deptno int not null",
                    "name varchar(25) not null"
            )
    )
            .withProperties("'unique_constraints' = 'deptno'")
            .withValues("(1, 'dept_name1'), (2, 'dept_name2'), (3, 'dept_name3')");

    public static final MTable DEPTS_NULL = new MTable("depts_null", "deptno",
            List.of(
                    "deptno int null",
                    "name varchar(25) null"
            )
    )
            .withProperties("'unique_constraints' = 'deptno'")
            .withValues("(1, 'dept_name1'), (2, 'dept_name2'), (3, 'dept_name3')");

    public static final MTable DEPENDENTS = new MTable("dependents", "empid",
            List.of(
                    "empid int not null",
                    "name varchar(25) not null"
            )
    ).withValues("(1, 'dependent_name1')");

    public static final MTable LOCATIONS = new MTable("locations", "locationid",
            List.of(
                    "locationid INT NOT NULL",
                    "state CHAR(2)",
                    "name varchar(25) not null"
            )
    )
            .withValues("(1, 1, 'location1')");

    public static final MTable TEST_ALL_TYPE = new MTable("test_all_type", "t1a",
            List.of(

                    "  `t1a` varchar(20) NULL",
                    "  `t1b` smallint(6) NULL",
                    "  `t1c` int(11) NULL",
                    "  `t1d` bigint(20) NULL",
                    "  `t1e` float NULL",
                    "  `t1f` double NULL",
                    "  `t1g` bigint(20) NULL",
                    "  `id_datetime` datetime NULL",
                    "  `id_date` date NULL",
                    "  `id_decimal` decimal(10,2) NULL "
            )
    ).withValues("('value1', 1, 2, 3, 4.0, 5.0, 6, '2022-11-11 10:00:01', '2022-11-11', 10.12)");

    public static final MTable TABLE_WITH_PARTITION = new MTable("table_with_partition", "t1a",
            List.of(
                    "  `t1a` varchar(20) NULL",
                    "  `id_date` date NULL",
                    "  `t1b` smallint(6) NULL",
                    "  `t1c` int(11) NULL",
                    "  `t1d` bigint(20) NULL"
            ),
            "id_date",
            List.of(
                    "PARTITION p1991 VALUES [('1991-01-01'), ('1992-01-01'))",
                    "PARTITION p1992 VALUES [('1992-01-01'), ('1993-01-01'))",
                    "PARTITION p1993 VALUES [('1993-01-01'), ('1994-01-01'))"
            ),
            "`t1a`,`id_date`"
    ).withValues("('varchar1', '1991-02-01', 1, 1, 1), " +
            "('varchar2','1992-02-01', 2, 1, 1), " +
            "('varchar3', '1993-02-01', 3, 1, 1)");

    public static final MTable TABLE_WITH_DAY_PARTITION = new MTable("table_with_day_partition", "t1a",
            List.of(
                    "  `t1a` varchar(20) NULL",
                    "  `id_date` date NULL",
                    "  `t1b` smallint(6) NULL",
                    "  `t1c` int(11) NULL",
                    "  `t1d` bigint(20) NULL"
            ),
            "id_date",
            List.of(
                    "PARTITION p19910330 VALUES [('1991-03-30'), ('1991-03-31'))",
                    "PARTITION p19910331 VALUES [('1991-03-31'), ('1991-04-01'))",
                    "PARTITION p19910401 VALUES [('1991-04-01'), ('1991-04-02'))",
                    "PARTITION p19910402 VALUES [('1991-04-02'), ('1991-04-03'))"
            ),
            "`t1a`,`id_date`"
    ).withValues("('varchar1', '1991-03-30', 1, 1, 1)," +
            "('varchar2', '1991-03-31', 2, 1, 1), " +
            "('varchar3', '1991-04-01', 3, 1, 1)," +
            "('varchar3', '1991-04-02', 4, 1, 1)");

    public static final MTable TABLE_WITH_DATETIME_PARTITION = new MTable("table_with_datetime_partition", "t1a",
            List.of(
                    "  `t1a` varchar(20) NULL",
                    "  `id_datetime` datetime NULL",
                    "  `t1b` smallint(6) NULL",
                    "  `t1c` int(11) NULL",
                    "  `t1d` bigint(20) NULL"
            ),
            "id_datetime",
            List.of(
                    "PARTITION p19910330 VALUES [('1991-03-30'), ('1991-03-31'))",
                    "PARTITION p19910331 VALUES [('1991-03-31'), ('1991-04-01'))",
                    "PARTITION p19910401 VALUES [('1991-04-01'), ('1991-04-02'))",
                    "PARTITION p19910402 VALUES [('1991-04-02'), ('1991-04-03'))"
            ),
            "`t1a`,`id_datetime`"
    ).withValues("('varchar1', '1991-03-30', 1, 1, 1)," +
            "('varchar2', '1991-03-31', 2, 1, 1), " +
            "('varchar3', '1991-04-01', 3, 1, 1)," +
            "('varchar3', '1991-04-02', 4, 1, 1)");

    public static final MTable TEST_BASE_PART = new MTable("test_base_part", "c1",
            List.of(
                    "c1 int",
                    "c2 bigint",
                    "c3 bigint",
                    "c4 bigint"
            ),
            "c3",
            List.of(
                    " partition p1 values less than ('100')",
                    " partition p2 values less than ('200')",
                    " partition p3 values less than ('1000')",
                    " PARTITION p4 values less than ('2000')",
                    " PARTITION p5 values less than ('3000')"
            )
    );

    public static final MTable T0 = new MTable("t0", "v1",
            List.of(
                    "  `v1` bigint NULL",
                    "  `v2` bigint NULL",
                    "  `v3` bigint NULL"
            )
    ).withValues("(1, 2, 3)");

    public static final MTable T1 = new MTable("t1", "k1",
            List.of(
                    "  `k1` int(11) NULL",
                    "  `v1` int(11) NULL",
                    "  `v2` int(11) NULL"
            ),
            "k1",
            List.of(
                    "PARTITION p1 VALUES [('-2147483648'), ('2'))",
                    "PARTITION p2 VALUES [('2'), ('3'))",
                    "PARTITION p3 VALUES [('3'), ('4'))",
                    "PARTITION p4 VALUES [('4'), ('5'))",
                    "PARTITION p5 VALUES [('5'), ('6'))",
                    "PARTITION p6 VALUES [('6'), ('7'))"
            )
    ).withValues("(1,1,1),(1,1,2),(1,1,3),(1,2,1),(1,2,2),(1,2,3),(1,3,1),(1,3,2),(1,3,3)" +
            " ,(2,1,1),(2,1,2),(2,1,3),(2,2,1),(2,2,2),(2,2,3),(2,3,1),(2,3,2),(2,3,3)" +
            " ,(3,1,1),(3,1,2),(3,1,3),(3,2,1),(3,2,2),(3,2,3),(3,3,1),(3,3,2),(3,3,3)");

    public static final MTable T2 = new MTable("t2", "v1",
            List.of(
                    "  `v1` bigint NULL",
                    "  `v2` bigint NULL",
                    "  `v3` bigint NULL"
            )
    ).withValues("(1, 2, 3)");

    public static final MTable T_METRICS = new MTable("t_metrics", "c1",
            List.of(
                    " c1 int",
                    " c2 int",
                    " c3 int",
                    " c4 int",
                    " c5 int",
                    " c6 int",
                    " c7 int",
                    " c8 int"
            )
    );

    public static final MTable JSON_TBL = new MTable("json_tbl", "p_dt",
            List.of(
                    "  `p_dt` date NULL",
                    "  `d_user` json NULL "
            )
    ).withValues("('2020-01-01', '{'a': 1, 'gender': 'man'}')");

    public static final MTable TABLE_WITH_DAY_PARTITION1 = TABLE_WITH_DAY_PARTITION.copyWithName("table_with_day_partition1");
    public static final MTable TABLE_WITH_DAY_PARTITION2 = TABLE_WITH_DAY_PARTITION.copyWithName("table_with_day_partition2");

    public static final MTable TEST10 = new MTable("test10", "event_id",
            List.of(
                    "  `event_id` int NULL",
                    "  `event_type` varchar(65533) NULL ",
                    "  `event_time` datetime NULL "
            ),
            "event_time",
            List.of(
                    "PARTITION p20230105 VALUES [(\"2023-01-05 00:00:00\"), (\"2023-01-06 00:00:00\"))",
                    "PARTITION p20230106 VALUES [(\"2023-01-06 00:00:00\"), (\"2023-01-07 00:00:00\"))"
            )
    ).withValues("(1, 'a', '2023-01-05 10:20:22'),(2, 'b', '2023-01-05 10:20:22')," +
            "(11, 'aa', '2023-01-06 10:20:22'),(22, 'bb', '2023-01-06 10:20:22')");

    public static final MTable TEST11 = new MTable("test11", "event_id1",
            List.of(
                    "  `event_id1` int NULL",
                    "  `event_type1` varchar(65533) NULL ",
                    "  `event_time1` datetime NULL "
            ),
            "event_time1",
            List.of(
                    "PARTITION p20230105 VALUES [(\"2023-01-05 00:00:00\"), (\"2023-01-06 00:00:00\"))",
                    "PARTITION p20230106 VALUES [(\"2023-01-06 00:00:00\"), (\"2023-01-07 00:00:00\"))"
            )
    ).withValues("(1, 'a', '2023-01-05 10:20:22'),(2, 'b', '2023-01-05 10:20:23')," +
            "(11, 'aa', '2023-01-06 10:20:22'),(22, 'bbx', '2023-01-06 10:20:22')");

    public static final List<MTable>  TABLE_MARKETING = List.of(
            EMPS,
            EMPS_NULL,
            EMPS_BIGINT,
            EMPS_NO_CONSTRAINT,
            EMPS_PAR,
            DEPTS,
            DEPTS_NULL,
            DEPENDENTS,
            LOCATIONS,
            TEST_ALL_TYPE,
            T0,
            TABLE_WITH_PARTITION,
            TABLE_WITH_DAY_PARTITION,
            TABLE_WITH_DAY_PARTITION1,
            TABLE_WITH_DAY_PARTITION2,
            TABLE_WITH_DATETIME_PARTITION,
            TEST_BASE_PART,
            T1,
            JSON_TBL,
            T_METRICS,
            TEST10,
            TEST11,
            T2
    );
    public static final Map<String, MTable> TABLE_MAP = Maps.newHashMap();

    static {
        TABLE_MARKETING.stream().forEach(t -> TABLE_MAP.put(t.getTableName(), t));
    }

    public static MTable getTable(String tableName) {
        if (!TABLE_MAP.containsKey(tableName)) {
            throw new RuntimeException(String.format("%s is not in metadata marketing, please add it in the marketing",
                    tableName));
        }
        return TABLE_MAP.get(tableName);
    }
}
