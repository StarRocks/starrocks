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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import java.util.HashMap;
import java.util.Map;

/**
 * MVUnionRewriteMode is used to store union rewrite mode for materialized view union.
 * The mode is used to control the union rewrite behavior:
 *
 * <p>Default mode(materialized_view_union_rewrite_mode=0)</p>
 * Query = MV + (Query - MV)
 * Query - MV = not(Query Based MV Rewrite Compensation)
 * eg:
 *  query   : select * from t where a > 10
 *  mv      : select * from t where a > 20
 *
 * Because MV = (Query) where a > 20
 * Query Based MV Rewrite Compensation: a > 20
 * Query - MV = not (Query Based MV Rewrite Compensation) and Query's predicates
 *            = not (a > 20) and a > 10
 *            = (a <= 20 or a is null) and a > 10
 *            = 10 < a <= 20
 *
 * Query = MV + (Query - MV)
 *       = MVBasedRewrite + QueryBasedRewrite(not mv to query compensation)
 *       = (select * from t where a > 10) union all (select * from t where 10 < a <= 20)
 *       = MV + (select * from t where 10 < a <= 20)
 *
 * <p>Pull Predicate V1(materialized_view_union_rewrite_mode=1) </p>
 * Default mode needs query can be used for rewrite for mv's define query which is not always satisfied.
 * But we can pull up some predicates to increase the ability of union rewrite.
 *
 * eg:
 * Original Query   : select dt, k1, k2 from tblA where dt >= '2023-11-01' and k2 > 1
 *  MV              : select dt, k2 from tblA where dt = '2023-11-01'
 *
 * At default mode, query cannot be rewritten by mv's define query, but if we pull up some predicates to Query' and then
 * it will be rewritten for mv.
 *
 *  Pulled Query    : select dt, k1, k2 from tblA where dt >= '2023-11-01
 *  MV              : select dt, k2 from tblA where dt = '2023-11-01'
 *  Original Query  : select * from <Pulled Query> where k2 > 1
 *  Rewritten Query : select * from (select dt, k2 from MV union all select dt, k2 from tblA where dt > '2023-11-01') t where
 *  k2 > 1
 *
 * So mv's query can rewrite the Original Query, only need add extra predicates after rewrite.
 *
 * <p>Pull Predicate V2(materialized_view_union_rewrite_mode=2) </p>
 * Pull Predicate V1 only pull predicates that are not contained in MV's define query, but v2 can pull up all predicates
 * eg:
 * Original Query   : select dt, k2 from tblA where date_trunc('day', dt) >= '2023-11-01'
 * MV               : select dt, k2 from tblA where dt = '2023-11-01'
 * Pulled Query     : select dt, k2 from tblA
 * Original Query   : select * from <Pulled Query> where date_trunc('day', dt) >= '2023-11-01'
 * Rewritten Query  : select * from (select dt, k2 from MV union all select dt, k2 from tblA where dt > '2023-11-01') t where
 * date_trunc('day', dt) >= '2023-11-01'
 *
 *<p> Transparent Union Rewrite </p>
 * If predicates are partition predicates(eg, query refreshed partitions and to-refresh partitions), we can use treat mv as a
 * transparent table (freshness is always satisfied) and rewrite by union all.
 *
 * eg:
 * MV   : select * from t, but only refreshed partitions (dt = '2023-11-01')
 * Query: select col1, col2 from t where dt in ('2023-11-01', '2023-11-02')
 *
 * In this case we cannot use other modes to rewrite, since a query cannot be rewritten by mv's define query(query only selects
 * partial columns and mv's partial refreshed).
 *
 * If we treat mv as completely refreshed, we can rewrite it.
 * Transparent MV: select * from t(all partition refreshed) union all select * from mv's defined query(all to-refresh partitions)
 *
 * Query            : select col1, col2 from t where dt in ('2023-11-01', '2023-11-02')
 * Transparent MV   : Refreshed MV Partitions union all To-Refresh MV Partitions
 *                = select * from mv where dt = '2023-11-01' union all select * from t where dt = '2023-11-02';
 *                = select * from mv union all select * from t where dt = '2023-11-02';
 * Query Rewritten  : select col1, col2 from <Transparent MV> where dt in ('2023-11-01', '2023-11-02')
 *
 * so the final Query Rewritten:
 * Query Rewritten: select col1, col2 from <Transparent MV> where dt in ('2023-11-01', '2023-11-02')
 *  = select col1, col2 from (select * from mv where dt = '2023-11-01' union all select * from t where dt = '2023-11-02') t
 *      where dt in ('2023-11-01', '2023-11-02')
 *  = select col1, col2 from mv where dt = '2023-11-01' union all select col1, col2 from t where dt = '2023-11-02'
 *  = select col1, col2 from mv union all select col1, col2 from t where dt = '2023-11-02'
 *
 * <p>Enum</p>
 *  default: default mode, only try to union all rewrite by logical plan tree after partition compensate
 *  1: pull predicate v1, try to pull up query's filter after union when query's output matches mv's define query
 *      which will increase union rewrite's ability, and then use query to rewrite mv's define query.
 *  2: pull predicate v2, try to pull up query's filter after union as much as possible.
 *  3: transparent union all rewrite, treat mv as a transparent table (freshness is always satisfied) and rewrite by union all.
 */
public enum MVUnionRewriteMode {
    DEFAULT(0),
    PULL_PREDICATE_V1(1),
    PULL_PREDICATE_V2(2);

    private final int ordinal;

    MVUnionRewriteMode(int ordinal) {
        this.ordinal = ordinal;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public boolean isPullPredicateRewrite() {
        return this == PULL_PREDICATE_V1 || this == PULL_PREDICATE_V2;
    }

    public boolean isPullPredicateRewriteV2() {
        return this == PULL_PREDICATE_V2;
    }

    public static Map<Integer, MVUnionRewriteMode> ordinalMap = getOrdinalMap();
    public static Map<Integer, MVUnionRewriteMode> getOrdinalMap() {
        Map<Integer, MVUnionRewriteMode> ordinalMap = new HashMap<>();
        for (MVUnionRewriteMode mode : MVUnionRewriteMode.values()) {
            ordinalMap.put(mode.ordinal, mode);
        }
        return ordinalMap;
    }

    public static MVUnionRewriteMode getInstance(int ordinal) {
        if (ordinalMap.containsKey(ordinal)) {
            return ordinalMap.get(ordinal);
        }
        return DEFAULT;
    }
}
