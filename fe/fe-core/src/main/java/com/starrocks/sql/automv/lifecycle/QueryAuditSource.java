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

package com.starrocks.sql.automv.lifecycle;

import com.google.common.collect.ImmutableList;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.automv.qe.CustomizedQueryExecutor;
import com.starrocks.sql.automv.util.PrettyPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class QueryAuditSource {
    private static final Logger LOG = LogManager.getLogger(QueryAuditSource.class);
    private final String auditDb;
    private final String auditTbl;

    public QueryAuditSource(String auditDb, String auditTbl) {
        this.auditDb = auditDb;
        this.auditTbl = auditTbl;
    }

    // WITH audit_tbl AS (
    //     SELECT
    //       catalog,
    //       db,
    //       stmt,
    //       queryTime,
    //       scanBytes,
    //       scanRows,
    //       returnRows,
    //       cpuCostNs,
    //       memCostBytes,
    //       candidateMVs,
    //       hitMvs
    //     FROM
    //       starrocks_audit_db__.starrocks_audit_tbl__
    //     WHERE
    //       queryType IN ("query", "slow_query")
    //       AND isQuery = 1
    //       AND state = "EOF"
    //       AND coalesce(db,"") NOT IN ("", "starrocks_audit_db__")
    //   )
    // SELECT * FROM audit_tbl
    // [where_clause]
    public String cookSql(List<String> conjuncts) {

        PrettyPrinter cteBody = new PrettyPrinter();
        cteBody.add("SELECT").newLine();
        List<String> items = QueryAuditEntry.getColumns()
                .stream()
                .map(cp -> cp.getColumn().getName())
                .collect(Collectors.toList());
        cteBody.indentEnclose(() -> {
            cteBody.addItemsWithDelNl(",", items);
        });
        cteBody.newLine();
        cteBody.add("FROM ").addBacktickQuoted(auditDb).add(".").addBacktickQuoted(auditTbl).newLine();

        cteBody.add("WHERE").newLine();
        List<String> defaultConjuncts = ImmutableList.of(
                "queryType IN (\"query\", \"slow_query\")",
                "isQuery = 1",
                "state = \"EOF\"",
                String.format("coalesce(db,\"\") NOT IN (\"\", \"%s\")", auditDb)
        );
        cteBody.indentEnclose(() -> {
            cteBody.addItemsWithNlDel("AND ", defaultConjuncts);
        });

        PrettyPrinter printer = new PrettyPrinter();
        printer.add("WITH audit_tbl AS (").newLine();
        printer.addSuperStepWithIndent(cteBody);
        printer.newLine().add(")").newLine();
        printer.add("SELECT * FROM audit_tbl").newLine();
        if (!conjuncts.isEmpty()) {
            printer.add("WHERE").newLine();
            printer.indentEnclose(() -> {
                printer.addItemsWithNlDel("AND ", conjuncts);
            });
        }
        return printer.getResult();
    }

    public List<QueryAuditEntry> getQueryAuditInfoList(ConnectContext ctx, Supplier<List<String>> conjunctsBuilder) {
        CustomizedQueryExecutor executor = new CustomizedQueryExecutor();
        String sql = cookSql(conjunctsBuilder.get());
        LOG.info("[AUTOMV] SQL={}", sql);
        return executor.query(QueryAuditEntry.class, QueryAuditEntry.getColumns(), ctx, sql);
    }
}
