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

import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.metric.MetricRepo;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class HttpConnectProcessorTest extends DDLTestBase {

    @BeforeAll
    public static void setUpMetrics() {
        MetricRepo.init();
    }

    @Test
    public void testHandleQueryMarksRelationForPolicyRewrite() throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                "select v1 from testDb1.testTable1", ctx);

        HttpConnectContext httpContext = new HttpConnectContext();
        httpContext.setCurrentUserIdentity(UserIdentity.ROOT);
        httpContext.setQualifiedUser(UserIdentity.ROOT.getUser());
        httpContext.setCurrentCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        httpContext.setDatabase("testDb1");
        httpContext.setStatement(stmt);

        // AstTraverser.visitSelect() recurses into the FROM-clause relation rather than
        // delegating to visitRelation() for the SelectRelation itself, so the table
        // relation inside the FROM clause is what actually gets marked, not the top-level
        // SelectRelation returned by getQueryRelation().
        Relation tableRelation = ((SelectRelation) ((QueryStatement) stmt).getQueryRelation()).getRelation();

        // Before handleQuery(), the raw parsed relation is not yet marked for policy rewrite.
        Assertions.assertFalse(tableRelation.isNeedRewrittenByPolicy());

        HttpConnectProcessor processor = new HttpConnectProcessor(httpContext);
        processor.handleQuery();

        Assertions.assertTrue(tableRelation.isNeedRewrittenByPolicy());
    }
}
