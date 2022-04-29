// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.alter;

import java.util.ArrayList;
import java.util.List;

import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.analysis.AlterClause;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.ModifyBackendAddressClause;
import com.starrocks.analysis.ModifyFrontendAddressClause;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.UtFrameUtils;

import org.junit.Before;
import org.junit.Test;

import mockit.Mock;
import mockit.MockUp;

public class SystemHandlerTest {
    
    SystemHandler systemHandler;

    private static Analyzer analyzer;

    @Before
    public void setUp() {
        systemHandler = new SystemHandler();
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        UtFrameUtils.createMinStarRocksCluster();
        new MockUp<SystemInfoService>() {
            @Mock
            public Pair<String, Integer> validateHostAndPort(String hostPort) throws AnalysisException {
                return new Pair<String, Integer>("test", 1000);
            }
        };
    }

    @Test(expected = DdlException.class)
    public void testModifyBackendAddressLogic() throws UserException {
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause("test:1000", "sandbox:1000");
        clause.analyze(analyzer);
        List<AlterClause> clauses = new ArrayList<>();
        clauses.add(clause);
        systemHandler.process(clauses, null, null, null);
    }

    @Test(expected = DdlException.class)
    public void testModifyFrontendAddressLogic() throws UserException {
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("test:1000", "sandbox:1000");
        clause.analyze(analyzer);
        List<AlterClause> clauses = new ArrayList<>();
        clauses.add(clause);
        systemHandler.process(clauses, null, null, null);
    }
}
