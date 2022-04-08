// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.system.SystemInfoService;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import mockit.Mock;
import mockit.MockUp;

public class UpdateBackendAddressClauseTest {
    private static Analyzer analyzer;

    @BeforeClass
    public static void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        Config.enable_fqdn = true;
    }

    @Test
    public void testGetOriginalHost() throws AnalysisException {
        new MockUp<SystemInfoService>() {
            @Mock
            public Pair<String, Integer> validateHostAndPort(String hostPort) throws AnalysisException {
                return new Pair<String, Integer>("originalHost", 1000);
            }
        };
        UpdateBackendAddressClause clause = new UpdateBackendAddressClause("originalHost:1000", "testHost:1000");
        clause.analyze(analyzer);
        Assert.assertEquals(new Pair<String, Integer>("originalHost", 1000), clause.getDiscardedHostPort());
    }

    @Test
    public void testGetNewHost() throws AnalysisException {
        new MockUp<SystemInfoService>() {
            @Mock
            public Pair<String, Integer> validateHostAndPort(String hostPort) throws AnalysisException {
                return new Pair<String, Integer>("newHost", 1000);
            }
        };
        UpdateBackendAddressClause clause = new UpdateBackendAddressClause("testHost:1000", "newHost:1000");
        clause.analyze(analyzer);
        Assert.assertEquals(new Pair<String, Integer>("newHost", 1000), clause.getNewlyEffectiveHostPort());
    }

    @Test
    public void testNormal() throws AnalysisException {
        UpdateBackendAddressClause clause = new UpdateBackendAddressClause(null);
        Assert.assertTrue(clause.getHostPortPairs().size() == 0);
    }
}
