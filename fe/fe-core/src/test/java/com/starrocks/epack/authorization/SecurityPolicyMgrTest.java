// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authorization;

import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Type;
import com.starrocks.epack.persist.ApplyOrRevokeMaskingPolicyLog;
import com.starrocks.epack.persist.CreatePasswordPolicyLog;
import com.starrocks.epack.persist.CreatePolicyLog;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class SecurityPolicyMgrTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MetricRepo.init();
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterClass
    public static void teardown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testSaveLoadImage() throws Exception {
        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        securityPolicyMgr.replayCreatePolicy(new CreatePolicyLog(new Policy(PolicyType.ROW_ACCESS, 3L, "policy1",
                new DbUID("1111"), List.of("a"),
                List.of(Type.INT), Type.INT, "add(a, 1)", "")));

        securityPolicyMgr.registerMaskingPolicyContext(new ApplyOrRevokeMaskingPolicyLog(
                new TableUID("test", "test"), ColumnId.create("a"), new MaskingPolicyContext(4L, List.of(ColumnId.create("a")))));

        securityPolicyMgr.doCreatePasswordPolicy(new CreatePasswordPolicyLog(5L, "policy2", "test", new HashMap<>()));
        securityPolicyMgr.setGlobalPasswordPolicy(5L);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        securityPolicyMgr.save(image.getImageWriter());

        SecurityPolicyMgr securityPolicyMgr2 = new SecurityPolicyMgr();
        SRMetaBlockReader reader = image.getMetaBlockReader();
        securityPolicyMgr2.load(reader);
        reader.close();

        Assert.assertEquals("policy1", securityPolicyMgr2.getPolicyById(3L).getName());
        Assert.assertEquals(Long.valueOf(5), securityPolicyMgr2.getPasswordPolicy("policy2").getPolicyId());
        Assert.assertTrue(securityPolicyMgr2.getTableAppliedPolicyInfo(
                new TableUID("test", "test")).getMaskingPolicyApply().containsKey(ColumnId.create("a")));
    }
}
