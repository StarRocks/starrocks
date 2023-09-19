// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.collect.Sets;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.epack.thrift.TFailoverGroupHandshakeRequest;
import com.starrocks.epack.thrift.TFailoverGroupRequestMetaRequest;
import com.starrocks.leader.LeaderImpl;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class FailoverGroupTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    @Test
    public void testPrimary() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testPrimaryFailoverGroup " +
                    "CATALOGS = default_catalog " +
                    "MEMBERS = " +
                        "'az1:SELF'," +
                        "'az2:192.168.0.1:9090'" +
                    "SCHEDULE = '1h'");

        FailoverGroupMgr failoverGroupMgr = new FailoverGroupMgr();
        failoverGroupMgr.createFailoverGroup(stmt);

        FailoverGroup failoverGroup = failoverGroupMgr.getFailoverGroup("testPrimaryFailoverGroup");
        failoverGroup.run();

        LeaderImpl leaderImpl = new LeaderImpl();

        TFailoverGroupRequestMetaRequest request = new TFailoverGroupRequestMetaRequest();
        request.setFailover_group_name("testPrimaryFailoverGroup");
        FailoverGroupMember secondaryMember = new FailoverGroupMember();
        secondaryMember.setName("az2");
        secondaryMember.setRole(FailoverGroupRole.SECONDARY);
        NetworkAddress address = new NetworkAddress("192.168.0.1", 9090);
        secondaryMember.setAddresses(Sets.newHashSet(address));
        secondaryMember.setLeader(address);
        request.setSecondary_member(secondaryMember.toThrift());
        request.setLast_meta_version(0);
        request.setSecondary_http_port(8080);

        leaderImpl.failoverGroupRequestMeta(request);

        Class<?> failoverGroupClass = failoverGroup.getClass();
        Field stateField = failoverGroupClass.getDeclaredField("state");
        stateField.setAccessible(true);
        stateField.set(failoverGroup, FailoverGroupState.RUNNING);

        Field membersField = failoverGroupClass.getDeclaredField("members");
        membersField.setAccessible(true);
        ConcurrentHashMap<String, FailoverGroupMember> members = 
                (ConcurrentHashMap<String, FailoverGroupMember>) membersField.get(failoverGroup);
        members.put(secondaryMember.getName(), secondaryMember);

        leaderImpl.failoverGroupRequestMeta(request);

        failoverGroupMgr.replayDropFailoverGroup(failoverGroup.getId());
        failoverGroupMgr.replayCreateFailoverGroup(failoverGroup);
        failoverGroupMgr.replayUpdateFailoverGroup(failoverGroup);
    }

    @Test
    public void testSecondary() throws Exception {
        CreateSecondaryFailoverGroupStmt stmt = (CreateSecondaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testSecondaryFailoverGroup " +
                    "AS REPLICA OF '192.168.0.1:9090'");

        FailoverGroupMgr failoverGroupMgr = new FailoverGroupMgr();
        failoverGroupMgr.createFailoverGroup(stmt);

        FailoverGroup failoverGroup = failoverGroupMgr.getFailoverGroup("testSecondaryFailoverGroup");
        failoverGroup.run();

        TFailoverGroupHandshakeRequest request  = new TFailoverGroupHandshakeRequest();
        request.setFailover_group_name("testSecondaryFailoverGroup");
        FailoverGroupMember primaryMember = new FailoverGroupMember();
        primaryMember.setName("az1");
        primaryMember.setRole(FailoverGroupRole.PRIMARY);
        NetworkAddress address = new NetworkAddress("192.168.0.1", 9090);
        primaryMember.setAddresses(Sets.newHashSet(address));
        primaryMember.setLeader(address);
        request.setPrimary_member(primaryMember.toThrift());
        request.setFailover_group_meta(GsonUtils.GSON.toJson(failoverGroup).getBytes());

        new LeaderImpl().failoverGroupHandshake(request);

        failoverGroup.run();
    }
}
