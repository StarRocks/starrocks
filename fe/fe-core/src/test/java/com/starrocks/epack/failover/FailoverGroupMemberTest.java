// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.collect.Sets;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

public class FailoverGroupMemberTest {
    @Test
    public void testSerialization() throws IOException {
        FailoverGroupMember member = new FailoverGroupMember();
        member.setName("test_member");
        Set<NetworkAddress> addresses = Sets.newHashSet();
        NetworkAddress address = new NetworkAddress("192.168.0.1", 8888);
        addresses.add(address);
        member.setAddresses(addresses);
        member.setLeader(address);
        member.setRole(FailoverGroupRole.PRIMARY);

        String json = GsonUtils.GSON.toJson(member);

        FailoverGroupMember newMember = GsonUtils.GSON.fromJson(json, FailoverGroupMember.class);

        Assert.assertEquals(newMember.toString(), member.toString());
    }
}
