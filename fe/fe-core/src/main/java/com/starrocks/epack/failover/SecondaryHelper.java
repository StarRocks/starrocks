// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.epack.thrift.TFailoverGroupRequestMetaRequest;
import com.starrocks.epack.thrift.TFailoverGroupRequestMetaResponse;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SecondaryHelper {
    private static final Logger LOG = LogManager.getLogger(SecondaryHelper.class);

    public static FailoverGroupMember initSecondaryMembers(String primaryMemberString,
            Map<String, FailoverGroupMember> members)
            throws DdlException {
        FailoverGroupMember primary = null;
        String[] splitStrings = primaryMemberString.split(":");
        if (splitStrings.length != 2) {
            ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, primaryMemberString);
        }
        NetworkAddress leaderAddress = new NetworkAddress(splitStrings[0], Integer.parseInt(splitStrings[1]));
        Set<NetworkAddress> addresses = new HashSet<>();
        addresses.add(leaderAddress);
        FailoverGroupMember member = new FailoverGroupMember();
        member.setName("");
        member.setAddresses(addresses);
        member.setLeader(leaderAddress);
        member.setRole(FailoverGroupRole.PRIMARY);

        members.put(member.getName(), member);
        primary = member;
        return primary;
    }

    public static TFailoverGroupRequestMetaResponse sendRequestMetaTo(NetworkAddress address,
            TFailoverGroupRequestMetaRequest request) {
        TNetworkAddress thriftAddress = address.toThrift();
        try {
            TFailoverGroupRequestMetaResponse response = FrontendServiceProxy.call(thriftAddress,
                    Config.thrift_rpc_timeout_ms * 100,
                    Config.thrift_rpc_retry_times,
                    client -> client.failoverGroupRequestMeta(request));
            if (response.getStatus().getStatus_code() == TStatusCode.OK) {
                return response;
            }
            LOG.warn("Send request meta to {} returns failed: {}", address, response.getStatus());
        } catch (Exception e) {
            LOG.warn("Send request meta to {} failed ", address, e);
        }
        return null;
    }
}
