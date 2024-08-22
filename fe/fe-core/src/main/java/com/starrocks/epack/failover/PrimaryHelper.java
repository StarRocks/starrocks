// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.epack.thrift.TFailoverGroupHandshakeRequest;
import com.starrocks.epack.thrift.TFailoverGroupHandshakeResponse;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PrimaryHelper {
    private static final Logger LOG = LogManager.getLogger(PrimaryHelper.class);

    public static FailoverGroupMember initPrimaryMembers(List<String> memberStrings,
            Map<String, FailoverGroupMember> members)
            throws DdlException {
        FailoverGroupMember primary = null;
        NetworkAddress primaryLeaderAddress = NetworkAddress.getLocalLeaderAddress();
        for (String membeString : memberStrings) {
            String[] splitStrings = membeString.split(":");
            if (splitStrings.length == 2) {
                if (splitStrings[0].isEmpty() ||
                        !(splitStrings[1].equalsIgnoreCase("self") ||
                                splitStrings[1].equalsIgnoreCase("local"))) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, membeString);
                }

                FailoverGroupMember localMember = FailoverGroupMember.getLocalMember(
                        splitStrings[0], FailoverGroupRole.PRIMARY);
                FailoverGroupMember previous = members.putIfAbsent(localMember.getName(), localMember);
                if (previous != null || primary != null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, membeString);
                }
                primary = localMember;
            } else if (splitStrings.length == 3) {
                int port = 0;
                try {
                    port = Integer.parseInt(splitStrings[2]);
                } catch (Exception e) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, membeString);
                }
                NetworkAddress leaderAddress = new NetworkAddress(splitStrings[1], port);
                Set<NetworkAddress> addresses = new HashSet<>();
                addresses.add(leaderAddress);
                FailoverGroupMember member = new FailoverGroupMember();
                member.setName(splitStrings[0]);
                member.setAddresses(addresses);
                member.setLeader(leaderAddress);
                member.setRole(FailoverGroupRole.NONE);
                FailoverGroupMember previous = members.putIfAbsent(member.getName(), member);
                if (previous != null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, membeString);
                }
                if (addresses.contains(primaryLeaderAddress)) {
                    if (primary != null) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, membeString);
                    }
                    primary = member;
                }
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, membeString);
            }
        }

        if (primary == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, "No primary member");
        }
        return primary;
    }

    public static Map<String, FailoverGroupMember> initMembers(List<String> memberStrings) throws DdlException {
        Map<String, FailoverGroupMember> members = new HashMap<>(memberStrings.size());
        for (String membeString : memberStrings) {
            String[] splitStrings = membeString.split(":");
            if (splitStrings.length == 3) {
                int port = 0;
                try {
                    port = Integer.parseInt(splitStrings[2]);
                } catch (Exception e) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, membeString);
                }
                NetworkAddress leaderAddress = new NetworkAddress(splitStrings[1], port);
                Set<NetworkAddress> addresses = new HashSet<>();
                addresses.add(leaderAddress);
                FailoverGroupMember member = new FailoverGroupMember();
                member.setName(splitStrings[0]);
                member.setAddresses(addresses);
                member.setLeader(leaderAddress);
                member.setRole(FailoverGroupRole.NONE);
                FailoverGroupMember previous = members.putIfAbsent(member.getName(), member);
                if (previous != null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, membeString);
                }
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, membeString);
            }
        }
        return members;
    }

    public static TFailoverGroupHandshakeResponse sendHandshakeTo(NetworkAddress address,
            TFailoverGroupHandshakeRequest request) {
        TNetworkAddress thriftAddress = address.toThrift();
        try {
            TFailoverGroupHandshakeResponse response = ThriftRPCRequestExecutor.call(
                    ThriftConnectionPool.frontendPool,
                    thriftAddress,
                    client -> client.failoverGroupHandshake(request));

            if (response.getStatus().getStatus_code() == TStatusCode.OK) {
                return response;
            }
        } catch (Exception e) {
            // Ignore the error
        }
        return null;
    }
}
