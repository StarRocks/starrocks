// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.epack.thrift.TFailoverGroupHandshakeRequest;
import com.starrocks.epack.thrift.TFailoverGroupHandshakeResponse;
import com.starrocks.leader.MetaHelper;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PrimaryHelper {
    private static final Logger LOG = LogManager.getLogger(PrimaryHelper.class);
    private static final int PUT_IMAGE_TIMEOUT_MS = 3600000;

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
                NetworkAddress leaderAddress = new NetworkAddress(splitStrings[1], Integer.parseInt(splitStrings[2]));
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

    public static void pushImageTo(String httpHost, int httpPort, long imageVersion, String imageSubDir)
            throws IOException {
        String url = "http://" + httpHost + ":" + httpPort + "/put?version=" + imageVersion
                + "&port=" + Config.http_port + "&subdir=" + imageSubDir + "&is_failover_image=true";
        MetaHelper.getHttpOutput(url, PUT_IMAGE_TIMEOUT_MS, new NullOutputStream());
    }

    public static TFailoverGroupHandshakeResponse sendHandshakeTo(NetworkAddress address,
            TFailoverGroupHandshakeRequest request) {
        TNetworkAddress thriftAddress = address.toThrift();
        try {
            TFailoverGroupHandshakeResponse response = FrontendServiceProxy.call(thriftAddress,
                    Config.thrift_rpc_timeout_ms,
                    Config.thrift_rpc_retry_times,
                    client -> client.failoverGroupHandshake(request));
            if (response.getStatus().getStatus_code() == TStatusCode.OK) {
                return response;
            }
            LOG.warn("Send handshake to {} returns failed: {}", address, response.getStatus());
        } catch (Exception e) {
            LOG.warn("Send handshake to {} failed ", address, e);
        }
        return null;
    }
}
