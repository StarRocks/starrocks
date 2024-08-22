// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.NetUtils;
import com.starrocks.epack.thrift.TFailoverGroupRequestMetaRequest;
import com.starrocks.epack.thrift.TFailoverGroupRequestMetaResponse;
import com.starrocks.leader.MetaHelper;
import com.starrocks.persist.MetaCleaner;
import com.starrocks.persist.Storage;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
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
        int port = 0;
        try {
            port = Integer.parseInt(splitStrings[1]);
        } catch (Exception e) {
            ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, primaryMemberString);
        }
        NetworkAddress leaderAddress = new NetworkAddress(splitStrings[0], port);
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
            TFailoverGroupRequestMetaResponse response = ThriftRPCRequestExecutor.call(
                    ThriftConnectionPool.frontendPool,
                    thriftAddress,
                    client -> client.failoverGroupRequestMeta(request));

            if (response.getStatus().getStatus_code() == TStatusCode.OK) {
                return response;
            }
            if (response.getStatus().getStatus_code() != TStatusCode.REMOTE_FILE_NOT_FOUND) {
                LOG.warn("Send request meta to {} returns failed: {}", address, response.getStatus());
            }
        } catch (Exception e) {
            LOG.warn("Send request meta to {} failed ", address, e);
        }
        return null;
    }

    public static boolean pullImage(String token, String httpHost, int httpPort, long imageVersion,
            String imageSubDir) {
        String url = "http://" + NetUtils.getHostPortInAccessibleFormat(httpHost, httpPort) +
                "/image?version=" + imageVersion + "&token=" + token;
        String filename = Storage.IMAGE + "." + imageVersion;
        String realDir = GlobalStateMgr.getCurrentState().getImageDir() + imageSubDir;
        File dir = new File(realDir);
        try {
            OutputStream out = MetaHelper.getOutputStream(filename, dir);
            MetaHelper.getRemoteFile(url, Config.failover_group_pull_image_timeout_sec * 1000, out);
            MetaHelper.complete(filename, dir);
        } catch (FileNotFoundException e) {
            LOG.warn("File not found. dir: {}, file: {}", realDir, filename, e);
            return false;
        } catch (IOException e) {
            LOG.warn("Failed to get remote file. url: {}", url, e);
            return false;
        }

        // Delete old image files
        MetaCleaner cleaner = new MetaCleaner(realDir);
        try {
            cleaner.clean();
        } catch (IOException e) {
            LOG.warn("Failed to delete old image file. image dir: {}", realDir, e);
        }
        return true;
    }
}
