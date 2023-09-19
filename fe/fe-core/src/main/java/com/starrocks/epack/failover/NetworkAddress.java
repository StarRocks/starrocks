// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Pair;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TNetworkAddress;

import java.util.Objects;

public class NetworkAddress {
    @SerializedName(value = "host")
    private String host;

    @SerializedName(value = "port")
    private int port;

    public NetworkAddress(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public TNetworkAddress toThrift() {
        return new TNetworkAddress(host, port);
    }

    public static NetworkAddress fromThrift(TNetworkAddress thriftAddress) {
        return new NetworkAddress(thriftAddress.getHostname(), thriftAddress.getPort());
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        NetworkAddress other = (NetworkAddress) obj;
        if (!Objects.equals(host, other.host)) {
            return false;
        }
        if (port != other.port) {
            return false;
        }
        return true;
    }

    public static NetworkAddress getLocalLeaderAddress() {
        Pair<String, Integer> leaderIpAndRpcPort = GlobalStateMgr.getServingState().getLeaderIpAndRpcPort();
        NetworkAddress leaderAddress = new NetworkAddress(leaderIpAndRpcPort.first, leaderIpAndRpcPort.second);
        return leaderAddress;
    }
}
