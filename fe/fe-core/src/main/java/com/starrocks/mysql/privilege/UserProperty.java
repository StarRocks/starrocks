// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/privilege/UserProperty.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.mysql.privilege;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.AccessPrivilege;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.conf.Config;
import com.starrocks.common.exception.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.structure.Pair;
import com.starrocks.load.DppConfig;
import com.starrocks.sql.ast.SetUserPropertyVar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/*
 * UserProperty contains properties set for a user
 * This user is just qualified by cluster name, not host which it connected from.
 */
public class UserProperty implements Writable {
    private static final Logger LOG = LogManager.getLogger(UserProperty.class);

    private static final String PROP_MAX_USER_CONNECTIONS = "max_user_connections";
    private static final String PROP_RESOURCE = "resource";
    private static final String PROP_QUOTA = "quota";

    // for system user
    public static final Set<Pattern> ADVANCED_PROPERTIES = Sets.newHashSet();
    // for normal user
    public static final Set<Pattern> COMMON_PROPERTIES = Sets.newHashSet();

    private String qualifiedUser;

    private long maxConn = 100;

    /*
     *  We keep white list here to save Baidu domain name (BNS) or DNS as white list.
     *  Each frontend will periodically resolve the domain name to ip, and update the privilege table.
     *  We never persist the resolved IPs.
     */
    private WhiteList whiteList = new WhiteList();

    @Deprecated
    private byte[] password;
    @Deprecated
    private boolean isAdmin = false;
    @Deprecated
    private boolean isSuperuser = false;
    @Deprecated
    private Map<String, AccessPrivilege> dbPrivMap = Maps.newHashMap();

    static {
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_MAX_USER_CONNECTIONS + "$", Pattern.CASE_INSENSITIVE));
        ADVANCED_PROPERTIES.add(Pattern.compile("^" + PROP_RESOURCE + ".", Pattern.CASE_INSENSITIVE));

        COMMON_PROPERTIES.add(Pattern.compile("^" + PROP_QUOTA + ".", Pattern.CASE_INSENSITIVE));
    }

    public UserProperty() {
    }

    public UserProperty(String qualifiedUser) {
        this.qualifiedUser = qualifiedUser;
    }

    public String getQualifiedUser() {
        return qualifiedUser;
    }

    public long getMaxConn() {
        return maxConn;
    }

    public WhiteList getWhiteList() {
        return whiteList;
    }

    @Deprecated
    public byte[] getPassword() {
        return password;
    }

    @Deprecated
    public boolean isAdmin() {
        return isAdmin;
    }

    @Deprecated
    public boolean isSuperuser() {
        return isSuperuser;
    }

    @Deprecated
    public Map<String, AccessPrivilege> getDbPrivMap() {
        return dbPrivMap;
    }

    public void setPasswordForDomain(String domain, byte[] password, boolean errOnExist) throws DdlException {
        if (errOnExist && whiteList.containsDomain(domain)) {
            throw new DdlException("Domain " + domain + " of user " + qualifiedUser + " already exists");
        }

        if (password != null) {
            whiteList.setPassword(domain, password);
        }
    }

    public void removeDomain(String domain) {
        whiteList.removeDomain(domain);
    }

    public void update(List<Pair<String, String>> properties, boolean isReplay) throws DdlException {
        // copy
        long newMaxConn = maxConn;

        // update
        for (Pair<String, String> entry : properties) {
            String key = entry.first;
            String value = entry.second;

            String[] keyArr = key.split("\\" + SetUserPropertyVar.DOT_SEPARATOR);
            if (keyArr[0].equalsIgnoreCase(PROP_MAX_USER_CONNECTIONS)) {
                // set property "max_user_connections" = "1000"
                if (keyArr.length != 1) {
                    throw new DdlException(PROP_MAX_USER_CONNECTIONS + " format error");
                }

                try {
                    newMaxConn = Long.parseLong(value);
                } catch (NumberFormatException e) {
                    throw new DdlException(PROP_MAX_USER_CONNECTIONS + " is not number");
                }

                if (newMaxConn <= 0 || newMaxConn > 10000) {
                    throw new DdlException(PROP_MAX_USER_CONNECTIONS + " is not valid, must between 1 and 10000");
                }

                if (!isReplay && newMaxConn > Config.qe_max_connection) {
                    throw new DdlException(
                            PROP_MAX_USER_CONNECTIONS + " is not valid, must less than qe_max_connection " +
                                    Config.qe_max_connection);
                }
            } else {
                throw new DdlException("Unknown user property(" + key + ")");
            }
        }

        // set
        maxConn = newMaxConn;
    }

    public List<List<String>> fetchProperty() {
        List<List<String>> result = Lists.newArrayList();

        // max user connections
        result.add(Lists.newArrayList(PROP_MAX_USER_CONNECTIONS, String.valueOf(maxConn)));

        // get resolved ips if user has domain
        Map<String, Set<String>> resolvedIPs = whiteList.getResolvedIPs();
        List<String> ips = Lists.newArrayList();
        for (Map.Entry<String, Set<String>> entry : resolvedIPs.entrySet()) {
            ips.add(entry.getKey() + ":" + Joiner.on(",").join(entry.getValue()));
        }
        if (!ips.isEmpty()) {
            result.add(Lists.newArrayList("resolved IPs", Joiner.on(";").join(ips)));
        }

        // sort
        Collections.sort(result, new Comparator<List<String>>() {
            @Override
            public int compare(List<String> o1, List<String> o2) {
                return o1.get(0).compareTo(o2.get(0));
            }
        });

        return result;
    }

    public static UserProperty read(DataInput in) throws IOException {
        UserProperty userProperty = new UserProperty();
        userProperty.readFields(in);
        return userProperty;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, ClusterNamespace.getFullName(qualifiedUser));
        out.writeLong(maxConn);

        UserResource.write(out);

        // Be compatible with default load cluster
        out.writeBoolean(false);
        // Be compatible with clusterToDppConfig
        out.writeInt(0);

        whiteList.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        // username
        qualifiedUser = ClusterNamespace.getNameFromFullName(Text.readString(in));

        maxConn = in.readLong();

        UserResource.readIn(in);

        // load cluster
        // Be compatible with defaultLoadCluster
        if (in.readBoolean()) {
            Text.readString(in);
        }

        int clusterNum = in.readInt();
        // TODO(zc): Be compatible with dppConfig, we can assert clusterNum is 0 in version 2.5
        for (int i = 0; i < clusterNum; ++i) {
            Text.readString(in);
            DppConfig dppConfig = new DppConfig();
            dppConfig.readFields(in);
        }

        whiteList.readFields(in);
    }
}
