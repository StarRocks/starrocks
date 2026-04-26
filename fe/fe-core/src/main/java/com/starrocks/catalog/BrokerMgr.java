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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/BrokerMgr.java

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

package com.starrocks.catalog;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.NetUtils;
import com.starrocks.persist.ModifyBrokerInfo;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Broker manager
 */
public class BrokerMgr implements GsonPostProcessable {
    // { BrokerName -> { list of FsBroker }
    @SerializedName(value = "bm")
    private final Map<String, List<FsBroker>> brokerListMap = Maps.newHashMap();

    // we need IP to find the co-location broker.
    // { BrokerName -> { IP -> [FsBroker] } }
    private final Map<String, ArrayListMultimap<String, FsBroker>> brokersMap = Maps.newHashMap();
    private final ReentrantLock lock = new ReentrantLock();

    public BrokerMgr() {
    }

    public Map<String, List<FsBroker>> getBrokerListMap() {
        return brokerListMap;
    }

    public List<FsBroker> getAllBrokers() {
        List<FsBroker> brokers = Lists.newArrayList();
        lock.lock();
        try {
            for (List<FsBroker> list : brokerListMap.values()) {
                brokers.addAll(list);
            }
        } finally {
            lock.unlock();
        }
        return brokers;
    }

    public boolean containsBroker(String brokerName) {
        lock.lock();
        try {
            return brokersMap.containsKey(brokerName);
        } finally {
            lock.unlock();
        }
    }

    public FsBroker getAnyBroker(String brokerName) {
        lock.lock();
        try {
            List<FsBroker> brokerList = brokerListMap.get(brokerName);
            if (brokerList == null || brokerList.isEmpty()) {
                return null;
            }

            Collections.shuffle(brokerList);
            for (FsBroker fsBroker : brokerList) {
                if (fsBroker.isAlive) {
                    return fsBroker;
                }
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    public FsBroker getBroker(String brokerName, String host) {
        lock.lock();
        try {
            ArrayListMultimap<String, FsBroker> brokerAddsMap = brokersMap.get(brokerName);
            if (brokerAddsMap == null || brokerAddsMap.size() == 0) {
                throw new SemanticException("Unknown broker name(" + brokerName + ")");
            }
            List<FsBroker> brokers = brokerAddsMap.get(host);
            for (FsBroker fsBroker : brokers) {
                if (fsBroker.isAlive) {
                    return fsBroker;
                }
            }

            // not find, get an arbitrary one
            brokers = brokerListMap.get(brokerName);
            Collections.shuffle(brokers);
            for (FsBroker fsBroker : brokers) {
                if (fsBroker.isAlive) {
                    return fsBroker;
                }
            }

            throw new SemanticException("failed to find alive broker: " + brokerName);
        } finally {
            lock.unlock();
        }
    }

    // find broker which is exactly matching name, host and port. return null if not found
    public FsBroker getBroker(String name, String host, int port) {
        lock.lock();
        try {
            ArrayListMultimap<String, FsBroker> brokerAddsMap = brokersMap.get(name);
            if (brokerAddsMap == null || brokerAddsMap.size() == 0) {
                return null;
            }

            List<FsBroker> addressList = brokerAddsMap.get(host);
            if (addressList.isEmpty()) {
                return null;
            }

            for (FsBroker fsBroker : addressList) {
                if (fsBroker.port == port) {
                    return fsBroker;
                }
            }
            return null;

        } finally {
            lock.unlock();
        }
    }

    public void addBrokers(String name, Collection<Pair<String, Integer>> addresses) throws DdlException {
        lock.lock();
        try {
            ArrayListMultimap<String, FsBroker> brokerAddrsMap = brokersMap.get(name);
            if (brokerAddrsMap == null) {
                brokerAddrsMap = ArrayListMultimap.create();
            }

            List<FsBroker> addedBrokerAddress = Lists.newArrayList();
            for (Pair<String, Integer> pair : addresses) {
                List<FsBroker> addressList = brokerAddrsMap.get(pair.first);
                for (FsBroker addr : addressList) {
                    if (addr.port == pair.second) {
                        throw new DdlException("Broker(" + NetUtils.getHostPortInAccessibleFormat(pair.first, pair.second)
                                + ") has already in brokers.");
                    }
                }
                addedBrokerAddress.add(new FsBroker(pair.first, pair.second));
            }
            GlobalStateMgr.getCurrentState().getEditLog().logAddBroker(
                new ModifyBrokerInfo(name, addedBrokerAddress), wal -> applyAddBrokers((ModifyBrokerInfo) wal));
        } finally {
            lock.unlock();
        }
    }

    private void applyAddBrokers(ModifyBrokerInfo info) {
        String name = info.getName();
        ArrayListMultimap<String, FsBroker> brokerAddrsMap = brokersMap.get(name);
        if (brokerAddrsMap == null) {
            brokerAddrsMap = ArrayListMultimap.create();
            brokersMap.put(name, brokerAddrsMap);
        }
        
        for (FsBroker address : info.getAddresses()) {
            brokerAddrsMap.put(address.ip, address);
        }

        brokerListMap.put(name, Lists.newArrayList(brokerAddrsMap.values()));
    }

    public void replayAddBrokers(ModifyBrokerInfo info) {
        lock.lock();
        try {
            applyAddBrokers(info);
        } finally {
            lock.unlock();
        }
    }

    public void dropBrokers(String name, Collection<Pair<String, Integer>> addresses) throws DdlException {
        lock.lock();
        try {
            ArrayListMultimap<String, FsBroker> brokerAddrsMap = brokersMap.get(name);
            if (brokerAddrsMap == null) {
                throw new DdlException("Unknown broker name(" + name + ")");
            }

            List<FsBroker> dropedAddressList = Lists.newArrayList();
            for (Pair<String, Integer> pair : addresses) {
                List<FsBroker> addressList = brokerAddrsMap.get(pair.first);
                boolean found = false;
                for (FsBroker addr : addressList) {
                    if (addr.port == pair.second) {
                        dropedAddressList.add(addr);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new DdlException("Broker(" + NetUtils.getHostPortInAccessibleFormat(pair.first, pair.second) +
                            ") has not in brokers.");
                }
            }
            GlobalStateMgr.getCurrentState().getEditLog().logDropBroker(
                new ModifyBrokerInfo(name, dropedAddressList), wal -> applyDropBrokers((ModifyBrokerInfo) wal));
        } finally {
            lock.unlock();
        }
    }

    private void applyDropBrokers(ModifyBrokerInfo info) {
        String name = info.getName();
        ArrayListMultimap<String, FsBroker> brokerAddrsMap = brokersMap.get(name);
        for (FsBroker address : info.getAddresses()) {
            brokerAddrsMap.remove(address.ip, address);
        }
        brokerListMap.put(name, Lists.newArrayList(brokerAddrsMap.values()));
    }

    public void replayDropBrokers(ModifyBrokerInfo info) {
        lock.lock();
        try {
            applyDropBrokers(info);
        } finally {
            lock.unlock();
        }
    }

    public void dropAllBroker(String name) throws DdlException {
        lock.lock();
        try {
            if (!brokersMap.containsKey(name)) {
                throw new DdlException("Unknown broker name(" + name + ")");
            }
            GlobalStateMgr.getCurrentState().getEditLog().logDropAllBroker(name, wal -> applyDropAllBroker(name));
        } finally {
            lock.unlock();
        }
    }

    private void applyDropAllBroker(String name) {
        brokersMap.remove(name);
        brokerListMap.remove(name);
    }

    public void replayDropAllBroker(String name) {
        lock.lock();
        try {
            applyDropAllBroker(name);
        } finally {
            lock.unlock();
        }
    }

    public Map<String, ArrayListMultimap<String, FsBroker>> getBrokersMap() {
        lock.lock();
        try {
            return new HashMap<>(brokersMap);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        for (Map.Entry<String, List<FsBroker>> brokers : brokerListMap.entrySet()) {
            String name = brokers.getKey();
            ArrayListMultimap<String, FsBroker> brokerAddrsMap = brokersMap.get(name);
            if (brokerAddrsMap == null) {
                brokerAddrsMap = ArrayListMultimap.create();
                brokersMap.put(name, brokerAddrsMap);
            }
            for (FsBroker address : brokers.getValue()) {
                brokerAddrsMap.put(address.ip, address);
            }
        }
    }
}

