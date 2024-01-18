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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/ha/BDBHA.java

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

package com.starrocks.ha.bdbje;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.starrocks.ha.HAProtocol;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.CloseSafeDatabase;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BDBHA implements HAProtocol {
    private static final Logger LOG = LogManager.getLogger(BDBHA.class);

    private final BDBEnvironment environment;
    private final String nodeName;
    private static final int RETRY_TIME = 3;
    // Unstable node is a follower node that is joining the cluster but have not completed.
    // We should record this kind node and set the bdb electable group size to
    // (size_of_all_followers - size_of_unstable_nodes).
    // Because once the handshake is successful, the joined node is put into the optional group,
    // but it may take a little time for this node to replicate the historical data.
    // This node will never respond to a new data replication until the historical replication is completed,
    // and if the master cannot receive a quorum response, the write operation will fail.
    private final Set<String> unstableNodes = new HashSet<>();

    public BDBHA(BDBEnvironment env, String nodeName) {
        this.environment = env;
        this.nodeName = nodeName;
    }

    @Override
    public boolean fencing() {
        CloseSafeDatabase epochDb = environment.getEpochDB();

        for (int i = 0; i < RETRY_TIME; i++) {
            try {
                long myEpoch = getLatestEpoch(epochDb.getDb()) + 1;
                LOG.info("start fencing, epoch number is {}", myEpoch);
                Long key = myEpoch;
                DatabaseEntry theKey = new DatabaseEntry();
                TupleBinding<Long> idBinding = TupleBinding.getPrimitiveBinding(Long.class);
                idBinding.objectToEntry(key, theKey);
                DatabaseEntry theData = new DatabaseEntry(new byte[1]);
                OperationStatus status = epochDb.putNoOverwrite(null, theKey, theData);
                if (status == OperationStatus.SUCCESS) {
                    GlobalStateMgr.getCurrentState().setEpoch(myEpoch);
                    return true;
                } else if (status == OperationStatus.KEYEXIST) {
                    return false;
                } else {
                    throw new Exception(status.toString());
                }
            } catch (Exception e) {
                LOG.error("fencing failed. tried {} times", i, e);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e1) {
                    LOG.warn(e1);
                }
            }
        }
        return false;
    }

    private long getLatestEpoch(Database epochDB) {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        OperationResult result = epochDB.get(null, key, data, Get.LAST,
                new ReadOptions().setLockMode(LockMode.READ_COMMITTED));
        if (result == null) {
            return 0L;
        } else {
            TupleBinding<Long> binding = TupleBinding.getPrimitiveBinding(Long.class);
            long latestEpoch = binding.entryToObject(key);
            Preconditions.checkState(latestEpoch >= epochDB.count(), "latestEpoch must >= epochDB.count()");
            return latestEpoch;
        }
    }

    @Override
    public InetSocketAddress getLeader() {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        String leaderName = replicationGroupAdmin.getMasterNodeName();
        ReplicationGroup rg = replicationGroupAdmin.getGroup();
        ReplicationNode rn = rg.getMember(leaderName);
        return rn.getSocketAddress();
    }

    @Override
    public String getLeaderNodeName() {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        return replicationGroupAdmin.getMasterNodeName();
    }

    @Override
    public List<InetSocketAddress> getObserverNodes() {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        if (replicationGroupAdmin == null) {
            return null;
        }
        List<InetSocketAddress> ret = new ArrayList<InetSocketAddress>();
        try {
            ReplicationGroup replicationGroup = replicationGroupAdmin.getGroup();
            for (ReplicationNode replicationNode : replicationGroup.getSecondaryNodes()) {
                ret.add(replicationNode.getSocketAddress());
            }
        } catch (UnknownMasterException e) {
            LOG.warn("Catch UnknownMasterException when calling getObserverNodes.", e);
            return Lists.newArrayList();
        }
        return ret;
    }

    @Override
    public List<InetSocketAddress> getElectableNodes(boolean leaderIncluded) {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        if (replicationGroupAdmin == null) {
            return null;
        }
        List<InetSocketAddress> ret = new ArrayList<InetSocketAddress>();
        try {
            ReplicationGroup replicationGroup = replicationGroupAdmin.getGroup();
            for (ReplicationNode replicationNode : replicationGroup.getElectableNodes()) {
                if (leaderIncluded) {
                    ret.add(replicationNode.getSocketAddress());
                } else {
                    if (!replicationNode.getName().equals(replicationGroupAdmin.getMasterNodeName())) {
                        ret.add(replicationNode.getSocketAddress());
                    }
                }
            }
        } catch (UnknownMasterException e) {
            LOG.warn("Catch UnknownMasterException when calling getElectableNodes.", e);
            return Lists.newArrayList();
        }
        return ret;
    }

    @Override
    public boolean removeElectableNode(String nodeName) {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        if (replicationGroupAdmin == null) {
            return false;
        }
        try {
            replicationGroupAdmin.removeMember(nodeName);
        } catch (MemberNotFoundException e) {
            LOG.error("the deleting electable node is not found {}", nodeName, e);
            return false;
        } catch (MasterStateException e) {
            LOG.error("the deleting electable node is master {}", nodeName, e);
            return false;
        }
        return true;
    }

    public void removeNodeIfExist(String host, int port, String excludeNodeName) {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        if (replicationGroupAdmin == null) {
            return;
        }

        List<String> conflictNodes = Lists.newArrayList();
        Set<ReplicationNode> nodes = replicationGroupAdmin.getGroup().getElectableNodes();
        for (ReplicationNode node : nodes) {
            if (node.getName().equals(excludeNodeName)) {
                continue;
            }
            if (node.getHostName().equals(host) && node.getPort() == port) {
                conflictNodes.add(node.getName());
            }
        }

        for (String nodeName : conflictNodes) {
            removeElectableNode(nodeName);
        }
    }

    public synchronized void addUnstableNode(String nodeName, int currentFollowerCnt) {
        unstableNodes.add(nodeName);
        ReplicatedEnvironment replicatedEnvironment = environment.getReplicatedEnvironment();
        if (replicatedEnvironment != null) {
            replicatedEnvironment.
                    setRepMutableConfig(new ReplicationMutableConfig().
                            setElectableGroupSizeOverride(currentFollowerCnt - unstableNodes.size()));
        }
    }

    public synchronized void removeUnstableNode(String nodeName, int currentFollowerCnt) {
        unstableNodes.remove(nodeName);
        ReplicatedEnvironment replicatedEnvironment = environment.getReplicatedEnvironment();
        if (replicatedEnvironment != null) {
            if (unstableNodes.isEmpty()) {
                // Setting ElectableGroupSizeOverride to 0 means remove this config,
                // and bdb will use the normal electable group size.
                replicatedEnvironment.
                        setRepMutableConfig(new ReplicationMutableConfig().setElectableGroupSizeOverride(0));
            } else {
                replicatedEnvironment.
                        setRepMutableConfig(new ReplicationMutableConfig().
                                setElectableGroupSizeOverride(currentFollowerCnt - unstableNodes.size()));
            }
        }
    }

    public synchronized void updateFrontendHostAndPort(String nodeName, String newHostName, int newPort) {
        ReplicationGroupAdmin replicationGroupAdmin = environment.getReplicationGroupAdmin();
        replicationGroupAdmin.updateAddress(nodeName, newHostName, newPort);
    }
}
