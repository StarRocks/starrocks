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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/task/UpdateTabletMetaInfoTask.java

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

package com.starrocks.task;

import com.google.common.collect.Lists;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTabletMetaInfo;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TUpdateTabletMetaInfoReq;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

public class UpdateTabletMetaInfoTask extends AgentTask {

    private static final Logger LOG = LogManager.getLogger(UpdateTabletMetaInfoTask.class);

    // used for synchronous process
    private MarkedCountDownLatch<Long, Set<Pair<Long, Integer>>> latch;

    private Set<Pair<Long, Integer>> tableIdWithSchemaHash;
    private boolean isInMemory;
    private boolean enablePersistentIndex;
    private int primaryIndexCacheExpireSec;

    private BinlogConfig binlogConfig;

    private TTabletMetaType metaType;

    private TTabletType tabletType;

    private long txnId;

    // <tablet id, tablet schema hash, tablet in memory> or
    // <tablet id, tablet schema hash, tablet enable persistent index>
    private List<Triple<Long, Integer, Boolean>> tabletToMeta;

    // <tablet id, tablet schema hash, BinlogConfig>
    private List<Triple<Long, Integer, BinlogConfig>> tabletToBinlogCofig;

    // <tablet id, tablet schema hash, primary index cache expire sec>
    private List<Triple<Long, Integer, Integer>> tabletToPrimaryCacheExpireSec;

    public UpdateTabletMetaInfoTask(long backendId, Set<Pair<Long, Integer>> tableIdWithSchemaHash,
                                    TTabletMetaType metaType) {
        super(null, backendId, TTaskType.UPDATE_TABLET_META_INFO,
                -1L, -1L, -1L, -1L, -1L, tableIdWithSchemaHash.hashCode());
        this.tableIdWithSchemaHash = tableIdWithSchemaHash;
        this.metaType = metaType;
    }

    public UpdateTabletMetaInfoTask(long backendId,
                                    Set<Pair<Long, Integer>> tableIdWithSchemaHash,
                                    boolean metaValue,
                                    MarkedCountDownLatch<Long, Set<Pair<Long, Integer>>> latch,
                                    TTabletMetaType metaType) {
        this(backendId, tableIdWithSchemaHash, metaType);
        if (metaType == TTabletMetaType.INMEMORY) {
            this.isInMemory = metaValue;
        } else if (metaType == TTabletMetaType.ENABLE_PERSISTENT_INDEX) {
            this.enablePersistentIndex = metaValue;
        }
        this.latch = latch;
    }

    public UpdateTabletMetaInfoTask(long backendId,
                                    Set<Pair<Long, Integer>> tableIdWithSchemaHash,
                                    boolean metaValue,
                                    MarkedCountDownLatch<Long, Set<Pair<Long, Integer>>> latch,
                                    TTabletMetaType metaType, TTabletType tabletType, long txnId) {
        this(backendId, tableIdWithSchemaHash, metaValue, latch, metaType);
        this.tabletType = tabletType;
        this.txnId = txnId;
    }

    public UpdateTabletMetaInfoTask(long backendId,
                                    Set<Pair<Long, Integer>> tableIdWithSchemaHash,
                                    BinlogConfig binlogConfig,
                                    MarkedCountDownLatch<Long, Set<Pair<Long, Integer>>> latch,
                                    TTabletMetaType metaType) {
        this(backendId, tableIdWithSchemaHash, metaType);
        this.binlogConfig = binlogConfig;
        this.latch = latch;
    }

    public UpdateTabletMetaInfoTask(long backendId,
                                    Set<Pair<Long, Integer>> tableIdWithSchemaHash,
                                    int primaryIndexCacheExpireSec,
                                    MarkedCountDownLatch<Long, Set<Pair<Long, Integer>>> latch,
                                    TTabletMetaType metaType) {
        this(backendId, tableIdWithSchemaHash, metaType);
        this.primaryIndexCacheExpireSec = primaryIndexCacheExpireSec;
        this.latch = latch;
    }

    public UpdateTabletMetaInfoTask(long backendId, Object tabletToMeta, TTabletMetaType metaType) {
        super(null, backendId, TTaskType.UPDATE_TABLET_META_INFO,
                -1L, -1L, -1L, -1L, -1L, tabletToMeta.hashCode());
        this.metaType = metaType;
        if (metaType == TTabletMetaType.BINLOG_CONFIG) {
            this.tabletToBinlogCofig = (List<Triple<Long, Integer, BinlogConfig>>) tabletToMeta;
        } else if (metaType == TTabletMetaType.PRIMARY_INDEX_CACHE_EXPIRE_SEC) {
            this.tabletToPrimaryCacheExpireSec = (List<Triple<Long, Integer, Integer>>) tabletToMeta;
        } else {
            this.tabletToMeta = (List<Triple<Long, Integer, Boolean>>) tabletToMeta;
        }
    }

    public void countDownLatch(long backendId, Set<Pair<Long, Integer>> tablets) {
        if (this.latch != null) {
            if (latch.markedCountDown(backendId, tablets)) {
                LOG.debug("UpdateTabletMetaInfoTask current latch count: {}, backend: {}, tablets:{}",
                        latch.getCount(), backendId, tablets);
            }
        }
    }

    // call this always means one of tasks is failed. count down to zero to finish entire task
    public void countDownToZero(String errMsg) {
        if (this.latch != null) {
            latch.countDownToZero(new Status(TStatusCode.CANCELLED, errMsg));
            LOG.debug("UpdateTabletMetaInfoTask count down to zero. error msg: {}", errMsg);
        }
    }

    public Set<Pair<Long, Integer>> getTablets() {
        return tableIdWithSchemaHash;
    }

    public TUpdateTabletMetaInfoReq toThrift() {
        TUpdateTabletMetaInfoReq updateTabletMetaInfoReq = new TUpdateTabletMetaInfoReq();
        List<TTabletMetaInfo> metaInfos = Lists.newArrayList();
        switch (metaType) {
            case PARTITIONID: {
                int tabletEntryNum = 0;
                for (Pair<Long, Integer> pair : tableIdWithSchemaHash) {
                    // add at most 10000 tablet meta during one sync to avoid too large task
                    if (tabletEntryNum > 10000) {
                        break;
                    }
                    TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                    metaInfo.setTablet_id(pair.first);
                    metaInfo.setSchema_hash(pair.second);
                    TabletMeta tabletMeta =
                            GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletMeta(pair.first);
                    if (tabletMeta == null) {
                        LOG.warn("could not find tablet [{}] in meta ignore it", pair.second);
                        continue;
                    }
                    metaInfo.setPartition_id(tabletMeta.getPartitionId());
                    metaInfo.setMeta_type(metaType);
                    metaInfos.add(metaInfo);
                    ++tabletEntryNum;
                }
                break;
            }
            case INMEMORY: {
                if (latch != null) {
                    // for schema change
                    for (Pair<Long, Integer> pair : tableIdWithSchemaHash) {
                        TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                        metaInfo.setTablet_id(pair.first);
                        metaInfo.setSchema_hash(pair.second);
                        metaInfo.setIs_in_memory(isInMemory);
                        metaInfo.setMeta_type(metaType);
                        metaInfos.add(metaInfo);
                    }
                } else {
                    // for ReportHandler
                    for (Triple<Long, Integer, Boolean> triple : tabletToMeta) {
                        TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                        metaInfo.setTablet_id(triple.getLeft());
                        metaInfo.setSchema_hash(triple.getMiddle());
                        metaInfo.setIs_in_memory(triple.getRight());
                        metaInfo.setMeta_type(metaType);
                        metaInfos.add(metaInfo);
                    }
                }
                break;
            }
            case ENABLE_PERSISTENT_INDEX: {
                if (latch != null) {
                    // for schema change
                    for (Pair<Long, Integer> pair : tableIdWithSchemaHash) {
                        TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                        metaInfo.setTablet_id(pair.first);
                        metaInfo.setSchema_hash(pair.second);
                        metaInfo.setEnable_persistent_index(enablePersistentIndex);
                        metaInfo.setMeta_type(metaType);
                        metaInfos.add(metaInfo);
                    }
                } else {
                    // for ReportHandler
                    for (Triple<Long, Integer, Boolean> triple : tabletToMeta) {
                        TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                        metaInfo.setTablet_id(triple.getLeft());
                        metaInfo.setSchema_hash(triple.getMiddle());
                        metaInfo.setEnable_persistent_index(triple.getRight());
                        metaInfo.setMeta_type(metaType);
                        metaInfos.add(metaInfo);
                    }
                }
                break;
            }
            case BINLOG_CONFIG: {
                if (latch != null) {
                    // for schema change
                    for (Pair<Long, Integer> pair : tableIdWithSchemaHash) {
                        TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                        metaInfo.setTablet_id(pair.first);
                        metaInfo.setSchema_hash(pair.second);
                        metaInfo.setBinlog_config(binlogConfig.toTBinlogConfig());
                        metaInfo.setMeta_type(metaType);
                        metaInfos.add(metaInfo);
                    }
                } else {
                    // for ReportHandler
                    for (Triple<Long, Integer, BinlogConfig> triple : tabletToBinlogCofig) {
                        TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                        metaInfo.setTablet_id(triple.getLeft());
                        metaInfo.setSchema_hash(triple.getMiddle());
                        metaInfo.setBinlog_config(triple.getRight().toTBinlogConfig());
                        metaInfo.setMeta_type(metaType);
                        metaInfos.add(metaInfo);
                    }
                }
                break;
            }
            case PRIMARY_INDEX_CACHE_EXPIRE_SEC: {
                if (latch != null) {
                    // for schema change
                    for (Pair<Long, Integer> pair : tableIdWithSchemaHash) {
                        TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                        metaInfo.setTablet_id(pair.first);
                        metaInfo.setSchema_hash(pair.second);
                        metaInfo.setPrimary_index_cache_expire_sec(primaryIndexCacheExpireSec);
                        metaInfo.setMeta_type(metaType);
                        metaInfos.add(metaInfo);
                    }
                } else {
                    // for ReportHandler
                    for (Triple<Long, Integer, Integer> triple : tabletToPrimaryCacheExpireSec) {
                        TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                        metaInfo.setTablet_id(triple.getLeft());
                        metaInfo.setSchema_hash(triple.getMiddle());
                        metaInfo.setPrimary_index_cache_expire_sec(triple.getRight());
                        metaInfo.setMeta_type(metaType);
                        metaInfos.add(metaInfo);
                    }
                }
                break;
            }

        }
        updateTabletMetaInfoReq.setTabletMetaInfos(metaInfos);

        if (tabletType != null) {
            updateTabletMetaInfoReq.setTablet_type(tabletType);
            updateTabletMetaInfoReq.setTxn_id(txnId);
        }

        return updateTabletMetaInfoReq;
    }
}