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

import com.starrocks.catalog.FsBroker;
import com.starrocks.common.Config;
import com.starrocks.thrift.TDownloadReq;
import com.starrocks.thrift.THdfsProperties;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TResourceInfo;
import com.starrocks.thrift.TTaskType;

import java.util.Map;

public class DownloadTask extends AgentTask {

    private long jobId;
    private Map<String, String> srcToDestPath;
    private FsBroker brokerAddr;
    private Map<String, String> brokerProperties;
    private THdfsProperties hdfsProperties;

    public DownloadTask(TResourceInfo resourceInfo, long backendId, long signature, long jobId, long dbId,
                        Map<String, String> srcToDestPath, FsBroker brokerAddr, Map<String, String> brokerProperties) {
        super(resourceInfo, backendId, TTaskType.DOWNLOAD, dbId, -1, -1, -1, -1, signature);
        this.jobId = jobId;
        this.srcToDestPath = srcToDestPath;
        this.brokerAddr = brokerAddr;
        this.brokerProperties = brokerProperties;
    }

    public DownloadTask(TResourceInfo resourceInfo, long backendId, long signature, long jobId, long dbId,
                        Map<String, String> srcToDestPath, FsBroker brokerAddr, Map<String, String> brokerProperties,
                        THdfsProperties hdfsProperties) {
        super(resourceInfo, backendId, TTaskType.DOWNLOAD, dbId, -1, -1, -1, -1, signature);
        this.jobId = jobId;
        this.srcToDestPath = srcToDestPath;
        this.brokerAddr = brokerAddr;
        this.brokerProperties = brokerProperties;
        this.hdfsProperties = hdfsProperties;
    }

    public long getJobId() {
        return jobId;
    }

    public Map<String, String> getSrcToDestPath() {
        return srcToDestPath;
    }

    public FsBroker getBrokerAddr() {
        return brokerAddr;
    }

    public Map<String, String> getBrokerProperties() {
        return brokerProperties;
    }

    public TDownloadReq toThrift() {
        TNetworkAddress address;
        if (brokerAddr != null) {
            address = new TNetworkAddress(brokerAddr.ip, brokerAddr.port);
        } else {
            address = new TNetworkAddress("", 0);
        }
        TDownloadReq req = new TDownloadReq(jobId, srcToDestPath, address);
        if (brokerAddr != null) {
            req.setUse_broker(true);
            req.setBroker_prop(brokerProperties);
        } else {
            req.setUse_broker(false);
            req.setHdfs_read_buffer_size_kb(Config.hdfs_read_buffer_size_kb);
            req.setHdfs_properties(hdfsProperties);
        }
        return req;
    }
}
