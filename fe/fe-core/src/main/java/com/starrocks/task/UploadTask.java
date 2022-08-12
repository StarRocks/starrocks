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
import com.starrocks.thrift.THdfsProperties;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TResourceInfo;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TUploadReq;

import java.util.Map;

public class UploadTask extends AgentTask {

    private long jobId;

    private Map<String, String> srcToDestPath;
    private FsBroker broker;
    private Map<String, String> brokerProperties;
    private THdfsProperties hdfsProperties;

    public UploadTask(TResourceInfo resourceInfo, long backendId, long signature, long jobId, Long dbId,
                      Map<String, String> srcToDestPath, FsBroker broker, Map<String, String> brokerProperties) {
        super(resourceInfo, backendId, TTaskType.UPLOAD, dbId, -1, -1, -1, -1, signature);
        this.jobId = jobId;
        this.srcToDestPath = srcToDestPath;
        this.broker = broker;
        this.brokerProperties = brokerProperties;
    }

    public UploadTask(TResourceInfo resourceInfo, long backendId, long signature, long jobId, Long dbId,
                      Map<String, String> srcToDestPath, FsBroker broker, Map<String, String> brokerProperties, 
                      THdfsProperties hdfsProperties) {
        super(resourceInfo, backendId, TTaskType.UPLOAD, dbId, -1, -1, -1, -1, signature);
        this.jobId = jobId;
        this.srcToDestPath = srcToDestPath;
        this.broker = broker;
        this.brokerProperties = brokerProperties;
        this.hdfsProperties = hdfsProperties;
    }

    public long getJobId() {
        return jobId;
    }

    public Map<String, String> getSrcToDestPath() {
        return srcToDestPath;
    }

    public FsBroker getBrokerAddress() {
        return broker;
    }

    public Map<String, String> getBrokerProperties() {
        return brokerProperties;
    }

    public TUploadReq toThrift() {
        TNetworkAddress address;
        if (broker != null) {
            address = new TNetworkAddress(broker.ip, broker.port);
        } else {
            address = new TNetworkAddress("", 0);
        }
        TUploadReq request = new TUploadReq(jobId, srcToDestPath, address);
        if (broker != null) {
            request.setBroker_prop(brokerProperties);
            request.setUse_broker(true);
        } else {
            request.setUse_broker(false);
            request.setHdfs_write_buffer_size_kb(Config.hdfs_write_buffer_size_kb);
            request.setHdfs_properties(hdfsProperties);
        }
        return request;
    }
}
