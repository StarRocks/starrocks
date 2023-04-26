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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/common/util/BrokerUtilTest.java

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

package com.starrocks.common.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.GenericPool;
import com.starrocks.common.UserException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TBrokerCloseReaderRequest;
import com.starrocks.thrift.TBrokerCloseWriterRequest;
import com.starrocks.thrift.TBrokerDeletePathRequest;
import com.starrocks.thrift.TBrokerFD;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TBrokerListPathRequest;
import com.starrocks.thrift.TBrokerListResponse;
import com.starrocks.thrift.TBrokerOpenReaderRequest;
import com.starrocks.thrift.TBrokerOpenReaderResponse;
import com.starrocks.thrift.TBrokerOpenWriterRequest;
import com.starrocks.thrift.TBrokerOpenWriterResponse;
import com.starrocks.thrift.TBrokerOperationStatus;
import com.starrocks.thrift.TBrokerOperationStatusCode;
import com.starrocks.thrift.TBrokerPReadRequest;
import com.starrocks.thrift.TBrokerPWriteRequest;
import com.starrocks.thrift.TBrokerReadResponse;
import com.starrocks.thrift.TFileBrokerService;
import com.starrocks.thrift.TNetworkAddress;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BrokerUtilTest {

    @Test
    public void parseColumnsFromPath() {
        String path = "/path/to/dir/k1=v1/xxx.csv";
        try {
            List<String> columns = BrokerUtil.parseColumnsFromPath(path, Collections.singletonList("k1"));
            assertEquals(1, columns.size());
            assertEquals(Collections.singletonList("v1"), columns);
        } catch (UserException e) {
            fail();
        }

        path = "/path/to/dir/k1/xxx.csv";
        try {
            List<String> columns = BrokerUtil.parseColumnsFromPath(path, Collections.singletonList("k1"));
            fail();
        } catch (UserException ignored) {
        }

        path = "/path/to/dir/k1=v1/xxx.csv";
        try {
            List<String> columns = BrokerUtil.parseColumnsFromPath(path, Collections.singletonList("k2"));
            fail();
        } catch (UserException ignored) {
        }

        path = "/path/to/dir/k1=v2/k1=v1/xxx.csv";
        try {
            List<String> columns = BrokerUtil.parseColumnsFromPath(path, Collections.singletonList("k1"));
            assertEquals(1, columns.size());
            assertEquals(Collections.singletonList("v1"), columns);
        } catch (UserException e) {
            fail();
        }

        path = "/path/to/dir/k2=v2/k1=v1/xxx.csv";
        try {
            List<String> columns = BrokerUtil.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            assertEquals(2, columns.size());
            assertEquals(Lists.newArrayList("v1", "v2"), columns);
        } catch (UserException e) {
            fail();
        }

        path = "/path/to/dir/k2=v2/a/k1=v1/xxx.csv";
        try {
            List<String> columns = BrokerUtil.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            fail();
        } catch (UserException ignored) {
        }

        path = "/path/to/dir/k2=v2/k1=v1/xxx.csv";
        try {
            List<String> columns = BrokerUtil.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2", "k3"));
            fail();
        } catch (UserException ignored) {
        }

        path = "/path/to/dir/k2=v2//k1=v1//xxx.csv";
        try {
            List<String> columns = BrokerUtil.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            assertEquals(2, columns.size());
            assertEquals(Lists.newArrayList("v1", "v2"), columns);
        } catch (UserException e) {
            fail();
        }

        path = "/path/to/dir/k2==v2=//k1=v1//xxx.csv";
        try {
            List<String> columns = BrokerUtil.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            assertEquals(2, columns.size());
            assertEquals(Lists.newArrayList("v1", "=v2="), columns);
        } catch (UserException e) {
            fail();
        }

        path = "/path/to/dir/k2==v2=//k1=v1/";
        try {
            List<String> columns = BrokerUtil.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            fail();
        } catch (UserException ignored) {
        }

        path = "/path/to/dir/k1=2/a/xxx.csv";
        try {
            List<String> columns = BrokerUtil.parseColumnsFromPath(path, Collections.singletonList("k1"));
            fail();
        } catch (UserException ignored) {
            ignored.printStackTrace();
        }

    }

    @Test
    public void testReadFile(@Mocked TFileBrokerService.Client client, @Mocked GlobalStateMgr globalStateMgr,
                             @Injectable BrokerMgr brokerMgr)
            throws TException, UserException {
        // list response
        TBrokerListResponse listResponse = new TBrokerListResponse();
        TBrokerOperationStatus status = new TBrokerOperationStatus();
        status.statusCode = TBrokerOperationStatusCode.OK;
        listResponse.opStatus = status;
        List<TBrokerFileStatus> files = Lists.newArrayList();
        String filePath = "hdfs://127.0.0.1:10000/starrocks/jobs/1/label6/9/dpp_result.json";
        files.add(new TBrokerFileStatus(filePath, false, 10, false));
        listResponse.files = files;

        // open reader response
        TBrokerOpenReaderResponse openReaderResponse = new TBrokerOpenReaderResponse();
        openReaderResponse.opStatus = status;
        openReaderResponse.fd = new TBrokerFD(1, 2);

        // read response
        String dppResultStr = "{'normal_rows': 10, 'abnormal_rows': 0, 'failed_reason': 'etl job failed'}";
        TBrokerReadResponse readResponse = new TBrokerReadResponse();
        readResponse.opStatus = status;
        readResponse.setData(dppResultStr.getBytes(StandardCharsets.UTF_8));

        FsBroker fsBroker = new FsBroker("127.0.0.1", 99999);

        new MockUp<GenericPool<TFileBrokerService.Client>>() {
            @Mock
            public TFileBrokerService.Client borrowObject(TNetworkAddress address, int timeoutMs) throws Exception {
                return client;
            }

            @Mock
            public void returnObject(TNetworkAddress address, TFileBrokerService.Client object) {
                return;
            }

            @Mock
            public void invalidateObject(TNetworkAddress address, TFileBrokerService.Client object) {
                return;
            }
        };

        new Expectations() {
            {
                globalStateMgr.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.getBroker(anyString, anyString);
                result = fsBroker;
                client.listPath((TBrokerListPathRequest) any);
                result = listResponse;
                client.openReader((TBrokerOpenReaderRequest) any);
                result = openReaderResponse;
                client.pread((TBrokerPReadRequest) any);
                result = readResponse;
                times = 1;
                client.closeReader((TBrokerCloseReaderRequest) any);
                result = status;
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc("broker0", Maps.newHashMap());
        byte[] data = BrokerUtil.readFile(filePath, brokerDesc);
        String readStr = new String(data, StandardCharsets.UTF_8);
        Assert.assertEquals(dppResultStr, readStr);
    }

    @Test
    public void testWriteFile(@Mocked TFileBrokerService.Client client, @Mocked GlobalStateMgr globalStateMgr,
                              @Injectable BrokerMgr brokerMgr)
            throws TException, UserException {
        // open writer response
        TBrokerOpenWriterResponse openWriterResponse = new TBrokerOpenWriterResponse();
        TBrokerOperationStatus status = new TBrokerOperationStatus();
        status.statusCode = TBrokerOperationStatusCode.OK;
        openWriterResponse.opStatus = status;
        openWriterResponse.fd = new TBrokerFD(1, 2);
        FsBroker fsBroker = new FsBroker("127.0.0.1", 99999);

        new MockUp<GenericPool<TFileBrokerService.Client>>() {
            @Mock
            public TFileBrokerService.Client borrowObject(TNetworkAddress address, int timeoutMs) throws Exception {
                return client;
            }

            @Mock
            public void returnObject(TNetworkAddress address, TFileBrokerService.Client object) {
                return;
            }

            @Mock
            public void invalidateObject(TNetworkAddress address, TFileBrokerService.Client object) {
                return;
            }
        };

        new Expectations() {
            {
                globalStateMgr.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.getBroker(anyString, anyString);
                result = fsBroker;
                client.openWriter((TBrokerOpenWriterRequest) any);
                result = openWriterResponse;
                client.pwrite((TBrokerPWriteRequest) any);
                result = status;
                times = 1;
                client.closeWriter((TBrokerCloseWriterRequest) any);
                result = status;
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc("broker0", Maps.newHashMap());
        byte[] configs = "{'label': 'label0'}".getBytes(StandardCharsets.UTF_8);
        String destFilePath = "hdfs://127.0.0.1:10000/starrocks/jobs/1/label6/9/configs/jobconfig.json";
        try {
            BrokerUtil.writeFile(configs, destFilePath, brokerDesc);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testDeletePath(@Mocked TFileBrokerService.Client client, @Mocked GlobalStateMgr globalStateMgr,
                               @Injectable BrokerMgr brokerMgr) throws AnalysisException, TException {
        // delete response
        TBrokerOperationStatus status = new TBrokerOperationStatus();
        status.statusCode = TBrokerOperationStatusCode.OK;
        FsBroker fsBroker = new FsBroker("127.0.0.1", 99999);

        new MockUp<GenericPool<TFileBrokerService.Client>>() {
            @Mock
            public TFileBrokerService.Client borrowObject(TNetworkAddress address, int timeoutMs) throws Exception {
                return client;
            }

            @Mock
            public void returnObject(TNetworkAddress address, TFileBrokerService.Client object) {
                return;
            }

            @Mock
            public void invalidateObject(TNetworkAddress address, TFileBrokerService.Client object) {
                return;
            }
        };

        new Expectations() {
            {
                globalStateMgr.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.getBroker(anyString, anyString);
                result = fsBroker;
                client.deletePath((TBrokerDeletePathRequest) any);
                result = status;
                times = 1;
            }
        };

        try {
            BrokerDesc brokerDesc = new BrokerDesc("broker0", Maps.newHashMap());
            BrokerUtil.deletePath("hdfs://127.0.0.1:10000/starrocks/jobs/1/label6/9", brokerDesc);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}
