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

package com.starrocks.qe;

import com.starrocks.common.UserException;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.thrift.TBatchReportExecStatusParams;
import com.starrocks.thrift.TBatchReportExecStatusResult;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TReportExecStatusResult;
import com.starrocks.thrift.TUniqueId;

import java.util.List;
import java.util.Map;

public interface QeProcessor {

    TReportExecStatusResult reportExecStatus(TReportExecStatusParams params, TNetworkAddress beAddr);

    TBatchReportExecStatusResult batchReportExecStatus(TBatchReportExecStatusParams params, TNetworkAddress beAddr);

    void registerQuery(TUniqueId queryId, Coordinator coord) throws UserException;

    void registerQuery(TUniqueId queryId, QeProcessorImpl.QueryInfo info) throws UserException;

    void unregisterQuery(TUniqueId queryId);

    Map<String, QueryStatisticsItem> getQueryStatistics();

    Coordinator getCoordinator(TUniqueId queryId);

    List<Coordinator> getCoordinators();
}
