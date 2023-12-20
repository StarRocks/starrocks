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

package com.starrocks.http;

import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerInfo;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.rest.ListWorkerGroupsAction;
import com.starrocks.http.rest.WorkerAction;
import com.starrocks.http.rest.WorkerGroupAction;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import io.netty.handler.codec.http.HttpResponseStatus;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AllWorkersActionTest {
    @Test
    public void testAllWorkerActionParams() throws Exception {
        WorkerInfo worker = WorkerInfo.newBuilder()
                                    .setServiceId("1")
                                    .setWorkerId(10L)
                                    .putWorkerProperties("propertyA", "valueA")
                                    .build();
        WorkerGroupDetailInfo workerGroup = WorkerGroupDetailInfo.newBuilder()
                                                    .setGroupId(100L)
                                                    .addWorkersInfo(worker)
                                                    .putLabels("labelA", "valueA")
                                                    .putProperties("propertyA", "valueA")
                                                    .build();
        List<WorkerGroupDetailInfo> workerGroupsList = new ArrayList<>(1);
        workerGroupsList.add(workerGroup);
        ActionController controller = new ActionController();
        BaseRequest request = new BaseRequest(null, null, null);
        BaseResponse response = new BaseResponse();
        StarOSAgent starOSAgent = null;
        ConnectContext connectContext = new ConnectContext();
        UserIdentity userIdentity = new UserIdentity();

        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                return connectContext;
            }
            public UserIdentity getCurrentUserIdentity() {
                return userIdentity;
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getCurrentStarOSAgent() {
                if (starOSAgent == null) {
                    return new StarOSAgent();
                }
                return starOSAgent;
            }
        };
        new MockUp<StarOSAgent>() {
            @Mock
            public WorkerInfo getWorkerInfo(String str, long workerId) {
                return worker;
            }
            @Mock
            public WorkerGroupDetailInfo getWorkerGroupInfo(String service, long workerGroupId) {
                return workerGroup;
            }
            @Mock
            public List<WorkerGroupDetailInfo> listWorkerGroupInfo(String service) {
                return workerGroupsList;
            }
        };

        // test for WorkerAction
        new MockUp<WorkerAction>() {
            @Mock
            public void checkUserOwnsAdminRole(UserIdentity user) {}
            @Mock
            public void sendResult(BaseRequest request, BaseResponse response, HttpResponseStatus status) {}
            @Mock
            public void sendResultByJson(BaseRequest request, BaseResponse response, Object obj) {
                Assert.assertNotNull(obj);
                if (obj instanceof Map<?, ?>) {
                    Map<?, ?> map = (Map<?, ?>) obj;
                    Assert.assertTrue(!map.isEmpty());
                }
            }
        };
        {
            WorkerAction workerAction = new WorkerAction(controller);
            new MockUp<BaseRequest>() {
                @Mock
                public String getSingleParameter(String str) {
                    if (str.equals("service")) {
                        return "starrocks";
                    } else if (str.equals("id")) {
                        return "1";
                    }
                    return "error";
                }
            };
            workerAction.executeWithoutPassword(request, response);
        }
        {
            WorkerAction workerAction = new WorkerAction(controller);
            new MockUp<BaseRequest>() {
                @Mock
                public String getSingleParameter(String str) {
                    if (str.equals("service")) {
                        return "";
                    } else if (str.equals("id")) {
                        return "1";
                    }
                    return "error";
                }
            };
            workerAction.executeWithoutPassword(request, response);
        }
        {
            WorkerAction workerAction = new WorkerAction(controller);
            new MockUp<BaseRequest>() {
                @Mock
                public String getSingleParameter(String str) {
                    if (str.equals("service")) {
                        return "starrocks";
                    } else if (str.equals("id")) {
                        return "abcd";
                    }
                    return "error";
                }
            };
            workerAction.executeWithoutPassword(request, response);
        }

        // test for WorkerGroupAction
        new MockUp<WorkerGroupAction>() {
            @Mock
            public void checkUserOwnsAdminRole(UserIdentity user) {}
            @Mock
            public void sendResult(BaseRequest request, BaseResponse response, HttpResponseStatus status) {}
            @Mock
            public void sendResultByJson(BaseRequest request, BaseResponse response, Object obj) {
                Assert.assertNotNull(obj);
                if (obj instanceof Map<?, ?>) {
                    Map<?, ?> map = (Map<?, ?>) obj;
                    Assert.assertTrue(!map.isEmpty());
                }
            }
        };
        {
            WorkerGroupAction workerGroupAction = new WorkerGroupAction(controller);
            new MockUp<BaseRequest>() {
                @Mock
                public String getSingleParameter(String str) {
                    if (str.equals("service")) {
                        return "starrocks";
                    } else if (str.equals("id")) {
                        return "1";
                    }
                    return "error";
                }
            };
            workerGroupAction.executeWithoutPassword(request, response);
        }
        {
            WorkerGroupAction workerGroupAction = new WorkerGroupAction(controller);
            new MockUp<BaseRequest>() {
                @Mock
                public String getSingleParameter(String str) {
                    if (str.equals("service")) {
                        return "";
                    } else if (str.equals("id")) {
                        return "1";
                    }
                    return "error";
                }
            };
            workerGroupAction.executeWithoutPassword(request, response);
        }
        {
            WorkerGroupAction workerGroupAction = new WorkerGroupAction(controller);
            new MockUp<BaseRequest>() {
                @Mock
                public String getSingleParameter(String str) {
                    if (str.equals("service")) {
                        return "starrocks";
                    } else if (str.equals("id")) {
                        return "abcd";
                    }
                    return "error";
                }
            };
            workerGroupAction.executeWithoutPassword(request, response);
        }

        // test for ListWorkerGroupsAction
        new MockUp<ListWorkerGroupsAction>() {
            @Mock
            public void checkUserOwnsAdminRole(UserIdentity user) {}
            @Mock
            public void sendResult(BaseRequest request, BaseResponse response, HttpResponseStatus status) {}
            @Mock
            public void sendResultByJson(BaseRequest request, BaseResponse response, Object obj) {
                Assert.assertNotNull(obj);
                if (obj instanceof Map<?, ?>) {
                    Map<?, ?> map = (Map<?, ?>) obj;
                    Assert.assertTrue(!map.isEmpty());
                }
            }
        };
        {
            ListWorkerGroupsAction listWorkerGroupsAction = new ListWorkerGroupsAction(controller);
            new MockUp<BaseRequest>() {
                @Mock
                public String getSingleParameter(String str) {
                    if (str.equals("service")) {
                        return "starrocks";
                    } else if (str.equals("id")) {
                        return "1";
                    }
                    return "error";
                }
            };
            listWorkerGroupsAction.executeWithoutPassword(request, response);
        }
        {
            ListWorkerGroupsAction listWorkerGroupsAction = new ListWorkerGroupsAction(controller);
            new MockUp<BaseRequest>() {
                @Mock
                public String getSingleParameter(String str) {
                    if (str.equals("service")) {
                        return "";
                    } else if (str.equals("id")) {
                        return "1";
                    }
                    return "error";
                }
            };
            listWorkerGroupsAction.executeWithoutPassword(request, response);
        }
        {
            ListWorkerGroupsAction listWorkerGroupsAction = new ListWorkerGroupsAction(controller);
            new MockUp<BaseRequest>() {
                @Mock
                public String getSingleParameter(String str) {
                    if (str.equals("service")) {
                        return "starrocks";
                    } else if (str.equals("id")) {
                        return "abcd";
                    }
                    return "error";
                }
            };
            listWorkerGroupsAction.executeWithoutPassword(request, response);
        }
    }
}