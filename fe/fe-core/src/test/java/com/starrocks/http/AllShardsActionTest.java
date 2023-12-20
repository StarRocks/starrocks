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

import com.staros.proto.ShardGroupInfo;
import com.staros.proto.ShardInfo;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.rest.ListShardGroupsAction;
import com.starrocks.http.rest.ShardAction;
import com.starrocks.http.rest.ShardGroupAction;
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

public class AllShardsActionTest {
    @Test
    public void testAllShardActionParams() throws Exception {
        ShardInfo shard = ShardInfo.newBuilder()
                                  .setServiceId("1")
                                  .setShardId(10L)
                                  .putShardProperties("propertyA", "valueA")
                                  .build();
        ShardGroupInfo shardGroup = ShardGroupInfo.newBuilder()
                                            .setServiceId("1")
                                            .setGroupId(11L)
                                            .putLabels("labelA", "valueA")
                                            .putProperties("propertyA", "valueA")
                                            .build();
        List<ShardGroupInfo> shardGroupsList = new ArrayList<>(1);
        shardGroupsList.add(shardGroup);
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
            public ShardInfo getShardInfo(String str, long shardId) {
                return shard;
            }
            @Mock
            public ShardGroupInfo getShardGroupInfo(String service, long shardGroupId) {
                return shardGroup;
            }
            @Mock
            public List<ShardGroupInfo> listShardGroupInfo(String service) {
                return shardGroupsList;
            }
        };

        // test for ShardAction
        new MockUp<ShardAction>() {
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
            ShardAction shardAction = new ShardAction(controller);
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
            shardAction.executeWithoutPassword(request, response);
        }
        {
            ShardAction shardAction = new ShardAction(controller);
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
            shardAction.executeWithoutPassword(request, response);
        }
        {
            ShardAction shardAction = new ShardAction(controller);
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
            shardAction.executeWithoutPassword(request, response);
        }

        // test for ShardGroupAction
        new MockUp<ShardGroupAction>() {
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
            ShardGroupAction shardGroupAction = new ShardGroupAction(controller);
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
            shardGroupAction.executeWithoutPassword(request, response);
        }
        {
            ShardGroupAction shardGroupAction = new ShardGroupAction(controller);
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
            shardGroupAction.executeWithoutPassword(request, response);
        }
        {
            ShardGroupAction shardGroupAction = new ShardGroupAction(controller);
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
            shardGroupAction.executeWithoutPassword(request, response);
        }

        // test for ListShardGroupsAction
        new MockUp<ListShardGroupsAction>() {
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
            ListShardGroupsAction listShardGroupsAction = new ListShardGroupsAction(controller);
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
            listShardGroupsAction.executeWithoutPassword(request, response);
        }
        {
            ListShardGroupsAction listShardGroupsAction = new ListShardGroupsAction(controller);
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
            listShardGroupsAction.executeWithoutPassword(request, response);
        }
        {
            ListShardGroupsAction listShardGroupsAction = new ListShardGroupsAction(controller);
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
            listShardGroupsAction.executeWithoutPassword(request, response);
        }
    }
}