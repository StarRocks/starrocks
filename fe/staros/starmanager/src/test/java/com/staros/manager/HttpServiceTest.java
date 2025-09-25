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

package com.staros.manager;

import com.staros.exception.NotImplementedStarException;
import com.staros.proto.ReplicationType;
import com.staros.proto.ShardGroupInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerInfo;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.AsciiString;
import io.netty.util.CharsetUtil;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HttpServiceTest {
    private static final Logger LOG = LogManager.getLogger(HttpServiceTest.class);
    private StarManager starManager;
    private HttpService httpService;
    ShardInfo shardInfo;
    ShardInfo shardInfo2;
    ShardGroupInfo shardGroupInfo;
    ShardGroupInfo shardGroupInfo2;
    WorkerInfo workerInfo;
    WorkerInfo workerInfo2;
    WorkerGroupDetailInfo workerGroupInfo;
    WorkerGroupDetailInfo workerGroupInfo2;
    List<ShardInfo> shardInfosList = new ArrayList<>();
    List<ShardGroupInfo> shardGroupInfosList = new ArrayList<>();
    List<WorkerInfo> workerInfosList = new ArrayList<>();
    List<WorkerGroupDetailInfo> workerGroupList = new ArrayList<>();

    @Before
    public void prepare() {
        starManager = new StarManager();
        httpService = new HttpService(starManager);

        // prepare protobuf
        shardInfo = ShardInfo.newBuilder().setServiceId("serviceId")
                .setShardId(1)
                .build();
        shardInfo2 = ShardInfo.newBuilder().setServiceId("serviceId")
                .setShardId(2)
                .build();
        shardGroupInfo = ShardGroupInfo.newBuilder().setServiceId("serviceId")
                .setGroupId(1)
                .build();
        shardGroupInfo2 = ShardGroupInfo.newBuilder().setServiceId("serviceId")
                .setGroupId(2)
                .build();
        workerInfo = WorkerInfo.newBuilder().setServiceId("serviceId")
                .setWorkerId(1)
                .build();
        workerInfo2 = WorkerInfo.newBuilder().setServiceId("serviceId")
                .setWorkerId(2)
                .build();
        workerGroupInfo = WorkerGroupDetailInfo.newBuilder().setServiceId("serviceId")
                .setGroupId(1)
                .setReplicaNumber(1)
                .setReplicationType(ReplicationType.SYNC)
                .setWarmupLevel(WarmupLevel.WARMUP_NOTHING)
                .build();
        workerGroupInfo2 = WorkerGroupDetailInfo.newBuilder().setServiceId("serviceId")
                .setGroupId(2)
                .setReplicaNumber(1)
                .setReplicationType(ReplicationType.SYNC)
                .setWarmupLevel(WarmupLevel.WARMUP_NOTHING)
                .build();

        // prepare protobuf list
        shardInfosList.add(shardInfo);
        shardInfosList.add(shardInfo2);
        shardGroupInfosList.add(shardGroupInfo);
        shardGroupInfosList.add(shardGroupInfo2);
        workerInfosList.add(workerInfo);
        workerInfosList.add(workerInfo2);
        workerGroupList.add(workerGroupInfo);
        workerGroupList.add(workerGroupInfo2);
    }

    @Test
    public void testGetContentTypeFromHeader() {
        HttpRequest httpRequest = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.GET, "httpRequest");

        httpRequest.headers().set(HttpHeaderNames.ACCEPT, "text/Plain");
        AsciiString res1 = httpService.getContentTypeFromHeader(httpRequest);
        Assert.assertEquals(res1, HttpHeaderValues.TEXT_PLAIN);

        httpRequest.headers().set(HttpHeaderNames.ACCEPT, "Application/Json");
        AsciiString res2 = httpService.getContentTypeFromHeader(httpRequest);
        Assert.assertEquals(res2, HttpHeaderValues.APPLICATION_JSON);

        httpRequest.headers().set(HttpHeaderNames.ACCEPT, "Application/Json");
        AsciiString res3 = httpService.getContentTypeFromHeader(httpRequest);
        Assert.assertEquals(res3, HttpHeaderValues.APPLICATION_JSON);
        try {
            httpRequest.headers().set(HttpHeaderNames.ACCEPT, "text/html");
            AsciiString badResult = httpService.getContentTypeFromHeader(httpRequest);
        } catch (NotImplementedStarException e) {
            Assert.assertEquals(e.getMessage(), "Unsupported Accept header value");
        }
    }

    @Test
    public void testSerializeObject() {
        { // serialize shard object
            String jsonStr = "";
            String textStr = "";
            try {
                jsonStr = httpService.serializeToJson(shardInfo);
                textStr = httpService.serializeToText(shardInfo);
            } catch (Exception e) {
                Assert.assertTrue(false);
            }
            Assert.assertEquals(jsonStr, "[{\"serviceId\":\"serviceId\",\"groupIds\":[]," +
                    "\"shardId\":\"1\",\"shardState\":\"NORMAL\",\"replicaInfo\":[],\"shardProperties\":{}," +
                    "\"expectedReplicaNum\":0,\"hashCode\":0}]");
            Assert.assertEquals(textStr, "service_id: \"serviceId\"\nshard_id: 1\n");
        }
        { // serialize shard list object
            String jsonStr = "";
            String textStr = "";
            try {
                jsonStr = httpService.serializeToJson(shardInfosList);
                textStr = httpService.serializeToText(shardInfosList);
            } catch (Exception e) {
                Assert.assertTrue(false);
            }
            Assert.assertEquals(jsonStr, "[{\"serviceId\":\"serviceId\",\"groupIds\":[]," +
                    "\"shardId\":\"1\",\"shardState\":\"NORMAL\",\"replicaInfo\":[],\"shardProperties\":{}," +
                    "\"expectedReplicaNum\":0,\"hashCode\":0},{\"serviceId\":\"serviceId\",\"groupIds\":[]," +
                    "\"shardId\":\"2\",\"shardState\":\"NORMAL\",\"replicaInfo\":[],\"shardProperties\":{}," +
                    "\"expectedReplicaNum\":0,\"hashCode\":0}]");
            Assert.assertEquals(textStr,
                    "service_id: \"serviceId\"\nshard_id: 1\nservice_id: \"serviceId\"\nshard_id: 2\n");
        }
        { // serialize shard group object
            String jsonStr = "";
            String textStr = "";
            try {
                jsonStr = httpService.serializeToJson(shardGroupInfo);
                textStr = httpService.serializeToText(shardGroupInfo);
            } catch (Exception e) {
                Assert.assertTrue(false);
            }
            Assert.assertEquals(jsonStr, "[{\"serviceId\":\"serviceId\",\"groupId\":\"1\",\"shardIds\":[]," +
                    "\"policy\":\"NONE\",\"anonymous\":false,\"metaGroupId\":\"0\",\"labels\":{},\"properties\":{}}]");
            Assert.assertEquals(textStr, "service_id: \"serviceId\"\ngroup_id: 1\n");
        }
        { // serialize shard group list object
            String jsonStr = "";
            String textStr = "";
            try {
                jsonStr = httpService.serializeToJson(shardGroupInfosList);
                textStr = httpService.serializeToText(shardGroupInfosList);
            } catch (Exception e) {
                Assert.assertTrue(false);
            }
            Assert.assertEquals(jsonStr, "[{\"serviceId\":\"serviceId\",\"groupId\":\"1\",\"shardIds\":[]," +
                    "\"policy\":\"NONE\",\"anonymous\":false,\"metaGroupId\":\"0\",\"labels\":{},\"properties\":{}}," +
                    "{\"serviceId\":\"serviceId\",\"groupId\":\"2\",\"shardIds\":[],\"policy\":\"NONE\"," +
                    "\"anonymous\":false,\"metaGroupId\":\"0\",\"labels\":{},\"properties\":{}}]");
            Assert.assertEquals(textStr,
                    "service_id: \"serviceId\"\ngroup_id: 1\nservice_id: \"serviceId\"\ngroup_id: 2\n");
        }
        { // serialize worker object
            String jsonStr = "";
            String textStr = "";
            try {
                jsonStr = httpService.serializeToJson(workerInfo);
                textStr = httpService.serializeToText(workerInfo);
            } catch (Exception e) {
                Assert.assertTrue(false);
            }
            Assert.assertEquals(jsonStr, "[{\"serviceId\":\"serviceId\",\"groupId\":\"0\",\"workerId\":\"1\"," +
                    "\"ipPort\":\"\",\"workerState\":\"OFF\",\"workerProperties\":{},\"startTime\":\"0\"," +
                    "\"tabletNum\":\"0\",\"lastDownTime\":\"0\"}]");
            Assert.assertEquals(textStr, "service_id: \"serviceId\"\nworker_id: 1\n");
        }
        { // serialize worker list object
            String jsonStr = "";
            String textStr = "";
            try {
                jsonStr = httpService.serializeToJson(workerInfosList);
                textStr = httpService.serializeToText(workerInfosList);
            } catch (Exception e) {
                Assert.assertTrue(false);
            }
            Assert.assertEquals(jsonStr, "[{\"serviceId\":\"serviceId\",\"groupId\":\"0\",\"workerId\":\"1\"," +
                    "\"ipPort\":\"\",\"workerState\":\"OFF\",\"workerProperties\":{},\"startTime\":\"0\"," +
                    "\"tabletNum\":\"0\",\"lastDownTime\":\"0\"},{\"serviceId\":\"serviceId\",\"groupId\":\"0\"," +
                    "\"workerId\":\"2\",\"ipPort\":\"\",\"workerState\":\"OFF\",\"workerProperties\":{},\"startTime\":\"0\"," +
                    "\"tabletNum\":\"0\",\"lastDownTime\":\"0\"}]");
            Assert.assertEquals(textStr,
                    "service_id: \"serviceId\"\nworker_id: 1\nservice_id: \"serviceId\"\nworker_id: 2\n");
        }
        { // serialize worker group object
            String jsonStr = "";
            String textStr = "";
            try {
                jsonStr = httpService.serializeToJson(workerGroupInfo);
                textStr = httpService.serializeToText(workerGroupInfo);
            } catch (Exception e) {
                Assert.assertTrue(false);
            }
            Assert.assertEquals(jsonStr, "[{\"serviceId\":\"serviceId\",\"groupId\":\"1\",\"owner\":\"\"," +
                    "\"state\":\"PENDING\",\"labels\":{},\"properties\":{},\"workersInfo\":[],\"replicaNumber\":1," +
                    "\"replicationType\":\"SYNC\",\"warmupLevel\":\"WARMUP_NOTHING\"}]");
            Assert.assertEquals(textStr, "service_id: \"serviceId\"\ngroup_id: 1\nreplica_number: 1\n" +
                    "replication_type: SYNC\nwarmup_level: WARMUP_NOTHING\n");
        }
        { // serialize worker group list object
            String jsonStr = "";
            String textStr = "";
            try {
                jsonStr = httpService.serializeToJson(workerGroupList);
                textStr = httpService.serializeToText(workerGroupList);
            } catch (Exception e) {
                Assert.assertTrue(false);
            }
            Assert.assertEquals(jsonStr, "[{\"serviceId\":\"serviceId\",\"groupId\":\"1\",\"owner\":\"\"," +
                    "\"state\":\"PENDING\",\"labels\":{},\"properties\":{},\"workersInfo\":[],\"replicaNumber\":1," +
                    "\"replicationType\":\"SYNC\",\"warmupLevel\":\"WARMUP_NOTHING\"}," +
                    "{\"serviceId\":\"serviceId\",\"groupId\":\"2\",\"owner\":\"\",\"state\":\"PENDING\"," +
                    "\"labels\":{},\"properties\":{},\"workersInfo\":[],\"replicaNumber\":1,\"replicationType\":\"SYNC\"" +
                    ",\"warmupLevel\":\"WARMUP_NOTHING\"}]");
            Assert.assertEquals(textStr,
                    "service_id: \"serviceId\"\ngroup_id: 1\nreplica_number: 1\nreplication_type: SYNC\n" +
                            "warmup_level: WARMUP_NOTHING\nservice_id: \"serviceId\"\ngroup_id: 2\n" +
                            "replica_number: 1\nreplication_type: SYNC\nwarmup_level: WARMUP_NOTHING\n");
        }
    }

    @Test
    public void testCreateResponse() {
        HttpRequest jsonTypeRequest = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.GET,
                "jsonTypeRequest");
        jsonTypeRequest.headers().set(HttpHeaderNames.ACCEPT, "application/json");
        HttpRequest textTypeRequest = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.GET,
                "textTypeRequest");
        textTypeRequest.headers().set(HttpHeaderNames.ACCEPT, "text/plain");
        HttpResponseStatus status = HttpResponseStatus.OK;

        { // test empty object
            FullHttpResponse jsonResponse =
                    (FullHttpResponse) httpService.createResponse(null, jsonTypeRequest, status);
            Assert.assertEquals(jsonResponse.content().toString(CharsetUtil.UTF_8), "{}");

            FullHttpResponse textResponse =
                    (FullHttpResponse) httpService.createResponse(null, textTypeRequest, status);
            Assert.assertEquals(textResponse.content().toString(CharsetUtil.UTF_8), "");
        }
        { // test normal object, use shardinfo as an example
            FullHttpResponse jsonResponse =
                    (FullHttpResponse) httpService.createResponse(shardInfo, jsonTypeRequest, status);
            Assert.assertEquals(jsonResponse.content().toString(CharsetUtil.UTF_8),
                    "[{\"serviceId\":\"serviceId\",\"groupIds\":[]," +
                            "\"shardId\":\"1\",\"shardState\":\"NORMAL\",\"replicaInfo\":[],\"shardProperties\":{}," +
                            "\"expectedReplicaNum\":0,\"hashCode\":0}]");

            FullHttpResponse textResponse =
                    (FullHttpResponse) httpService.createResponse(shardInfo, textTypeRequest, status);
            Assert.assertEquals(textResponse.content().toString(CharsetUtil.UTF_8),
                    "service_id: \"serviceId\"\nshard_id: 1\n");
        }
        { // mock exception
            new MockUp<HttpService>() {
                @Mock
                String serializeToJson(Object object) throws Exception {
                    throw new Exception("Mocked serializeToJson exception");
                }

                @Mock
                String serializeToText(Object object) throws Exception {
                    throw new Exception("Mocked serializeToText exception");
                }
            };
            FullHttpResponse httpResponse1 =
                    (FullHttpResponse) httpService.createResponse(shardInfo, jsonTypeRequest, status);
            FullHttpResponse httpResponse2 =
                    (FullHttpResponse) httpService.createResponse(shardInfo, textTypeRequest, status);

            Assert.assertEquals("Error serializing object: Mocked serializeToJson exception",
                    httpResponse1.content().toString(CharsetUtil.UTF_8));
            Assert.assertEquals("Error serializing object: Mocked serializeToText exception",
                    httpResponse2.content().toString(CharsetUtil.UTF_8));
        }
    }
}
