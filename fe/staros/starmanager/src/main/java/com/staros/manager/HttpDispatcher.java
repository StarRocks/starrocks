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

import com.staros.exception.InvalidArgumentStarException;
import com.staros.exception.NotExistStarException;
import com.staros.exception.StarException;
import com.staros.proto.ShardGroupInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerInfo;
import com.staros.util.Constant;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HttpDispatcher {
    public static final Logger LOG = LogManager.getLogger(HttpDispatcher.class);

    private StarManager starManager;
    private List<Pair<Pattern, Function<Matcher, Object>>> routeList = new ArrayList<>();
    private static final String SERVICE = "SERVICE";
    private static final String ID = "ID";

    private static final String SHARD_PATTERN =
            ".*/service/(?<" + SERVICE + ">.*?)/shard/(?<" + ID + ">[^/]*)";
    private static final String SHARDGROUP_PATTERN =
            ".*/service/(?<" + SERVICE + ">.*?)/shardgroup/(?<" + ID + ">[^/]*)";
    private static final String LIST_SHARDGROUP_PATTERN =
            ".*/service/(?<" + SERVICE + ">.*?)/listshardgroup";
    private static final String REMOVE_SHARDGROUP_REPLICAS_PATTERN =
            ".*/service/(?<" + SERVICE + ">.*?)/shardgroup/(?<" + ID + ">.*?)/removereplicas";
    private static final String WORKER_PATTERN =
            ".*/service/(?<" + SERVICE + ">.*?)/worker/(?<" + ID + ">[^/]*)";
    private static final String WORKERGROUP_PATTERN =
            ".*/service/(?<" + SERVICE + ">.*?)/workergroup/(?<" + ID + ">[^/]*)";
    private static final String LIST_WORKERGROUP_PATTERN =
            ".*/service/(?<" + SERVICE + ">.*?)/listworkergroup";

    public HttpDispatcher(StarManager manager) {
        routeList.add(Pair.of(Pattern.compile(SHARD_PATTERN, Pattern.CASE_INSENSITIVE),
                this::getShardInfoHandler));
        routeList.add(Pair.of(Pattern.compile(SHARDGROUP_PATTERN, Pattern.CASE_INSENSITIVE),
                this::getShardGroupInfoHandler));
        routeList.add(Pair.of(Pattern.compile(LIST_SHARDGROUP_PATTERN, Pattern.CASE_INSENSITIVE),
                this::listShardGroupInfoHandler));
        routeList.add(Pair.of(Pattern.compile(REMOVE_SHARDGROUP_REPLICAS_PATTERN, Pattern.CASE_INSENSITIVE),
                this::removeShardGroupReplicasHandler));
        routeList.add(Pair.of(Pattern.compile(WORKER_PATTERN, Pattern.CASE_INSENSITIVE),
                this::getWorkerInfoHandler));
        routeList.add(Pair.of(Pattern.compile(WORKERGROUP_PATTERN, Pattern.CASE_INSENSITIVE),
                this::getWorkerGroupInfoHandler));
        routeList.add(Pair.of(Pattern.compile(LIST_WORKERGROUP_PATTERN, Pattern.CASE_INSENSITIVE),
                this::listWorkerGroupInfoHandler));
        starManager = manager;
    }

    Object getObject(String path) throws StarException {
        for (Pair<Pattern, Function<Matcher, Object>> pair : routeList) {
            Matcher matcher = pair.getKey().matcher(path);
            if (matcher.matches()) {
                Function<Matcher, Object> handler = pair.getValue();
                return handler.apply(matcher);
            }
        }
        throw new NotExistStarException("No matching pattern found");
    }

    public Object getShardInfoHandler(Matcher matcher) throws StarException {
        String service = matcher.group(SERVICE);
        String shardId = matcher.group(ID);
        Long id;
        try {
            id = Long.valueOf(shardId);
        } catch (NumberFormatException e) {
            throw new InvalidArgumentStarException("Shard id: {} is not a long type number", shardId);
        }

        String serviceId = starManager.getServiceIdByIdOrName(service);
        List<ShardInfo> shardInfoList =
                starManager.getShardInfo(serviceId, Collections.singletonList(id), Constant.DEFAULT_ID);
        return shardInfoList;
    }

    public Object getShardGroupInfoHandler(Matcher matcher) throws StarException {
        String service = matcher.group(SERVICE);
        String shardGroupId = matcher.group(ID);
        Long id;
        try {
            id = Long.valueOf(shardGroupId);
        } catch (NumberFormatException e) {
            throw new InvalidArgumentStarException("Shard group id: {} is not a long type number", shardGroupId);
        }

        String serviceId = starManager.getServiceIdByIdOrName(service);
        List<ShardGroupInfo> shardGroupInfoList =
                starManager.getShardGroupInfo(serviceId, Collections.singletonList(id));
        return shardGroupInfoList;
    }

    public Object listShardGroupInfoHandler(Matcher matcher) throws StarException {
        String service = matcher.group(SERVICE);
        String serviceId = starManager.getServiceIdByIdOrName(service);
        List<ShardGroupInfo> shardGroupInfoList =
                starManager.listShardGroupInfo(serviceId, true, 0).getKey();
        return shardGroupInfoList;
    }

    public Object removeShardGroupReplicasHandler(Matcher matcher) throws StarException {
        String service = matcher.group(SERVICE);
        String shardGroupId = matcher.group(ID);
        Long id;
        try {
            id = Long.valueOf(shardGroupId);
        } catch (NumberFormatException e) {
            throw new InvalidArgumentStarException("Shard group id: {} is not a long type number", shardGroupId);
        }

        String serviceId = starManager.getServiceIdByIdOrName(service);
        starManager.removeShardGroupReplicas(serviceId, id);
        return null;
    }

    public Object getWorkerInfoHandler(Matcher matcher) throws StarException {
        String service = matcher.group(SERVICE);
        String workerId = matcher.group(ID);
        Long id;
        try {
            id = Long.valueOf(workerId);
        } catch (NumberFormatException e) {
            throw new InvalidArgumentStarException("Worker id: {} is not a long type number", workerId);
        }

        String serviceId = starManager.getServiceIdByIdOrName(service);
        WorkerInfo workerInfo = starManager.getWorkerInfo(serviceId, id);
        return workerInfo;
    }

    public Object getWorkerGroupInfoHandler(Matcher matcher) throws StarException {
        String service = matcher.group(SERVICE);
        String workerGroupId = matcher.group(ID);
        Long id;
        try {
            id = Long.valueOf(workerGroupId);
        } catch (NumberFormatException e) {
            throw new InvalidArgumentStarException("Worker group id: {} is not a long type number", workerGroupId);
        }

        String serviceId = starManager.getServiceIdByIdOrName(service);
        List<WorkerGroupDetailInfo> workerGroupList = starManager.listWorkerGroups(serviceId,
                Collections.singletonList(id), Collections.emptyMap(), true);
        return workerGroupList;
    }

    public Object listWorkerGroupInfoHandler(Matcher matcher) throws StarException {
        String service = matcher.group(SERVICE);
        String serviceId = starManager.getServiceIdByIdOrName(service);
        List<WorkerGroupDetailInfo> workerGroupList = starManager.listWorkerGroups(serviceId, Collections.emptyList(),
                Collections.emptyMap(), true);
        return workerGroupList;
    }
}