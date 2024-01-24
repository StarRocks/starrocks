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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/meta/ColocateMetaService.java

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

package com.starrocks.http.meta;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.starrocks.catalog.ColocateGroupSchema;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.RestBaseResult;
import com.starrocks.http.rest.RestResult;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Type;
import java.util.List;

/*
 * the colocate meta define in {@link ColocateTableIndex}
 * The actions in ColocateMetaService is for modifying or showing colocate group info manually.
 *
 * ColocateMetaAction:
 *  get all information in ColocateTableIndex, as a json string
 *      eg:
 *          GET /api/colocate
 *      return:
 *          {"colocate_meta":{"groupName2Id":{...},"group2Tables":{}, ...},"status":"OK"}
 *
 *      eg:
 *          POST    /api/colocate/group_stable?db_id=123&group_id=456   (mark group[123.456] as stable)
 *          POST    /api/colocate/group_unstable?db_id=123&group_id=456 (mark group[123.456] as unstable)
 *
 * BucketSeqAction:
 *  change the backends per bucket sequence of a group
 *      eg:
 *          POST    /api/colocate/bucketseq?db_id=123&group_id=456
 */
public class ColocateMetaService {
    private static final String GROUP_ID = "group_id";
    private static final String DB_ID = "db_id";

    private static ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();

    private static GroupId checkAndGetGroupId(BaseRequest request) throws DdlException {
        long grpId = Long.valueOf(request.getSingleParameter(GROUP_ID).trim());
        long dbId = Long.valueOf(request.getSingleParameter(DB_ID).trim());
        GroupId groupId = new GroupId(dbId, grpId);

        if (!colocateIndex.isGroupExist(groupId)) {
            throw new DdlException("the group " + groupId + "isn't  exist");
        }
        return groupId;
    }

    public static class ColocateMetaBaseAction extends RestBaseAction {
        ColocateMetaBaseAction(ActionController controller) {
            super(controller);
        }

        @Override
        public void executeWithoutPassword(BaseRequest request, BaseResponse response)
                throws DdlException, AccessDeniedException {
            if (redirectToLeader(request, response)) {
                return;
            }
            UserIdentity currentUser = ConnectContext.get().getCurrentUserIdentity();
            checkUserOwnsAdminRole(currentUser);
            executeInLeaderWithAdmin(request, response);
        }

        // implement in derived classes
        protected void executeInLeaderWithAdmin(BaseRequest request, BaseResponse response)
                throws DdlException {
            throw new DdlException("Not implemented");
        }
    }

    // get all colocate meta
    public static class ColocateMetaAction extends ColocateMetaBaseAction {
        ColocateMetaAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            ColocateMetaAction action = new ColocateMetaAction(controller);
            controller.registerHandler(HttpMethod.GET, "/api/colocate", action);
        }

        @Override
        public void executeInLeaderWithAdmin(BaseRequest request, BaseResponse response)
                throws DdlException {
            response.setContentType("application/json");
            RestResult result = new RestResult();
            result.addResultEntry("colocate_meta", GlobalStateMgr.getCurrentState().getColocateTableIndex());
            sendResult(request, response, result);
        }
    }

    // mark a colocate group as stable
    public static class MarkGroupStableAction extends ColocateMetaBaseAction {
        MarkGroupStableAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            MarkGroupStableAction action = new MarkGroupStableAction(controller);
            controller.registerHandler(HttpMethod.POST, "/api/colocate/group_stable", action);
        }

        @Override
        public void executeInLeaderWithAdmin(BaseRequest request, BaseResponse response)
                throws DdlException {
            GroupId groupId = checkAndGetGroupId(request);

            HttpMethod method = request.getRequest().method();
            if (method.equals(HttpMethod.POST)) {
                colocateIndex.markGroupStable(groupId, true);
            } else {
                response.appendContent(new RestBaseResult("HTTP method is not allowed.").toJson());
                writeResponse(request, response, HttpResponseStatus.METHOD_NOT_ALLOWED);
            }

            sendResult(request, response);
        }
    }

    // mark a colocate group as unstable
    public static class MarkGroupUnstableAction extends ColocateMetaBaseAction {
        MarkGroupUnstableAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            MarkGroupUnstableAction action = new MarkGroupUnstableAction(controller);
            controller.registerHandler(HttpMethod.POST, "/api/colocate/group_unstable", action);
        }

        @Override
        public void executeInLeaderWithAdmin(BaseRequest request, BaseResponse response)
                throws DdlException {
            GroupId groupId = checkAndGetGroupId(request);

            HttpMethod method = request.getRequest().method();
            if (method.equals(HttpMethod.POST)) {
                colocateIndex.markGroupUnstable(groupId, true);
            } else {
                response.appendContent(new RestBaseResult("HTTP method is not allowed.").toJson());
                writeResponse(request, response, HttpResponseStatus.METHOD_NOT_ALLOWED);
            }

            sendResult(request, response);
        }
    }

    // only applies to lake table
    public static class UpdateGroupAction extends ColocateMetaBaseAction {
        private static final String TABLE_ID = "table_id";
        private static final String IS_JOIN = "is_join";

        UpdateGroupAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            UpdateGroupAction action = new UpdateGroupAction(controller);
            controller.registerHandler(HttpMethod.POST, "/api/colocate/update_group", action);
        }

        @Override
        public void executeInLeaderWithAdmin(BaseRequest request, BaseResponse response)
                throws DdlException {
            HttpMethod method = request.getRequest().method();
            if (!method.equals(HttpMethod.POST)) {
                response.appendContent(new RestBaseResult("HTTP method is not allowed.").toJson());
                writeResponse(request, response, HttpResponseStatus.METHOD_NOT_ALLOWED);
            }
            long dbId = Long.valueOf(request.getSingleParameter(DB_ID).trim());
            if (dbId <= 0) {
                response.appendContent("Bad db_id parameter");
                writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                return;
            }
            long grpId = Long.valueOf(request.getSingleParameter(GROUP_ID).trim());
            if (grpId <= 0) {
                response.appendContent("Bad group_id parameter");
                writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                return;
            }
            GroupId groupId = new GroupId(dbId, grpId);
            long tableId = Long.valueOf(request.getSingleParameter(TABLE_ID).trim());
            if (tableId <= 0) {
                response.appendContent("Bad table_id parameter");
                writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                return;
            }
            String isJoinStr = request.getSingleParameter(IS_JOIN);
            boolean isJoin = true;
            if (Strings.isNullOrEmpty(isJoinStr)) {
                response.appendContent("Missing is_join parameter");
                writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                return;
            }
            if (!isJoinStr.equalsIgnoreCase("true") && !isJoinStr.equalsIgnoreCase("false")) {
                response.appendContent("Invalid is_join parameter");
                writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                return;
            }
            if (isJoinStr.equalsIgnoreCase("true")) {
                isJoin = true;
            } else {
                isJoin = false;
            }

            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIncludeRecycleBin(groupId.dbId);
            if (db == null) {
                response.appendContent("Non-exist db");
                writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                return;
            }
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.WRITE);
            try {
                OlapTable table = (OlapTable) globalStateMgr.getLocalMetastore().getTableIncludeRecycleBin(db, tableId);
                if (table == null) {
                    response.appendContent("Non-exist table");
                    writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                    return;
                }
                if (!table.isCloudNativeTable()) {
                    response.appendContent("Not-supported table type");
                    writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                    return;
                }
                colocateIndex.updateLakeTableColocationInfo(table, isJoin, groupId);
                response.appendContent("update succeed");
                sendResult(request, response);
            } finally {
                locker.unLockDatabase(db, LockType.WRITE);
            }
        }
    }

    // update a backendsPerBucketSeq meta for a colocate group
    public static class BucketSeqAction extends ColocateMetaBaseAction {
        private static final Logger LOG = LogManager.getLogger(BucketSeqAction.class);

        BucketSeqAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            BucketSeqAction action = new BucketSeqAction(controller);
            controller.registerHandler(HttpMethod.POST, "/api/colocate/bucketseq", action);
        }

        @Override
        public void executeInLeaderWithAdmin(BaseRequest request, BaseResponse response)
                throws DdlException {
            GroupId groupId = checkAndGetGroupId(request);

            String meta = request.getContent();
            Type type = new TypeToken<List<List<Long>>>() {
            }.getType();
            List<List<Long>> backendsPerBucketSeq = new Gson().fromJson(meta, type);
            LOG.info("get buckets sequence: {}", backendsPerBucketSeq);

            ColocateGroupSchema groupSchema = GlobalStateMgr.getCurrentState().getColocateTableIndex().getGroupSchema(groupId);
            if (backendsPerBucketSeq.size() != groupSchema.getBucketsNum()) {
                throw new DdlException("Invalid bucket num. expected: " + groupSchema.getBucketsNum() + ", actual: "
                        + backendsPerBucketSeq.size());
            }

            List<Long> clusterBackendIds =
                    GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(true);
            //check the Backend id
            for (List<Long> backendIds : backendsPerBucketSeq) {
                if (backendIds.size() != groupSchema.getReplicationNum()) {
                    throw new DdlException("Invalid backend num per bucket. expected: "
                            + groupSchema.getReplicationNum() + ", actual: " + backendIds.size());
                }
                for (Long beId : backendIds) {
                    if (!clusterBackendIds.contains(beId)) {
                        throw new DdlException("The backend " + beId + " does not exist or not available");
                    }
                }
            }

            int bucketsNum = colocateIndex.getBackendsPerBucketSeq(groupId).size();
            Preconditions.checkState(backendsPerBucketSeq.size() == bucketsNum,
                    backendsPerBucketSeq.size() + " vs. " + bucketsNum);
            updateBackendPerBucketSeq(groupId, backendsPerBucketSeq);
            LOG.info("the group {} backendsPerBucketSeq meta has been changed to {}", groupId, backendsPerBucketSeq);

            List<ColocateTableIndex.GroupId> colocateWithGroupsInOtherDb =
                    colocateIndex.getColocateWithGroupsInOtherDb(groupId);
            for (GroupId gid : colocateWithGroupsInOtherDb) {
                updateBackendPerBucketSeq(gid, backendsPerBucketSeq);
                LOG.info("the group {} backendsPerBucketSeq meta has been changed to {}",
                        gid, backendsPerBucketSeq);
            }

            sendResult(request, response);
        }

        private void updateBackendPerBucketSeq(GroupId groupId, List<List<Long>> backendsPerBucketSeq) {
            colocateIndex.addBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
            ColocatePersistInfo info2 =
                    ColocatePersistInfo.createForBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
            GlobalStateMgr.getCurrentState().getEditLog().logColocateBackendsPerBucketSeq(info2);
        }
    }

}
