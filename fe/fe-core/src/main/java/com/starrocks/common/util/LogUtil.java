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

package com.starrocks.common.util;

import com.starrocks.mysql.MysqlAuthPacket;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryDetail;
import com.starrocks.qe.QueryDetailQueue;
import com.starrocks.server.GlobalStateMgr;

public class LogUtil {

    public static void logConnectionInfoToAuditLogAndQueryQueue(ConnectContext ctx, MysqlAuthPacket authPacket) {
        AuditEvent.AuditEventBuilder builder = new AuditEvent.AuditEventBuilder()
                .setEventType(AuditEvent.EventType.CONNECTION)
                .setUser(authPacket == null ? "null" : authPacket.getUser())
                .setAuthorizedUser(ctx.getCurrentUserIdentity() == null
                        ? "null" : ctx.getCurrentUserIdentity().toString())
                .setClientIp(ctx.getMysqlChannel().getRemoteHostPortString())
                .setDb(authPacket == null ? "null" : authPacket.getDb())
                .setState(ctx.getState().toString())
                .setErrorCode(ctx.getState().getErrorMessage());
        GlobalStateMgr.getCurrentAuditEventProcessor().handleAuditEvent(builder.build());

        QueryDetail queryDetail = new QueryDetail();
        queryDetail.setQueryId(DebugUtil.printId(UUIDUtil.genUUID()));
        queryDetail.setState(ctx.getState().isError() ?
                QueryDetail.QueryMemState.FAILED : QueryDetail.QueryMemState.FINISHED);
        queryDetail.setUser(authPacket == null ? "null" : authPacket.getUser());
        queryDetail.setRemoteIP(ctx.getRemoteIP());
        queryDetail.setDatabase(authPacket == null ? "null" : authPacket.getDb());
        queryDetail.setErrorMessage(ctx.getState().getErrorMessage());
        QueryDetailQueue.addAndRemoveTimeoutQueryDetail(queryDetail);
    }
}
