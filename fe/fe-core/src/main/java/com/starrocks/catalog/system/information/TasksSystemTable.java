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
package com.starrocks.catalog.system.information;

import com.google.common.collect.Lists;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TGetTasksParams;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.thrift.TTaskInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static com.starrocks.catalog.system.SystemTable.MAX_FIELD_VARCHAR_LENGTH;
import static com.starrocks.catalog.system.SystemTable.builder;

public class TasksSystemTable {
    private static final Logger LOG = LogManager.getLogger(TasksSystemTable.class);

    public static final String NAME = "tasks";

    public static SystemTable create() {
        return new SystemTable(SystemId.TASKS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("TASK_NAME", ScalarType.createVarchar(64))
                        .column("CREATE_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("SCHEDULE", ScalarType.createVarchar(64))
                        .column("CATALOG", ScalarType.createVarchar(64))
                        .column("DATABASE", ScalarType.createVarchar(64))
                        .column("DEFINITION", ScalarType.createVarchar(MAX_FIELD_VARCHAR_LENGTH))
                        .column("EXPIRE_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("PROPERTIES", ScalarType.createVarcharType(MAX_FIELD_VARCHAR_LENGTH))
                        .column("CREATOR", ScalarType.createVarchar(64))
                        .build(), TSchemaTableType.SCH_TASKS);
    }

    public static List<TTaskInfo> query(TGetTasksParams params) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        TaskManager taskManager = globalStateMgr.getTaskManager();
        List<Task> taskList = taskManager.filterTasks(params);
        List<TTaskInfo> result = Lists.newArrayList();
        UserIdentity currentUser = null;
        if (params.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        }

        for (Task task : taskList) {
            if (task.getDbName() == null) {
                LOG.warn("Ignore the task db because information is incorrect: " + task);
                continue;
            }

            try {
                ConnectContext context = new ConnectContext();
                context.setCurrentUserIdentity(currentUser);
                context.setCurrentRoleIds(currentUser);
                Authorizer.checkAnyActionOnOrInDb(context, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        task.getDbName());
            } catch (AccessDeniedException e) {
                continue;
            }

            TTaskInfo info = new TTaskInfo();
            info.setTask_name(task.getName());
            info.setCreate_time(task.getCreateTime() / 1000);
            String scheduleStr = "UNKNOWN";
            if (task.getType() != null) {
                scheduleStr = task.getType().name();
            }
            if (task.getType() == Constants.TaskType.PERIODICAL) {
                scheduleStr += task.getSchedule();
            }
            info.setSchedule(scheduleStr);
            info.setCatalog(task.getCatalogName());
            info.setDatabase(ClusterNamespace.getNameFromFullName(task.getDbName()));
            info.setDefinition(task.getDefinition());
            info.setExpire_time(task.getExpireTime() / 1000);
            info.setProperties(task.getPropertiesString());
            if (task.getUserIdentity() != null) {
                info.setCreator(task.getUserIdentity().toString());
            } else {
                info.setCreator(task.getCreateUser());
            }
            result.add(info);
        }
        return result;
    }
}
