//  Copyright 2021-present StarRocks, Inc. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.starrocks.load.pipe;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.catalog.Table;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreatePipeStmt;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.thrift.TBrokerFileStatus;
import org.apache.commons.collections4.CollectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Pipe: continuously load and unload data
 */
public class Pipe implements Writable {

    @SerializedName(value = "name")
    private final String name;
    @SerializedName(value = "id")
    private final long id;
    @SerializedName(value = "targetTable")
    private Table targetTable;

    // FIXME: refine these data structure according to implementation of table function
    private TableFunctionRelation dataSource;
    private List<BrokerFileGroup> fileGroups;
    private List<List<TBrokerFileStatus>> fileStatusesList;

    // Internal state
    private State state;
    private List<PipeTaskDesc> readyTasks = new ArrayList<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public Pipe(String name, long id, Table targetTable) {
        this.name = name;
        this.id = id;
        this.targetTable = targetTable;
    }

    public static Pipe fromStatement(long id, CreatePipeStmt stmt) {
        TableFunctionRelation tableFunctionRelation = stmt.getTableFunctionRelation();

        return new Pipe(stmt.getPipeName(), id, stmt.getTargetTable());
    }

    /**
     * Poll event from data source
     * TODO: getObjects from s3
     */
    public void poll() throws UserException {
        if (CollectionUtils.isNotEmpty(fileStatusesList)) {
            return;
        }

        // FIXME: get a broker desc
        BrokerDesc brokerDesc = null;
        List<List<TBrokerFileStatus>> fileStatusList = Lists.newArrayList();
        for (BrokerFileGroup fileGroup : fileGroups) {
            List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
            for (String path : fileGroup.getFilePaths()) {
                HdfsUtil.parseFile(path, brokerDesc, fileStatuses);
            }
            fileStatusList.add(fileStatuses);
        }
        this.fileStatusesList = fileStatusList;
    }

    /**
     * Try to execute the pipe
     */
    public List<PipeTaskDesc> execute() {
        try {
            lock.writeLock().lock();
            if (!CollectionUtils.isNotEmpty(readyTasks)) {
                List<PipeTaskDesc> runnable =
                        readyTasks.stream().filter(PipeTaskDesc::isRunnable).collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(runnable)) {
                    return runnable;
                }

                // TODO: cleanup failed tasks
            }

            buildNewTasks();
            return readyTasks;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void buildNewTasks() {
        StringBuilder sb = new StringBuilder();
        // FIXME: keep projection and filter
        sb.append("INSERT INTO " + targetTable.getName() + " SELECT * FROM TABLE(");
        sb.append("file_list='");
        long sumSize = 0;
        for (List<TBrokerFileStatus> fileList : fileStatusesList) {
            for (TBrokerFileStatus file : fileList) {
                sb.append(file.getPath());
                sumSize += file.getSize();
                if (sumSize >= idealBatchSize()) {
                    break;
                }
            }
        }
        sb.append("'");
        sb.append(")");
        String sqlTask = sb.toString();
        String uniqueName = String.format("pipe-%d-task-%d", id, GlobalStateMgr.getCurrentState().getNextId());
        PipeTaskDesc taskDesc = new PipeTaskDesc(uniqueName, sqlTask, null);
        readyTasks.add(taskDesc);
    }

    private long idealBatchSize() {
        return 1 << 30;
    }

    public void pause() {
    }

    public void resume() {
    }

    public boolean isRunnable() {
        return this.state.equals(State.RUNNING);
    }

    public String getName() {
        return name;
    }

    public long getId() {
        return id;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(Table targetTable) {
        this.targetTable = targetTable;
    }

    public TableFunctionRelation getDataSource() {
        return dataSource;
    }

    public void setDataSource(TableFunctionRelation dataSource) {
        this.dataSource = dataSource;
    }

    public static Pipe read(DataInput input) throws IOException {
        String json = Text.readString(input);
        return GsonUtils.GSON.fromJson(json, Pipe.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Pipe pipe = (Pipe) o;
        return id == pipe.id && Objects.equals(name, pipe.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id);
    }

    enum State {
        PAUSED,
        RUNNING,
        FINISHED,
        ERROR,
    }

}
