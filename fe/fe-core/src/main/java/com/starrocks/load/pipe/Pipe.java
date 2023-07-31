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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.VariableMgr;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.SubmitResult;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.PipeAnalyzer;
import com.starrocks.sql.ast.pipe.CreatePipeStmt;
import com.starrocks.sql.ast.pipe.PipeName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Pipe: continuously load and unload data
 */
public class Pipe implements GsonPostProcessable {

    private static final Logger LOG = LogManager.getLogger(Pipe.class);

    public static final int DEFAULT_POLL_INTERVAL = 10;
    public static final long DEFAULT_BATCH_SIZE = 1 << 30;
    public static final int FAILED_TASK_THRESHOLD = 5;

    @SerializedName(value = "name")
    private final String name;
    @SerializedName(value = "id")
    private final PipeId id;
    @SerializedName(value = "type")
    private final Type type;
    @SerializedName(value = "state")
    private State state;
    @SerializedName(value = "originSql")
    private String originSql;
    @SerializedName(value = "filePipeSource")
    private FilePipeSource pipeSource;
    @SerializedName(value = "targetTable")
    private TableName targetTable;
    @SerializedName(value = "properties")
    private Map<String, String> properties;
    @SerializedName(value = "createdTime")
    private long createdTime = -1;
    @SerializedName(value = "load_status")
    private LoadStatus loadStatus = new LoadStatus();

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Map<Long, PipeTaskDesc> runningTasks = new HashMap<>();
    private int failedTaskExecutionCount = 0;
    private int pollIntervalSecond = DEFAULT_POLL_INTERVAL;
    private long lastPolledTime = 0;

    protected Pipe(PipeId id, String name, TableName targetTable, FilePipeSource sourceTable, String originSql) {
        this.name = Preconditions.checkNotNull(name);
        this.id = Preconditions.checkNotNull(id);
        this.targetTable = Preconditions.checkNotNull(targetTable);
        this.type = Type.FILE;
        this.state = State.RUNNING;
        this.pipeSource = sourceTable;
        this.originSql = originSql;
        this.createdTime = System.currentTimeMillis();
    }

    public static Pipe fromStatement(long id, CreatePipeStmt stmt) {
        PipeName pipeName = stmt.getPipeName();
        long dbId = GlobalStateMgr.getCurrentState().getDb(pipeName.getDbName()).getId();
        PipeId pipeId = new PipeId(dbId, id);
        Pipe res = new Pipe(pipeId, pipeName.getPipeName(), stmt.getTargetTable(), stmt.getDataSource(),
                stmt.getInsertSql());
        stmt.getDataSource().initPipeId(pipeId);
        res.properties = stmt.getProperties();
        res.processProperties();
        return res;
    }

    private void processProperties() {
        if (MapUtils.isEmpty(properties)) {
            return;
        }
        if (properties.containsKey(PipeAnalyzer.PROPERTY_POLL_INTERVAL)) {
            this.pollIntervalSecond = Integer.parseInt(properties.get(PipeAnalyzer.PROPERTY_POLL_INTERVAL));
        }
        if (properties.containsKey(PipeAnalyzer.PROPERTY_AUTO_INGEST)) {
            boolean value = VariableMgr.parseBooleanVariable(properties.get(PipeAnalyzer.PROPERTY_AUTO_INGEST));
            pipeSource.setAutoIngest(value);
        }
        if (properties.containsKey(PipeAnalyzer.PROPERTY_BATCH_SIZE)) {
            long batchSize = Long.parseLong(properties.get(PipeAnalyzer.PROPERTY_BATCH_SIZE));
            pipeSource.setBatchSize(batchSize);
        }
    }

    /**
     * Poll event from data source
     */
    public void poll() throws UserException {
        long nextPollTime = lastPolledTime + pollIntervalSecond;
        if (System.currentTimeMillis() / 1000 < nextPollTime) {
            return;
        }
        if (pipeSource.eos()) {
            return;
        }

        try {
            lock.writeLock().lock();
            lastPolledTime = System.currentTimeMillis() / 1000;
            pipeSource.poll();
        } catch (Throwable e) {
            changeState(State.ERROR, true);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Try to execute the pipe
     * 1. It should be event-driven and asynchronous, and drive the task lifecycle
     * 2. It needs to clean up the timeout and failed task in asynchronous style
     * <p>
     * Running Task Lifecycle:
     * 1. Pull from PipeSource: turn a PipePiece(a bunch of files) into a task
     * 2. Runnable and wait for schedule
     * 3. Get scheduled, and become running
     * 4. If the task execution get any error, retry for a few times, before it fails
     * 5. Either FINISHED/FAILED, change the state of corresponding source file and remove the tasks
     */
    public void schedule() {
        if (!getState().equals(State.RUNNING)) {
            return;
        }

        buildNewTasks();
        scheduleRunnableTasks();
        finalizeTasks();
    }

    /**
     * Pull PipePiece from source, and build new tasks
     */
    private void buildNewTasks() {
        Preconditions.checkState(type == Type.FILE);

        if (MapUtils.isNotEmpty(runningTasks)) {
            return;
        }
        FilePipeSource fileSource = (FilePipeSource) pipeSource;
        FilePipePiece piece = (FilePipePiece) fileSource.pullPiece();
        if (piece == null) {
            // EOS
            if (fileSource.eos()) {
                changeState(State.FINISHED, true);
                LOG.info("pipe {} finish all tasks, change state to {}", this, state);
            }
            return;
        }

        try {
            lock.writeLock().lock();

            // TODO: structural replace ?
            String sqlTask = originSql.replaceAll("(?i)TABLE\\(.*\\)", buildFileSelectSource(piece));
            long taskId = GlobalStateMgr.getCurrentState().getNextId();
            PipeId pipeId = getPipeId();
            String uniqueName = PipeTaskDesc.genUniqueTaskName(getName(), taskId, 0);
            String dbName = GlobalStateMgr.getCurrentState().mayGetDb(pipeId.getDbId())
                    .map(Database::getOriginName)
                    .orElseThrow(() -> ErrorReport.buildSemanticException(ErrorCode.ERR_BAD_DB_ERROR));
            PipeTaskDesc taskDesc = new PipeTaskDesc(taskId, uniqueName, dbName, sqlTask, piece);
            taskDesc.setErrorLimit(FAILED_TASK_THRESHOLD);

            runningTasks.put(taskId, taskDesc);
            LOG.debug("pipe {} build task: {}", name, taskDesc);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Schedule runnable tasks
     * // TODO: async execution
     */
    private void scheduleRunnableTasks() {
        if (MapUtils.isEmpty(runningTasks)) {
            return;
        }

        for (PipeTaskDesc task : runningTasks.values()) {
            executeTask(task);
        }
    }

    /**
     * Clean up FINISHED/FAILED tasks
     */
    private void finalizeTasks() {
        List<Long> removeTaskId = new ArrayList<>();
        try {
            lock.writeLock().lock();

            for (PipeTaskDesc task : runningTasks.values()) {
                if (task.isFinished() || task.tooManyErrors()) {
                    removeTaskId.add(task.getId());
                    pipeSource.finishPiece(task.getPiece(), task.getState());
                }
                if (task.isError()) {
                    failedTaskExecutionCount++;
                    if (failedTaskExecutionCount > FAILED_TASK_THRESHOLD) {
                        changeState(State.ERROR, false);
                    }
                }
                if (task.isFinished()) {
                    FilePipePiece piece = task.getPiece();
                    loadStatus.loadFiles++;
                    loadStatus.loadBytes += piece.getTotalBytes();
                    loadStatus.loadRows += piece.getTotalRows();
                }
            }
            for (long taskId : removeTaskId) {
                runningTasks.remove(taskId);
            }

            // Persist LoadStatus
            // TODO: currently we cannot guarantee the consistency of LoadStatus and FileList
            if (CollectionUtils.isNotEmpty(removeTaskId)) {
                persistPipe();
                LOG.info("pipe {} remove finalized tasks {}", this, removeTaskId);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void changeState(State state, boolean persist) {
        try {
            lock.writeLock().lock();
            this.state = state;
            if (persist) {
                PipeManager pm = GlobalStateMgr.getCurrentState().getPipeManager();
                pm.updatePipe(this);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void persistPipe() {
        try {
            lock.writeLock().lock();
            PipeManager pm = GlobalStateMgr.getCurrentState().getPipeManager();
            pm.updatePipe(this);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private String buildFileSelectSource(FilePipePiece piece) {
        FilePipeSource fileSource = (FilePipeSource) pipeSource;
        StringBuilder sb = new StringBuilder();
        sb.append("TABLE(");
        boolean isFirst = true;
        for (Map.Entry<String, String> entry : fileSource.getTableProperties().entrySet()) {
            if (!isFirst) {
                sb.append(", ");
            }
            isFirst = false;
            if (entry.getKey().equalsIgnoreCase(TableFunctionTable.PROPERTY_PATH)) {
                // TODO: it's not supported right now
                String files =
                        piece.getFiles().stream().map(PipeFileRecord::getFileName).collect(Collectors.joining(","));
                sb.append("'").append(TableFunctionTable.PROPERTY_PATH).append("'='").append(files).append("'");
            } else {
                sb.append("'").append(entry.getKey()).append("'='");
                sb.append(entry.getValue()).append("'");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    private void executeTask(PipeTaskDesc taskDesc) {
        if (!taskDesc.needSchedule()) {
            return;
        }

        if (taskDesc.isRunning()) {
            // Task is running, check the execution state
            // TODO: timeout
            Preconditions.checkNotNull(taskDesc.getFuture());
            if (taskDesc.getFuture().isDone()) {
                try {
                    Constants.TaskRunState taskRunState = taskDesc.getFuture().get();
                    if (taskRunState == Constants.TaskRunState.SUCCESS) {
                        taskDesc.onFinished();
                        LOG.info("finish pipe {} task {}", this, taskDesc);
                    } else {
                        taskDesc.onError(String.format("task execution state: " + taskRunState.toString()));
                    }
                } catch (Throwable e) {
                    taskDesc.onError(String.format("task exception: " + e.getMessage()));
                }
            } else if (taskDesc.getFuture().isCancelled()) {
                taskDesc.onError("task got cancelled");
            }
        } else if (taskDesc.isRunnable()) {
            // Submit a new task
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            Task task = TaskBuilder.buildPipeTask(taskDesc);
            try {
                taskManager.createTask(task, false);
            } catch (DdlException e) {
                LOG.error("create pipe task error: ", e);
                taskDesc.onError("create failed: " + e.getMessage());
                return;
            }
            // TODO: persist the submitted task list in pipe
            SubmitResult result = taskManager.executeTaskAsync(task, new ExecuteOption());
            taskDesc.onRunning();
            taskDesc.setFuture(result.getFuture());
            if (result.getStatus() != SubmitResult.SubmitStatus.SUBMITTED) {
                taskDesc.onError("submit task error");
            }
        } else if (taskDesc.isError()) {
            // On error, need retry
            // TODO: retry the task itself instead of creating another task
            taskDesc.onRetry();
            String newName = PipeTaskDesc.genUniqueTaskName(getName(), taskDesc.getId(), taskDesc.getRetryCount());
            taskDesc.setUniqueTaskName(newName);
            LOG.info("retry pipe {} task {}", this, taskDesc);
        }
    }

    public void suspend() {
        try {
            lock.writeLock().lock();

            if (this.state == State.RUNNING) {
                this.state = State.SUSPEND;

                for (PipeTaskDesc task : runningTasks.values()) {
                    task.interrupt();
                }
                LOG.info("suspend pipe " + this);

                if (!runningTasks.isEmpty()) {
                    runningTasks.clear();
                    LOG.info("suspend pipe {} and clear running tasks {}", this, runningTasks);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void resume() {
        try {
            lock.writeLock().lock();

            if (this.state == State.SUSPEND || this.state == State.ERROR) {
                this.state = State.RUNNING;
                this.failedTaskExecutionCount = 0;
                LOG.info("Resume pipe " + this);
            }

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void destroy() {
        getPipeSource().getFileListRepo().destroy();
    }

    public boolean isRunnable() {
        return this.state != null &&
                this.state.equals(State.RUNNING);
    }

    public List<PipeTaskDesc> getRunningTasks() {
        try {
            lock.readLock().lock();
            return new ArrayList<>(runningTasks.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    public int getFailedTaskExecutionCount() {
        return failedTaskExecutionCount;
    }

    public State getState() {
        try {
            lock.readLock().lock();
            return state;
        } finally {
            lock.readLock().unlock();
        }
    }

    public String getName() {
        return name;
    }

    public long getId() {
        return id.getId();
    }

    public PipeId getPipeId() {
        return id;
    }

    /**
     * Pair<DatabaseId, PipeName>
     */
    public Pair<Long, String> getDbAndName() {
        return Pair.create(getPipeId().getDbId(), getName());
    }

    public Type getType() {
        return type;
    }

    public TableName getTargetTable() {
        return targetTable;
    }

    public FilePipeSource getPipeSource() {
        return pipeSource;
    }

    public String getOriginSql() {
        return originSql;
    }

    public LoadStatus getLoadStatus() {
        return loadStatus;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public long getLastPolledTime() {
        return lastPolledTime;
    }

    @VisibleForTesting
    public void setLastPolledTime(long value) {
        this.lastPolledTime = value;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        this.runningTasks = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();
        pipeSource.initPipeId(id);
        processProperties();
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    public static Pipe fromJson(String json) {
        return GsonUtils.GSON.fromJson(json, Pipe.class);
    }

    @Override
    public String toString() {
        return "pipe(" + name + ")";
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
        return Objects.equals(name, pipe.name) && Objects.equals(id, pipe.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id);
    }

    public static class LoadStatus {
        @SerializedName(value = "load_files")
        public long loadFiles = 0;
        @SerializedName(value = "load_rows")
        public long loadRows = 0;
        @SerializedName(value = "load_bytes")
        public long loadBytes = 0;
    }

    public enum State {
        SUSPEND,
        RUNNING,
        FINISHED,
        ERROR,
    }

    enum Type {
        FILE,
        KAFKA,
    }

}
