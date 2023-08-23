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

package com.starrocks.load.pipe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.load.pipe.filelist.FileListRepo;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.VariableMgr;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.SubmitResult;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.PipeAnalyzer;
import com.starrocks.sql.ast.pipe.AlterPipeClauseRetry;
import com.starrocks.sql.ast.pipe.CreatePipeStmt;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionStatus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Pipe: continuously load and unload data
 */
public class Pipe implements GsonPostProcessable {

    private static final Logger LOG = LogManager.getLogger(Pipe.class);

    public static final int DEFAULT_POLL_INTERVAL = 10;
    public static final long DEFAULT_BATCH_SIZE = 1 << 30;
    public static final long DEFAULT_BATCH_FILES = 256;
    public static final int FAILED_TASK_THRESHOLD = 5;

    private static final ImmutableMap<String, String> DEFAULT_TASK_EXECUTION_VARIABLES =
            ImmutableMap.<String, String>builder()
                    .put("query_timeout", "3600")
                    .build();

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
    @SerializedName(value = "task_execution_variables")
    private Map<String, String> taskExecutionVariables;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Map<Long, PipeTaskDesc> runningTasks = new HashMap<>();
    private ErrorInfo lastErrorInfo = new ErrorInfo();
    private int failedTaskExecutionCount = 0;
    private int pollIntervalSecond = DEFAULT_POLL_INTERVAL;
    private long lastPolledTime = 0;
    private boolean recovered = false;

    protected Pipe(PipeId id, String name, TableName targetTable, FilePipeSource sourceTable, String originSql) {
        this.name = Preconditions.checkNotNull(name);
        this.id = Preconditions.checkNotNull(id);
        this.targetTable = Preconditions.checkNotNull(targetTable);
        this.type = Type.FILE;
        this.state = State.RUNNING;
        this.pipeSource = sourceTable;
        this.originSql = originSql;
        this.createdTime = System.currentTimeMillis();
        this.properties = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.taskExecutionVariables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.taskExecutionVariables.putAll(DEFAULT_TASK_EXECUTION_VARIABLES);
    }

    public static Pipe fromStatement(long id, CreatePipeStmt stmt) {
        PipeName pipeName = stmt.getPipeName();
        long dbId = GlobalStateMgr.getCurrentState().getDb(pipeName.getDbName()).getId();
        PipeId pipeId = new PipeId(dbId, id);
        Pipe res = new Pipe(pipeId, pipeName.getPipeName(), stmt.getTargetTable(), stmt.getDataSource(),
                stmt.getInsertSql());
        stmt.getDataSource().initPipeId(pipeId);
        res.recovered = true;
        res.processProperties(stmt.getProperties());
        return res;
    }

    public void processProperties(Map<String, String> properties) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            switch (key.toLowerCase()) {
                case PipeAnalyzer.PROPERTY_POLL_INTERVAL: {
                    this.pollIntervalSecond = Integer.parseInt(properties.get(PipeAnalyzer.PROPERTY_POLL_INTERVAL));
                    break;
                }
                case PipeAnalyzer.PROPERTY_AUTO_INGEST: {
                    pipeSource.setAutoIngest(
                            VariableMgr.parseBooleanVariable(properties.get(PipeAnalyzer.PROPERTY_AUTO_INGEST)));
                    break;
                }
                case PipeAnalyzer.PROPERTY_BATCH_SIZE: {
                    pipeSource.setBatchSize(
                            ParseUtil.parseDataVolumeStr(properties.get(PipeAnalyzer.PROPERTY_BATCH_SIZE)));
                    break;
                }
                case PipeAnalyzer.PROPERTY_BATCH_FILES: {
                    pipeSource.setBatchFiles(Integer.parseInt(properties.get(PipeAnalyzer.PROPERTY_BATCH_FILES)));
                    break;
                }
                default: {
                    // task execution variables
                    if (key.toUpperCase().startsWith("TASK.")) {
                        String taskVariable = StringUtils.removeStart(key.toUpperCase(), "TASK.");
                        this.taskExecutionVariables.put(taskVariable, value);
                    } else {
                        throw new IllegalArgumentException("unsupported property: " + key);
                    }
                }
            }
            this.properties.put(key, value);
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
            lastPolledTime = System.currentTimeMillis() / 1000;
            pipeSource.poll();
        } catch (Throwable e) {
            recordPipeError("poll from source failed: " + e.getMessage());
            changeState(State.ERROR, true);
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
     * Recovery after restart, to guarantee exactly-once ingestion
     * 1. Persist the insert label along with file before loading state
     * 2. Check the insert success or not by insert-label after restart recovery
     * 3. As a result, we could clearly distinguish insert success or not,
     * so that each file would not be ingested repeatedly
     */
    public void recovery() {
        if (recovered) {
            return;
        }
        LOG.info("pipe {} start to recover", name);

        GlobalTransactionMgr txnMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();
        long dbId = getPipeId().getDbId();
        List<PipeFileRecord> loadingFiles =
                pipeSource.getFileListRepo().listFilesByState(FileListRepo.PipeFileState.LOADING, -1);

        if (CollectionUtils.isEmpty(loadingFiles)) {
            recovered = true;
            return;
        }

        for (PipeFileRecord file : loadingFiles) {
            if (StringUtils.isEmpty(file.insertLabel)) {
                file.loadState = FileListRepo.PipeFileState.ERROR;
            } else {
                TransactionStatus txnStatus = txnMgr.getLabelStatus(dbId, file.insertLabel);
                if (txnStatus == null || txnStatus.isFailed()) {
                    file.loadState = FileListRepo.PipeFileState.ERROR;
                } else {
                    file.loadState = FileListRepo.PipeFileState.FINISHED;
                }
            }
        }

        List<PipeFileRecord> failedFiles = loadingFiles.stream()
                .filter(x -> x.loadState == FileListRepo.PipeFileState.ERROR)
                .collect(Collectors.toList());
        List<PipeFileRecord> loadedFiles = loadingFiles.stream()
                .filter(x -> x.loadState == FileListRepo.PipeFileState.FINISHED)
                .collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(failedFiles)) {
            pipeSource.getFileListRepo().updateFileState(failedFiles, FileListRepo.PipeFileState.ERROR, null);
        }
        if (CollectionUtils.isNotEmpty(loadedFiles)) {
            pipeSource.getFileListRepo().updateFileState(loadedFiles, FileListRepo.PipeFileState.FINISHED, null);
        }

        LOG.info("{} pipe recovered to state {}, failed-files: {}, loaded-files: {}",
                name, state, failedFiles, loadedFiles);
        recovered = true;
        persistPipe();
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
                boolean allLoaded = fileSource.allLoaded();
                if (allLoaded) {
                    changeState(State.FINISHED, true);
                    LOG.info("pipe {} finish all tasks, change state to {}", this, state);
                } else {
                    // Some error happen
                    recordPipeError("leave some unfinished files");
                    changeState(State.ERROR, true);
                    LOG.info("pipe {} finish all tasks but with error files, change state to {}, ", this, state);
                }
            }
            return;
        }

        try {
            lock.writeLock().lock();

            long taskId = GlobalStateMgr.getCurrentState().getNextId();
            PipeId pipeId = getPipeId();
            String uniqueName = PipeTaskDesc.genUniqueTaskName(getName(), taskId, 0);
            String dbName = GlobalStateMgr.getCurrentState().mayGetDb(pipeId.getDbId())
                    .map(Database::getOriginName)
                    .orElseThrow(() -> ErrorReport.buildSemanticException(ErrorCode.ERR_BAD_DB_ERROR));
            String sqlTask = FilePipeSource.buildInsertSql(this, piece, uniqueName);
            PipeTaskDesc taskDesc = new PipeTaskDesc(taskId, uniqueName, dbName, sqlTask, piece);
            taskDesc.getProperties().putAll(taskExecutionVariables);
            taskDesc.setErrorLimit(FAILED_TASK_THRESHOLD);

            // Persist the loading state
            fileSource.getFileListRepo()
                    .updateFileState(piece.getFiles(), FileListRepo.PipeFileState.LOADING, uniqueName);

            runningTasks.put(taskId, taskDesc);
            loadStatus.loadingFiles += piece.getNumFiles();
            LOG.debug("pipe {} build task: {}", name, taskDesc);
        } catch (Throwable e) {
            recordPipeError(e.getMessage());
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
                    pipeSource.finishPiece(task);
                    loadStatus.loadingFiles -= task.getPiece().getNumFiles();
                }
                if (task.isError()) {
                    failedTaskExecutionCount++;
                    if (failedTaskExecutionCount > FAILED_TASK_THRESHOLD) {
                        changeState(State.ERROR, false);
                    }
                }
                if (task.isFinished()) {
                    FilePipePiece piece = task.getPiece();
                    loadStatus.loadedFiles += piece.getNumFiles();
                    loadStatus.loadedBytes += piece.getTotalBytes();
                    loadStatus.loadRows += piece.getTotalRows();
                    loadStatus.lastLoadedTime = LocalDateTime.now(ZoneId.systemDefault());
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

    private void recordTaskError(PipeTaskDesc task, Throwable e) {
        task.onError(e.getMessage());
        lastErrorInfo.errorMessage = e.getMessage();
        lastErrorInfo.errorTime = LocalDateTime.now(ZoneId.systemDefault());
        LOG.warn("pipe {} execute task {} failed: {}", this, task, e.getMessage(), e);
    }

    private void recordTaskError(PipeTaskDesc task, String error) {
        task.onError(error);
        lastErrorInfo.errorMessage = error;
        lastErrorInfo.errorTime = LocalDateTime.now(ZoneId.systemDefault());
        LOG.warn("pipe {} execute task {} failed: {}", this, task, error);
    }

    private void recordPipeError(String error) {
        lastErrorInfo.errorMessage = error;
        lastErrorInfo.errorTime = LocalDateTime.now(ZoneId.systemDefault());
    }

    private void changeState(State state, boolean persist) {
        try {
            lock.writeLock().lock();
            this.state = state;
            if (persist) {
                PipeManager pm = GlobalStateMgr.getCurrentState().getPipeManager();
                pm.updatePipe(this);
            }
        } catch (Throwable e) {
            LOG.error("update pipe state {} failed: {}", toString(), e.getMessage(), e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void persistPipe() {
        try {
            lock.writeLock().lock();
            PipeManager pm = GlobalStateMgr.getCurrentState().getPipeManager();
            pm.updatePipe(this);
        } catch (Throwable e) {
            LOG.error("persist pipe {} state failed: {}", this, e.getMessage(), e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void executeTask(PipeTaskDesc taskDesc) {
        if (!taskDesc.needSchedule()) {
            return;
        }

        if (taskDesc.isRunning()) {
            // Task is running, check the execution state
            Preconditions.checkNotNull(taskDesc.getFuture());
            if (taskDesc.getFuture().isDone()) {
                try {
                    Constants.TaskRunState taskRunState = taskDesc.getFuture().get();
                    if (taskRunState == Constants.TaskRunState.SUCCESS) {
                        taskDesc.onFinished();
                        LOG.info("finish pipe {} task {}", this, taskDesc);
                    } else {
                        TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();
                        TaskRunStatus status = tm.getTaskRunHistory().getTaskByName(taskDesc.getUniqueTaskName());
                        if (status != null) {
                            taskDesc.onError(status.getErrorMessage());
                            recordTaskError(taskDesc, status.getErrorMessage());
                        } else {
                            recordTaskError(taskDesc, "task failed with unknown status");
                        }
                    }
                } catch (Throwable e) {
                    recordTaskError(taskDesc, e);
                }
            } else if (taskDesc.getFuture().isCancelled()) {
                recordTaskError(taskDesc, "task got cancelled");
            }
        } else if (taskDesc.isRunnable()) {
            // Submit a new task
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            Task task = TaskBuilder.buildPipeTask(taskDesc);
            try {
                taskManager.createTask(task, false);
            } catch (DdlException e) {
                recordTaskError(taskDesc, "create task failed");
                return;
            }
            SubmitResult result = taskManager.executeTaskAsync(task, new ExecuteOption());
            taskDesc.onRunning();
            taskDesc.setFuture(result.getFuture());
            if (result.getStatus() != SubmitResult.SubmitStatus.SUBMITTED) {
                recordTaskError(taskDesc, "submit task failed: " + result);
            }
        } else if (taskDesc.isError()) {
            // On error, need retry
            // TODO: retry the task itself instead of creating another task
            taskDesc.onRetry();
            String newName = PipeTaskDesc.genUniqueTaskName(getName(), taskDesc.getId(), taskDesc.getRetryCount());
            taskDesc.setUniqueTaskName(newName);
            LOG.info("retry pipe {} failed task {}", this, taskDesc);
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
                loadStatus.loadingFiles = 0;
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

    public void retry(AlterPipeClauseRetry retry) {
        if (retry.isRetryAll()) {
            getPipeSource().retryErrorFiles();
        } else {
            getPipeSource().retryFailedFile(retry.getFile());
        }
    }

    public void destroy() {
        getPipeSource().getFileListRepo().destroy();
    }

    public boolean isRecovered() {
        return recovered;
    }

    public boolean isRunnable() {
        return recovered && this.state != null && this.state.equals(State.RUNNING);
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

    public ErrorInfo getLastErrorInfo() {
        return lastErrorInfo;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public long getLastPolledTime() {
        return lastPolledTime;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Map<String, String> getTaskProperties() {
        return taskExecutionVariables;
    }

    public String getPropertiesJson() {
        if (MapUtils.isEmpty(properties)) {
            return "";
        }
        Gson gsonObj = new Gson();
        return gsonObj.toJson(properties);
    }

    @VisibleForTesting
    public void setLastPolledTime(long value) {
        this.lastPolledTime = value;
    }

    public void setState(State state) {
        this.state = state;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        this.runningTasks = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.lastErrorInfo = new ErrorInfo();
        pipeSource.initPipeId(id);
        processProperties(this.properties);
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

    public static class LoadStatus implements GsonPostProcessable {
        @SerializedName(value = "loadedFiles")
        public long loadedFiles = 0;
        @SerializedName(value = "loadedBytes")
        public long loadedBytes = 0;

        // Display it, but not persist it
        @SerializedName(value = "loadingFiles")
        public long loadingFiles = 0;
        @SerializedName(value = "lastLoadedTime")
        public LocalDateTime lastLoadedTime;

        // TODO: account loaded rows
        // @SerializedName(value = "load_rows")
        public long loadRows = 0;

        public String toJson() {
            return DateUtils.GSON_PRINTER.toJson(this);
        }

        @Override
        public void gsonPostProcess() throws IOException {
            loadingFiles = 0;
            lastLoadedTime = null;
        }
    }

    /**
     * Last error information of pipe
     */
    public static class ErrorInfo {
        @SerializedName(value = "errorMessage")
        public String errorMessage;
        @SerializedName(value = "errorTime")
        public LocalDateTime errorTime;

        // TODO: file locator

        public String toJson() {
            if (StringUtils.isEmpty(errorMessage)) {
                return null;
            }
            return DateUtils.GSON_PRINTER.toJson(this);
        }
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
