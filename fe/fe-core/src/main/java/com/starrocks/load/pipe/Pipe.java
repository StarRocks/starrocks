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
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.TableName;
import com.starrocks.common.CloseableLock;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.pipe.filelist.FileListRepo;
import com.starrocks.metric.PipeMetricMgr;
import com.starrocks.persist.AlterPipeLog;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
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
import com.starrocks.sql.common.DmlException;
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

    public static final int MAX_POLL_INTERVAL = 3600; // 1 hour
    public static final long DEFAULT_BATCH_SIZE = 1 << 30; // 1 GB
    public static final long DEFAULT_BATCH_FILES = 256;
    public static final int FAILED_TASK_THRESHOLD = 5;

    // Cap the error text embedded in a PIPE_TASK_FAILED log record. Matches the width of
    // information_schema.pipe_files.ERROR_MSG so the two stay comparable.
    private static final int ERROR_MSG_LOG_LIMIT = 512;

    private static final String TASK_PROPERTY_PREFIX = "task.";

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
    private long createdTime;
    @SerializedName(value = "load_status")
    private LoadStatus loadStatus = new LoadStatus();
    @SerializedName(value = "task_execution_variables")
    private Map<String, String> taskExecutionVariables;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Map<Long, PipeTaskDesc> runningTasks = new HashMap<>();
    private ErrorInfo lastErrorInfo = new ErrorInfo();
    private int failedTaskExecutionCount = 0;
    private int pollIntervalSecond = Config.pipe_default_poll_interval_s;
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
        this.createdTime = TimeUtils.getEpochSeconds();
        this.properties = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.taskExecutionVariables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    }

    public static Pipe fromStatement(long id, CreatePipeStmt stmt) {
        PipeName pipeName = stmt.getPipeName();
        long dbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(pipeName.getDbName()).getId();
        PipeId pipeId = new PipeId(dbId, id);
        com.starrocks.catalog.TableName targetTableName = 
                com.starrocks.catalog.TableName.fromTableRef(stmt.getTargetTableRef());
        if (targetTableName == null) {
            throw new IllegalArgumentException("Target table reference cannot be null when creating a pipe");
        }
        Pipe res = new Pipe(pipeId, pipeName.getPipeName(), targetTableName, stmt.getDataSource(),
                stmt.getInsertSql());
        stmt.getDataSource().initPipeId(pipeId);
        res.recovered = true;
        res.processProperties(stmt.getProperties());
        return res;
    }

    public void alterProperties(Map<String, String> properties) {
        try (CloseableLock l = takeWriteLock()) {
            validateProperties(properties);
            AlterPipeLog alterPipeLog = new AlterPipeLog(id);
            alterPipeLog.setChangeProps(properties);
            GlobalStateMgr.getCurrentState().getEditLog().logAlterPipe(alterPipeLog, wal -> {
                processProperties(properties);
            });
        }
    }

    private void validateProperties(Map<String, String> properties) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey().toLowerCase();
            String value = entry.getValue();
            switch (key) {
                case PipeAnalyzer.PROPERTY_POLL_INTERVAL, PipeAnalyzer.PROPERTY_BATCH_FILES: {
                    Integer.parseInt(value);
                    break;
                }
                case PipeAnalyzer.PROPERTY_AUTO_INGEST: {
                    ParseUtil.parseBooleanValue(value, PipeAnalyzer.PROPERTY_AUTO_INGEST);
                    break;
                }
                case PipeAnalyzer.PROPERTY_BATCH_SIZE: {
                    ParseUtil.parseDataVolumeStr(value);
                    break;
                }
                case PropertyAnalyzer.PROPERTIES_WAREHOUSE: {
                    // warehouse property is validated in PipeAnalyzer.analyzeWarehouseProperty
                    // Just check that value is not empty
                    if (StringUtils.isEmpty(value)) {
                        throw new IllegalArgumentException("warehouse property cannot be empty");
                    }
                    break;
                }
                default: {
                    // task execution variables
                    if (!key.startsWith(TASK_PROPERTY_PREFIX)) {
                        throw new IllegalArgumentException("unsupported property: " + entry.getKey());
                    }
                }
            }
        }
    }

    public void processProperties(Map<String, String> properties) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey().toLowerCase();
            String value = entry.getValue();
            switch (key) {
                case PipeAnalyzer.PROPERTY_POLL_INTERVAL: {
                    this.pollIntervalSecond = Integer.parseInt(value);
                    break;
                }
                case PipeAnalyzer.PROPERTY_AUTO_INGEST: {
                    pipeSource.setAutoIngest(ParseUtil.parseBooleanValue(value, PipeAnalyzer.PROPERTY_AUTO_INGEST));
                    break;
                }
                case PipeAnalyzer.PROPERTY_BATCH_SIZE: {
                    pipeSource.setBatchSize(ParseUtil.parseDataVolumeStr(value));
                    break;
                }
                case PipeAnalyzer.PROPERTY_BATCH_FILES: {
                    pipeSource.setBatchFiles(Integer.parseInt(value));
                    break;
                }
                case PropertyAnalyzer.PROPERTIES_WAREHOUSE: {
                    // put the warehouse into variables, and it would be processed by TaskRun
                    this.taskExecutionVariables.put(key, value);
                    break;
                }
                default: {
                    // task execution variables
                    if (key.startsWith(TASK_PROPERTY_PREFIX)) {
                        String taskVariable = StringUtils.removeStart(key, TASK_PROPERTY_PREFIX);
                        this.taskExecutionVariables.put(taskVariable, value);
                    } else {
                        throw new IllegalArgumentException("unsupported property: " + entry.getKey());
                    }
                }
            }
            if (this.properties != properties) {
                this.properties.put(key, value);
            }
        }
    }

    /**
     * Poll event from data source
     */
    public void poll() throws StarRocksException {
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

        GlobalTransactionMgr txnMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
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
                TransactionStatus txnStatus = txnMgr.getLabelStatus(dbId, file.insertLabel).getStatus();
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

        // These files are resolved outside the task lifecycle, so they never reach finalizeTasks().
        // Account them here: without this, every file in flight across a leader change is missing
        // from both counters. No task counters (there is no task) and no rows (getTotalRows() is a
        // stub).
        if (CollectionUtils.isNotEmpty(failedFiles)) {
            pipeSource.getFileListRepo().updateFileState(failedFiles, FileListRepo.PipeFileState.ERROR, null);
            PipeMetricMgr.incPipeFailedFiles(dbId, type.name(), failedFiles.size());
            PipeMetricMgr.incPipeFailedBytes(dbId, type.name(), totalFileSize(failedFiles));
        }
        if (CollectionUtils.isNotEmpty(loadedFiles)) {
            pipeSource.getFileListRepo().updateFileState(loadedFiles, FileListRepo.PipeFileState.FINISHED, null);
            PipeMetricMgr.incPipeLoadedFiles(dbId, type.name(), loadedFiles.size());
            PipeMetricMgr.incPipeLoadedBytes(dbId, type.name(), totalFileSize(loadedFiles));
        }

        LOG.info("{} pipe recovered to state {}, failed-files: {}, loaded-files: {}",
                name, state, failedFiles, loadedFiles);
        recovered = true;
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

        try (CloseableLock l = takeWriteLock()) {
            long taskId = GlobalStateMgr.getCurrentState().getNextId();
            PipeId pipeId = getPipeId();
            String uniqueName = PipeTaskDesc.genUniqueTaskName(getName(), taskId, 0);
            String dbName = GlobalStateMgr.getCurrentState().getLocalMetastore().mayGetDb(pipeId.getDbId())
                    .map(Database::getOriginName)
                    .orElseThrow(() -> ErrorReport.buildSemanticException(ErrorCode.ERR_BAD_DB_ERROR));
            String sqlTask = FilePipeSource.buildInsertSql(this, piece, uniqueName);
            PipeTaskDesc taskDesc = new PipeTaskDesc(taskId, uniqueName, dbName, sqlTask, piece);
            taskDesc.getVariables().putAll(taskExecutionVariables);
            taskDesc.setErrorLimit(FAILED_TASK_THRESHOLD);

            // Persist the loading state
            fileSource.getFileListRepo()
                    .updateFileState(piece.getFiles(), FileListRepo.PipeFileState.LOADING, uniqueName);

            runningTasks.put(taskId, taskDesc);
            loadStatus.loadingFiles += piece.getNumFiles();
            LOG.debug("pipe {} build task: {}", name, taskDesc);
        } catch (Throwable e) {
            recordPipeError(e.getMessage());
        }
    }

    /**
     * Schedule runnable tasks
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
        Runnable changeStateAction = null;
        LoadStatus changedLoadStatus = loadStatus.cloneForUpdate();
        try (CloseableLock l = takeWriteLock()) {
            for (PipeTaskDesc task : runningTasks.values()) {
                if (task.isFinished() || task.tooManyErrors()) {
                    removeTaskId.add(task.getId());
                    pipeSource.finishPiece(task);
                    changedLoadStatus.loadingFiles -= task.getPiece().getNumFiles();
                    if (task.tooManyErrors()) {
                        // finishPiece() has just marked every file of the piece as ERROR, so this is
                        // the only point at which the failure is final. Do not move this into the
                        // task.isError() branch below: that state is re-entered once per failed
                        // attempt, so it would count the same files FAILED_TASK_THRESHOLD + 1 times.
                        recordTerminalTaskFailure(task);
                    }
                }
                if (task.isError()) {
                    PipeMetricMgr.incPipeCompleteTasks(getPipeId().getDbId(), type.name(), "ERROR", 1);
                    failedTaskExecutionCount++;
                    if (failedTaskExecutionCount > FAILED_TASK_THRESHOLD) {
                        changeStateAction = () -> changeState(State.ERROR, false);
                    }
                }
                if (task.isFinished()) {
                    FilePipePiece piece = task.getPiece();
                    changedLoadStatus.loadedFiles += piece.getNumFiles();
                    changedLoadStatus.loadedBytes += piece.getTotalBytes();
                    changedLoadStatus.loadRows += piece.getTotalRows();
                    changedLoadStatus.lastLoadedTime = LocalDateTime.now(ZoneId.systemDefault());

                    long dbId = getPipeId().getDbId();
                    PipeMetricMgr.incPipeCompleteTasks(dbId, type.name(), "SUCCESS", 1);
                    PipeMetricMgr.incPipeLoadedFiles(dbId, type.name(), piece.getNumFiles());
                    PipeMetricMgr.incPipeLoadedBytes(dbId, type.name(), piece.getTotalBytes());
                    PipeMetricMgr.incPipeLoadedRows(dbId, type.name(), piece.getTotalRows());
                }
            }
            for (long taskId : removeTaskId) {
                runningTasks.remove(taskId);
            }
        }

        if (changeStateAction != null) {
            changeStateAction.run();
        }

        // Persist LoadStatus
        // TODO: currently we cannot guarantee the consistency of LoadStatus and FileList
        if (CollectionUtils.isNotEmpty(removeTaskId)) {
            PipeManager pm = GlobalStateMgr.getCurrentState().getPipeManager();
            AlterPipeLog alterPipeLog = new AlterPipeLog(id);
            alterPipeLog.setLoadStatus(changedLoadStatus);
            try (CloseableLock l = pm.takeWriteLock();
                    CloseableLock r = takeWriteLock()) {
                GlobalStateMgr.getCurrentState().getEditLog().logAlterPipe(
                        alterPipeLog, wal -> this.loadStatus = changedLoadStatus);
            }
            LOG.info("pipe {} remove finalized tasks {}", this, removeTaskId);
        }
    }

    /**
     * Account a task that has exhausted its retries: the failure counters plus one structured log
     * record carrying the pipe identity and the error text.
     *
     * <p>The log record exists because per-pipe identity does not belong in a metric label. Pipes
     * can be created and dropped in quick succession, so a {@code pipe_name} label would accumulate
     * dead series for {@link Config#pipe_metric_expire_minutes}. A log event carries no time-series
     * cardinality cost, and it can carry the error message, which a counter cannot.</p>
     */
    private void recordTerminalTaskFailure(PipeTaskDesc task) {
        FilePipePiece piece = task.getPiece();
        long dbId = getPipeId().getDbId();
        long failedFiles = piece.getNumFiles();
        long failedBytes = piece.getTotalBytes();

        PipeMetricMgr.incPipeFailedTasks(dbId, type.name(), 1);
        PipeMetricMgr.incPipeFailedFiles(dbId, type.name(), failedFiles);
        PipeMetricMgr.incPipeFailedBytes(dbId, type.name(), failedBytes);

        // Keep this record on one line and keep its shape stable: it is meant to be matched by
        // log-based alerting, which is what gives per-pipe granularity without a metric label.
        // Every value has to stay a single token, so pipe_id emits the bare numeric id rather
        // than the PipeId object, whose toString() renders as "PipeId{dbId=1, id=2}" and would
        // split across two tokens under a logfmt parser, one of them a spurious "id" key.
        LOG.warn("PIPE_TASK_FAILED pipe={} pipe_id={} db_id={} task={} failed_files={} "
                        + "failed_bytes={} error=\"{}\"",
                name, id.getId(), dbId, task.getUniqueTaskName(), failedFiles, failedBytes,
                singleLineError(task.getErrorMsg()));
    }

    private static long totalFileSize(List<PipeFileRecord> files) {
        return files.stream().mapToLong(PipeFileRecord::getFileSize).sum();
    }

    /**
     * Collapse an error message onto a single line so a PIPE_TASK_FAILED record stays parseable by
     * log-based alerting, and bound its length.
     *
     * <p>Worth testing directly: the messages that actually reach this method today are short and
     * already single-line, because {@link #checkTaskExecutionResult} substitutes a placeholder when
     * the task run is missing from the history. The collapse and the cap therefore get no coverage
     * from the end-to-end failure path.</p>
     */
    @VisibleForTesting
    static String singleLineError(String error) {
        if (StringUtils.isEmpty(error)) {
            return "";
        }
        String flat = error.replaceAll("\\s+", " ").replace('"', '\'').trim();
        return flat.length() <= ERROR_MSG_LOG_LIMIT
                ? flat : flat.substring(0, ERROR_MSG_LOG_LIMIT) + "...";
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
        PipeManager pm = GlobalStateMgr.getCurrentState().getPipeManager();
        try (CloseableLock l = pm.takeWriteLock();
                CloseableLock r = this.takeWriteLock()) {
            unprotectedChangeState(state, persist);
        } catch (Throwable e) {
            LOG.error("update pipe state {} failed: {}", toString(), e.getMessage(), e);
        }
    }

    private void unprotectedChangeState(State state, boolean persist) {
        if (this.state.equals(state)) {
            return;
        }
        if (persist) {
            AlterPipeLog alterPipeLog = new AlterPipeLog(this.id);
            alterPipeLog.setState(state);
            GlobalStateMgr.getCurrentState().getEditLog().logAlterPipe(alterPipeLog, wal -> this.state = state);
        } else {
            this.state = state;
        }
    }

    private void executeTask(PipeTaskDesc taskDesc) {
        if (!taskDesc.needSchedule()) {
            return;
        }

        if (taskDesc.isRunning()) {
            // Task is running, check the execution state
            Preconditions.checkNotNull(taskDesc.getFuture());
            checkTaskExecutionResult(taskDesc);
        } else if (taskDesc.isRunnable()) {
            // Submit a new task
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            Task task = TaskBuilder.buildPipeTask(taskDesc);
            try {
                taskManager.createTask(task);
            } catch (DdlException e) {
                recordTaskError(taskDesc, "create task failed");
                return;
            }
            SubmitResult result = taskManager.executeTaskAsync(task, new ExecuteOption(task));
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

    private void checkTaskExecutionResult(PipeTaskDesc taskDesc) {
        if (taskDesc.getFuture().isCancelled()) {
            recordTaskError(taskDesc, "task got cancelled");
            return;
        } else if (!taskDesc.getFuture().isDone()) {
            return;
        }
        try {
            Constants.TaskRunState taskRunState = taskDesc.getFuture().get();
            if (taskRunState == Constants.TaskRunState.FAILED) {
                TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();
                TaskRunStatus status = tm.getTaskRunHistory().getTaskByName(taskDesc.getUniqueTaskName());
                if (status != null) {
                    throw new DmlException("execution failed: " + status.getErrorMessage());
                } else {
                    throw new DmlException("task failed with unknown status");
                }
            }
            taskDesc.onFinished();
        } catch (Throwable e) {
            String message = e.getMessage();
            if (LabelAlreadyUsedException.isLabelAlreadyUsed(message)) {
                taskDesc.onFinished();
                return;
            }
            recordTaskError(taskDesc, e.getMessage());
        }
    }

    public void suspend(boolean persist) {
        try (CloseableLock l = takeWriteLock()) {
            if (this.state == State.RUNNING) {
                unprotectedChangeState(State.SUSPEND, persist);

                for (PipeTaskDesc task : runningTasks.values()) {
                    if (task.isTaskRunning()) {
                        task.interrupt();
                    }
                }
                LOG.info("suspend pipe {}", this);

                loadStatus.loadingFiles = 0;
            }
        }
    }

    public void resume() {
        try (CloseableLock l = takeWriteLock()) {
            if (this.state == State.SUSPEND || this.state == State.ERROR) {
                unprotectedChangeState(State.RUNNING, true);
                this.failedTaskExecutionCount = 0;
                LOG.info("Resume pipe " + this);
            }
        }
    }

    public void retry(AlterPipeClauseRetry retry) {
        // Update file state to allow scheduling
        if (retry.isRetryAll()) {
            getPipeSource().retryErrorFiles();
        } else {
            getPipeSource().retryFailedFile(retry.getFile());
        }

        // Change the pipe state if it's stopped
        if (getState().canResume()) {
            changeState(State.RUNNING, true);
        }
    }

    public void destroy() {
        getPipeSource().getFileListRepo().destroy();
    }

    /**
     * Reset transient leader-session state so the next leader re-runs
     * {@link #recovery()} and rebuilds {@code runningTasks} via {@link #buildNewTasks()}.
     * Pipe metadata itself is persistent; only the in-memory progress flags are dropped.
     * The PipeTaskDesc handles in {@code runningTasks} were tied to BE RPC bookkeeping on
     * the old leader, so leaving them around would block buildNewTasks() (it skips when
     * runningTasks is non-empty) and Pipe.recovery() (it short-circuits when recovered).
     */
    public void resetForLeaderHandoff() {
        try (CloseableLock l = takeWriteLock()) {
            recovered = false;
            runningTasks.clear();
        }
    }

    public boolean isRecovered() {
        return recovered;
    }

    public boolean isRunnable() {
        return recovered && this.state != null && this.state.equals(State.RUNNING);
    }

    public List<PipeTaskDesc> getRunningTasks() {
        try (CloseableLock l = takeReadLock()) {
            return new ArrayList<>(runningTasks.values());
        }
    }

    public CloseableLock takeWriteLock() {
        return CloseableLock.lock(lock.writeLock());
    }

    public CloseableLock takeReadLock() {
        return CloseableLock.lock(lock.readLock());
    }

    public int getFailedTaskExecutionCount() {
        return failedTaskExecutionCount;
    }

    public State getState() {
        try (CloseableLock l = takeReadLock()) {
            return state;
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

    public String getTypeName() {
        return type.name();
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

    /**
     * Unix timestamp in seconds
     */
    public long getCreatedTime() {
        return createdTime;
    }

    public long getLastPolledTime() {
        return lastPolledTime;
    }

    public int getPollIntervalSecond() {
        return pollIntervalSecond;
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

    public String getPropertiesString() {
        return PropertyAnalyzer.stringifyProperties(properties);
    }

    @VisibleForTesting
    public void setLastPolledTime(long value) {
        this.lastPolledTime = value;
    }

    public void setState(State state) {
        this.state = state;
    }

    public void setLoadStatus(LoadStatus loadStatus) {
        this.loadStatus = loadStatus;
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

        public LoadStatus cloneForUpdate() {
            LoadStatus status = new LoadStatus();
            status.loadedFiles = this.loadedFiles;
            status.loadedBytes = this.loadedBytes;
            status.loadingFiles = this.loadingFiles;
            status.loadRows = this.loadRows;
            status.lastLoadedTime = this.lastLoadedTime;
            return status;
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
        ERROR;

        public boolean canResume() {
            return this.equals(SUSPEND) || this.equals(ERROR);
        }
    }

    enum Type {
        FILE,
        KAFKA,
    }

}
