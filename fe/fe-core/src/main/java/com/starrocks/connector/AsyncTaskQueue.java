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

package com.starrocks.connector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AsyncTaskQueue is a class that manages and executes asynchronous tasks
 * <p>
 * The class uses an Executor to run tasks concurrently. It maintains two queues:
 * - outputQueue: stores the results of completed tasks.
 * - taskQueue: stores the tasks that need to be executed.
 * <p>
 * The class also keeps track of the number of currently running tasks using an AtomicInteger.
 * It provides methods to start tasks, retrieve outputs, and handle task exceptions.
 * <p>
 * Make sure there is a single consumer.
 */

/*
 * Here is a code example:
 * AsyncTaskQueue<String> asyncTaskQueue = new AsyncTaskQueue<>(executorService);
 * asyncTaskQueue.setMaxRunningTaskCount(maxRunningTaskCount);
 * asyncTaskQueue.setMaxOutputQueueSize(maxOutputQueueSize);
 * asyncTaskQueue.start(tasks);
 * while(asyncTaskQueue.hasMoreOutput()) {
 *     List values = asyncTaskQueue.getOutputs();
 * }
 */

public class AsyncTaskQueue<T> {

    public interface Task<T> {
        List<T> run() throws InterruptedException;

        default List<Task<T>> moreTasks() {
            return null;
        }

        default boolean isDone() {
            return true;
        }
    }

    public AsyncTaskQueue(Executor executor) {
        this.executor = executor;
    }

    // output queue
    ReentrantLock outputQueueLock = new ReentrantLock();
    Condition outputQueueCondition = outputQueueLock.newCondition();
    ArrayList<T> outputQueue = new ArrayList<>();
    AtomicInteger outputQueueSize = new AtomicInteger();
    boolean hasMoreOutput = true;

    // task queue
    ConcurrentLinkedDeque<Task<T>> taskQueue = new ConcurrentLinkedDeque<>();
    AtomicInteger runningTaskCount = new AtomicInteger(0);
    AtomicInteger taskQueueSize = new AtomicInteger(0);
    AtomicReference<Exception> taskException = new AtomicReference<>(null);

    // ---------------
    int maxRunningTaskCount = Integer.MAX_VALUE;
    int maxOutputQueueSize = Integer.MAX_VALUE;
    Executor executor;

    public void setMaxRunningTaskCount(int maxRunningTaskCount) {
        this.maxRunningTaskCount = maxRunningTaskCount;
    }

    public void setMaxOutputQueueSize(int maxOutputQueueSize) {
        this.maxOutputQueueSize = maxOutputQueueSize;
    }

    public void start(List<? extends Task<T>> tasks) {
        taskQueue.addAll(tasks);
        taskQueueSize.addAndGet(tasks.size());
        // or we have to trigger tasks?
        triggerTask();
    }

    public int computeOutputSize(T output) {
        return 1;
    }

    private void tryGetOutputs(List<T> outputs, int maxSize) {
        if (!hasMoreOutput) {
            return;
        }
        try {
            outputQueueLock.lock();
            while (true) {
                if (!outputQueue.isEmpty()) {
                    break;
                }
                // which means any update on those variables should be
                // protected by this lock. we are not sure about if there is more output
                // unless we try to fetch it.
                if (taskException.get() != null || taskQueueSize.get() == 0) {
                    hasMoreOutput = false;
                    if (taskException.get() != null) {
                        throw new RuntimeException(taskException.get());
                    }
                    return;
                }
                outputQueueCondition.await();
            }
            while (maxSize > 0 && !outputQueue.isEmpty()) {
                T output = outputQueue.remove(outputQueue.size() - 1);
                outputs.add(output);
                int size = computeOutputSize(output);
                maxSize -= size;
                outputQueueSize.addAndGet(-size);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            outputQueueLock.unlock();
        }
    }

    public List<T> getOutputs(int maxSize) {
        List<T> outputs = new ArrayList<>();
        int expectedSize = maxSize;
        while (expectedSize > 0) {
            tryGetOutputs(outputs, expectedSize);
            if (!hasMoreOutput) {
                break;
            }
            expectedSize = maxSize - outputs.size();
            triggerTasks();
        }
        return outputs;
    }

    public boolean hasMoreOutput() {
        // update end of stream state
        tryGetOutputs(null, 0);
        return hasMoreOutput;
    }

    private void addOutputs(List<T> outputs) {
        try {
            outputQueueLock.lock();
            outputQueue.addAll(outputs);
            int size = 0;
            for (T output : outputs) {
                size += computeOutputSize(output);
            }
            outputQueueSize.addAndGet(size);
            outputQueueCondition.signal();
        } finally {
            outputQueueLock.unlock();
        }
    }

    private void updateTaskException(Exception e) {
        if (taskException.compareAndSet(null, e)) {
            // notify consumer an exception caught.
            addOutputs(List.of());
        }
    }

    // trigger a single task.
    private boolean triggerTask() {
        // don't trigger task when:
        // 1. output queue is pretty full
        // 2. no task to run.
        // 3. a lot of tasks are running.
        if (outputQueueSize.get() > maxOutputQueueSize || taskQueueSize.get() == 0 ||
                runningTaskCount.get() > maxRunningTaskCount) {
            return false;
        }
        // trigger a new task
        // 1. add running count first to see if ok
        // 2. fetch task from queue, and check if ok
        // 3. submit this task, and check if ok
        // 4. if not ok, dec running count.
        boolean success = false;
        try {
            int count = runningTaskCount.incrementAndGet();
            if (count > maxRunningTaskCount) {
                return false;
            }
            Task task = taskQueue.poll();
            if (task == null) {
                return false;
            }
            executor.execute(new RunnableTask(task));
            success = true;
            return true;
        } catch (RejectedExecutionException e) {
            updateTaskException(e);
        } finally {
            if (!success) {
                runningTaskCount.decrementAndGet();
            }
        }
        return false;
    }

    // trigger enough tasks.
    private void triggerTasks() {
        for (int i = 0; i < maxRunningTaskCount; i++) {
            if (!triggerTask()) {
                break;
            }
        }
    }

    private class RunnableTask implements Runnable {
        Task task;

        RunnableTask(Task task) {
            this.task = task;
        }

        @Override
        public void run() {
            if (taskException.get() == null) {
                // run it only when there is no exception.
                try {
                    List<T> outputs = task.run();
                    addOutputs(outputs);
                    List<Task<T>> moreTasks = task.moreTasks();
                    if (moreTasks != null) {
                        taskQueueSize.addAndGet(moreTasks.size());
                        taskQueue.addAll(moreTasks);
                    }
                    if (!task.isDone()) {
                        taskQueueSize.addAndGet(1);
                        taskQueue.addFirst(task);
                    }
                } catch (Exception e) {
                    updateTaskException(e);
                }
            }
            runningTaskCount.decrementAndGet();
            // all tasks are done, notify the consumer.
            if (taskQueueSize.decrementAndGet() == 0) {
                addOutputs(List.of());
            } else {
                triggerTask();
            }
        }
    }
}
