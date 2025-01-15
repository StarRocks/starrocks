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

package com.starrocks.load.batchwrite;

import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.planner.LoadScanNode;
import com.starrocks.system.ComputeNode;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Responsible for managing the assignment of coordinator backends to loads.
 *
 * <p>
 * These events will trigger the assignment
 * - Registering a new load. Need to assign nodes to it
 * - Unregistering a load. Need to deallocate nodes
 * - Detecting unavailable nodes when getting backends. Need to migrate loads from unavailable nodes to others
 * Besides the above events, the assigner periodically checks node status (scaling out/in, node alive), and reassign
 * nodes to loads if necessary so that the new nodes can be used, and evict the unavailable nodes. When assigning nodes,
 * the assigner tries to make the number of coordinators (parallels) on each node as balanced as possible. If the
 * balance factor exceeds a certain threshold, reassignments will be made.
 *
 * <p>
 * For simplify, the assigner uses a single thread to do all assignments. Each event will create a task, and put it
 * into a priority queue. The thread will poll the queue and execute the task according to the event type The periodical
 * check will also be executed in the same thread. Currently, We think there won't be too many loads that need to be
 * scheduled, so the single thread will not be the bottleneck.
 */
public final class CoordinatorBackendAssignerImpl implements CoordinatorBackendAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorBackendAssignerImpl.class);

    private static final int MIN_CHECK_INTERVAL_MS = 1000;

    private final AtomicLong taskIdAllocator;
    private final PriorityBlockingQueue<Task> taskPriorityQueue;
    private final ExecutorService singleExecutor;

    // Registered load. load id -> LoadMeta
    private final ConcurrentHashMap<Long, LoadMeta> registeredLoadMetas;

    // Warehouse metas. warehouse id -> WarehouseMeta. Only read/write in the singleExecutor
    private final Map<Long, WarehouseMeta> warehouseMetas;

    // The number of tasks to be executed triggered by EventType.DETECT_UNAVAILABLE_NODES.
    // It's used to avoid submitting too many similar tasks when there are unavailable nodes.
    // When the number of pending tasks exceeds a threshold, the new task will be rejected.
    private final AtomicLong numPendingTasksForDetectUnavailableNodes;

    // these are just for testing ========================================
    // The number of executed tasks
    private final AtomicLong numExecutedTasks;
    // The flag indicating whether the periodical check is running. It's mainly used for testing
    private final AtomicBoolean isPeriodicalCheckRunning;

    public CoordinatorBackendAssignerImpl() {
        this.taskIdAllocator = new AtomicLong(0);
        this.taskPriorityQueue = new PriorityBlockingQueue<>(32, TaskComparator.INSTANCE);
        this.singleExecutor = ThreadPoolManager.newDaemonCacheThreadPool(
                1, "coordinator-be-assigner", true);
        this.registeredLoadMetas = new ConcurrentHashMap<>();
        this.warehouseMetas = new HashMap<>();
        this.numPendingTasksForDetectUnavailableNodes = new AtomicLong(0);
        this.numExecutedTasks = new AtomicLong(0);
        this.isPeriodicalCheckRunning = new AtomicBoolean(false);
    }

    @Override
    public void start() {
        this.singleExecutor.submit(this::runSchedule);
        LOG.info("Start coordinator be assigner");
    }

    /**
     * Scheduling loop that continuously polls tasks from the priority queue and executes them.
     * If no tasks are available, it performs a periodical check.
     */
    private void runSchedule() {
        while (true) {
            Task task;
            try {
                int checkIntervalMs;
                if (warehouseMetas.isEmpty()) {
                    // No need to schedule periodically if there is no load, and
                    // it will be woken up by a EventType.REGISTER_LOAD task
                    checkIntervalMs = Integer.MAX_VALUE;
                    LOG.info("Disable periodical schedule because there is no load");
                } else {
                    checkIntervalMs = Math.max(MIN_CHECK_INTERVAL_MS, Config.merge_commit_be_assigner_schedule_interval_ms);
                    LOG.debug("Set schedule interval to {} ms", checkIntervalMs);
                }
                task = taskPriorityQueue.poll(checkIntervalMs, TimeUnit.MILLISECONDS);
            } catch (Throwable throwable) {
                LOG.warn("Failed to poll task queue", throwable);
                continue;
            }

            if (task == null) {
                if (isPeriodicalCheckRunning.compareAndSet(false, true)) {
                    runPeriodicalCheck();
                    isPeriodicalCheckRunning.set(false);
                }
                continue;
            }

            long startTime = System.currentTimeMillis();
            LOG.debug("Start to execute task, type: {}, task id: {}", task.getScheduleType(), task.getTaskId());
            Throwable throwable = null;
            try {
                task.getRunnable().run();
                LOG.info("Success to execute task, type: {}, task id: {}, cost: {} ms",
                        task.getScheduleType(), task.getTaskId(), System.currentTimeMillis() - startTime);
            } catch (Throwable e) {
                LOG.error("Failed to execute task, type: {}, task id: {}, cost: {} ms",
                        task.getScheduleType(), task.getTaskId(), System.currentTimeMillis() - startTime, e);
                throwable = e;
            } finally {
                // numExecutedTasks is just for testing. Should update it before calling task.finish()
                // which may notify the waiting thread. So that after the thread wakes up, it can get
                // the correct numExecutedTasks including this one.
                numExecutedTasks.incrementAndGet();
                task.finish(throwable);
                if (task.getScheduleType() == EventType.DETECT_UNAVAILABLE_NODES) {
                    numPendingTasksForDetectUnavailableNodes.decrementAndGet();
                }
            }
        }
    }


    // Registers a load, and try to wait for the node assignment to finish
    @Override
    public void registerBatchWrite(long loadId, long warehouseId, TableId tableId, int expectParallel) {
        LoadMeta loadMeta = registeredLoadMetas.computeIfAbsent(
                loadId, k -> new LoadMeta(loadId, warehouseId, tableId, expectParallel));
        LOG.info("Register load, load id: {}, warehouse: {}, {}, parallel: {}",
                loadId, warehouseId, tableId, expectParallel);

        // Submit a task to assign nodes to the load, and try to wait for the task to finish
        boolean success = false;
        try {
            Task task = new Task(
                    taskIdAllocator.incrementAndGet(),
                    EventType.REGISTER_LOAD,
                    () -> this.runRegisterLoadTask(loadMeta)
            );
            taskPriorityQueue.add(task);

            long startTime = System.currentTimeMillis();
            try {
                task.future.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOG.warn("Failed to wait for assigning coordinator backends, load id: {}", loadId, e);
                throw new RejectedExecutionException(
                        String.format("Failed to wait for assigning coordinator backends, load id: %s", loadId), e);
            }
            success = true;
            LOG.info("Finish to wait for assigning coordinator backends, load id: {}, cost: {} ms",
                    loadId, (System.currentTimeMillis() - startTime));
        } finally {
            if (!success) {
                registeredLoadMetas.remove(loadId);
            }
        }
    }

    // Unregisters a load.
    @Override
    public void unregisterBatchWrite(long loadId) {
        LoadMeta loadMeta = registeredLoadMetas.remove(loadId);
        if (loadMeta == null) {
            return;
        }
        LOG.info("Unregister load, load id: {}, warehouse: {}, {}", loadId, loadMeta.warehouseId, loadMeta.tableId);

        Task task = new Task(
                taskIdAllocator.incrementAndGet(),
                EventType.UNREGISTER_LOAD,
                () -> this.runUnregisterLoadTask(loadMeta));
        taskPriorityQueue.add(task);
        LOG.info("Success to submit task to deallocate nodes, load id: {}, task id: {}",
                loadMeta.loadId, task.getTaskId());
    }

    // Returns the coordinator backends for a load
    @Override
    public Optional<List<ComputeNode>> getBackends(long loadId) {
        LoadMeta loadMeta = registeredLoadMetas.get(loadId);
        if (loadMeta == null) {
            return Optional.empty();
        }
        List<ComputeNode> nodes = loadMeta.nodes;
        if (nodes == null) {
            return Optional.empty();
        }

        // check node available
        List<ComputeNode> availableNodes = new ArrayList<>(nodes.size());
        List<ComputeNode> unavailableNodes = new ArrayList<>();
        for (ComputeNode node : nodes) {
            if (!node.isAvailable()) {
                unavailableNodes.add(node);
            } else {
                availableNodes.add(node);
            }
        }

        // if there are unavailable nodes, submit an EventType.DETECT_UNAVAILABLE_NODES task
        if (!unavailableNodes.isEmpty()) {
            // avoid submitting too many tasks if there are some pending tasks
            if (numPendingTasksForDetectUnavailableNodes.incrementAndGet() > 3) {
                numPendingTasksForDetectUnavailableNodes.decrementAndGet();
            } else {
                Task task = new Task(
                        taskIdAllocator.incrementAndGet(),
                        EventType.DETECT_UNAVAILABLE_NODES,
                        () -> this.runDetectUnavailableNodesTask(loadMeta));
                taskPriorityQueue.add(task);
                LOG.info("Submit task after finding unavailable nodes, load id: {}, task id: {}, " +
                        "pending task num: {}", loadMeta.loadId, task.getTaskId(),
                            numPendingTasksForDetectUnavailableNodes.get());
            }
        }

        return Optional.of(availableNodes);
    }

    // Execute EventType.REGISTER_LOAD task which assigns nodes to the load
    void runRegisterLoadTask(LoadMeta loadMeta) {
        long warehouseId = loadMeta.warehouseId;
        WarehouseMeta warehouseMeta =
                warehouseMetas.computeIfAbsent(warehouseId, WarehouseMeta::new);

        NavigableSet<NodeMeta> nodeMetas = warehouseMeta.sortedNodeMetas;
        if (nodeMetas.isEmpty()) {
            try {
                List<ComputeNode> nodes = getAvailableNodes(warehouseId);
                nodes.forEach(node -> nodeMetas.add(new NodeMeta(node)));
            } catch (Exception e) {
                LOG.warn("Register load to unknown warehouse, load id: {}, warehouse: {}, {}}",
                        loadMeta.loadId, loadMeta.warehouseId, loadMeta.tableId);
                throw new RuntimeException("Unknown warehouse id " + loadMeta.warehouseId, e);
            }
        }

        Map<Long, LoadMeta> loadMetas = warehouseMeta.loadMetas;
        long loadId = loadMeta.loadId;
        if (loadMetas.putIfAbsent(loadId, loadMeta) != null) {
            LOG.warn("Register duplicate load, load id: {}, warehouse: {}, {}, nodes: {}",
                    loadMeta.loadId, loadMeta.warehouseId, loadMeta.tableId, loadMeta.nodes);
            return;
        }

        int actualParallel  = Math.min(nodeMetas.size(), loadMeta.expectParallel);
        List<NodeMeta> needUpdateNodeMetas = new ArrayList<>(actualParallel);
        List<ComputeNode> selectedNodes = new ArrayList<>(actualParallel);
        try {
            // allocate the nodes with the least number of loads to the load
            for (int i = 0; i < actualParallel; i++) {
                NodeMeta nodeMeta = nodeMetas.pollFirst();
                if (nodeMeta == null) {
                    String errorMsg = String.format("There is no enough node, load id: %s, expected node num: %s, " +
                                    "actual node num: %s, selected nodes: %s", loadId, actualParallel,
                            selectedNodes.size(), selectedNodes);
                    throw new RuntimeException(errorMsg);
                }
                if (nodeMeta.loadIds.contains(loadId)) {
                    String errorMsg = String.format("Find duplicated load in a node, load id: %s, node: %s",
                            loadId, nodeMeta.node);
                    throw new RuntimeException(errorMsg);
                }
                nodeMeta.loadIds.add(loadId);
                selectedNodes.add(nodeMeta.node);
                needUpdateNodeMetas.add(nodeMeta);
            }
        } catch (RuntimeException e) {
            // rollback if any exception happens
            needUpdateNodeMetas.forEach(meta -> meta.loadIds.remove(loadId));
            nodeMetas.addAll(needUpdateNodeMetas);
            throw e;
        }
        nodeMetas.addAll(needUpdateNodeMetas);
        loadMeta.nodes = selectedNodes;
        LOG.info("Allocate nodes for load id: {}, warehouse: {}, {}, expect parallel: {}, actual parallel: {}, " +
                        "selected nodes: {}", loadMeta.loadId, loadMeta.warehouseId, loadMeta.tableId,
                loadMeta.expectParallel, actualParallel, selectedNodes);
        logStatistics(warehouseMeta);
    }

    // Execute EventType.UNREGISTER_LOAD task which deallocates nodes of the load
    private void runUnregisterLoadTask(LoadMeta loadMeta) {
        long warehouseId = loadMeta.warehouseId;
        WarehouseMeta warehouseMeta = warehouseMetas.get(warehouseId);
        if (warehouseMeta == null) {
            LOG.warn("Unregister a load from a non-exist warehouse, load id: {}, warehouse: {}, {}, nodes: {}",
                    loadMeta.loadId, loadMeta.warehouseId, loadMeta.tableId, loadMeta.nodes);
            return;
        }

        Map<Long, LoadMeta> loadMetas = warehouseMeta.loadMetas;
        long loadId = loadMeta.loadId;
        if (loadMetas == null || !loadMetas.containsKey(loadId)) {
            LOG.warn("Unregister a non-exist load, load id: {}, warehouse: {}, {}, nodes: {}",
                    loadMeta.loadId, loadMeta.warehouseId, loadMeta.tableId, loadMeta.nodes);
            return;
        }

        List<NodeMeta> needUpdateNodeMetas = new ArrayList<>();
        NavigableSet<NodeMeta> nodeMetas = warehouseMeta.sortedNodeMetas;
        Iterator<NodeMeta> iterator = nodeMetas.iterator();
        while (iterator.hasNext()) {
            NodeMeta nodeMeta = iterator.next();
            if (nodeMeta.loadIds.remove(loadId)) {
                needUpdateNodeMetas.add(nodeMeta);
                iterator.remove();
            }
        }
        nodeMetas.addAll(needUpdateNodeMetas);
        loadMetas.remove(loadId);
        LOG.info("Deallocate nodes, load id: {}, warehouse: {}, {}, nodes: {}",
                loadMeta.loadId, loadMeta.warehouseId, loadMeta.tableId, loadMeta.nodes);
        logStatistics(warehouseMeta);
    }

    // Execute EventType.DETECT_UNAVAILABLE_NODES task which evicts unavailable nodes
    // and reallocates the loads to other nodes if necessary
    private void runDetectUnavailableNodesTask(LoadMeta loadMeta) {
        WarehouseMeta warehouseMeta = warehouseMetas.get(loadMeta.warehouseId);
        if (warehouseMeta == null) {
            return;
        }
        checkNodeStatusAndReassignment(warehouseMeta);
    }

    void runPeriodicalCheck() {
        long startTime = System.currentTimeMillis();
        try {
            List<Long> warehouseIds = new ArrayList<>(warehouseMetas.keySet());
            for (long warehouseId : warehouseIds) {
                WarehouseMeta warehouseMeta = warehouseMetas.get(warehouseId);
                if (warehouseMeta.loadMetas.isEmpty()) {
                    warehouseMetas.remove(warehouseMeta.warehouseId);
                    LOG.info("Remove empty warehouse {}", warehouseMeta.warehouseId);
                } else {
                    checkNodeStatusAndReassignment(warehouseMeta);
                    doBalanceIfNeeded(warehouseMeta, Config.merge_commit_be_assigner_balance_factor_threshold);
                    if (LOG.isDebugEnabled()) {
                        logStatistics(warehouseMeta);
                    }
                }
            }
            LOG.debug("Success to execute periodical schedule, cost: {} ms", System.currentTimeMillis() - startTime);
        } catch (Throwable throwable) {
            LOG.error("Failed to execute periodical schedule, cost: {} ms",
                    System.currentTimeMillis() - startTime, throwable);
        }
    }

    // Check node status under the warehouse and reassign nodes to loads if necessary.
    // - If there are new nodes, add them to the warehouse meta
    // - If there are unavailable nodes, remove them from the warehouse meta and reassign the loads
    private void checkNodeStatusAndReassignment(WarehouseMeta warehouseMeta) {
        // 1. find the newest unavailable/available nodes
        Map<Long, ComputeNode> currentAvailableNodes = new HashMap<>();
        try {
            getAvailableNodes(warehouseMeta.warehouseId)
                    .forEach(node -> currentAvailableNodes.put(node.getId(), node));
        } catch (Exception e) {
            LOG.debug("Skip to check node status for unknown warehouse, warehouse: {}", warehouseMeta.warehouseId, e);
            return;
        }

        Map<Long, NodeMeta> unavailableNodeMetas = new HashMap<>();
        Map<Long, NodeMeta> availableNodeMetas = new HashMap<>();
        for (NodeMeta nodeMeta : warehouseMeta.sortedNodeMetas) {
            ComputeNode node = nodeMeta.node;
            if (currentAvailableNodes.containsKey(node.getId())) {
                availableNodeMetas.put(node.getId(), nodeMeta);
            } else {
                unavailableNodeMetas.put(node.getId(), nodeMeta);
            }
        }

        // node status is changed if there are unavailable nodes or there are new available nodes
        boolean nodesStatusChanged =
                !unavailableNodeMetas.isEmpty() || availableNodeMetas.size() < currentAvailableNodes.size();
        if (!nodesStatusChanged) {
            LOG.debug("Node status does not change, warehouse: {}, num nodes: {}",
                    warehouseMeta.warehouseId, currentAvailableNodes.size());
            return;
        }

        List<ComputeNode> newAvailableNodes = new ArrayList<>();
        // add those new available nodes to the map
        for (ComputeNode node : currentAvailableNodes.values()) {
            if (!availableNodeMetas.containsKey(node.getId())) {
                newAvailableNodes.add(node);
            }
        }

        LOG.info("Node status changes, and need reassignment, warehouse: {}, num new nodes: {}, " +
                        "num unavailable nodes: {}, to add nodes: {}, to remove nodes: {}",
                warehouseMeta.warehouseId, newAvailableNodes.size(), unavailableNodeMetas.size(), newAvailableNodes,
                unavailableNodeMetas.values().stream().map(meta -> meta.node).collect(Collectors.toList()));

        // 2. Reassign nodes to each load if
        //      1. the old node is unavailable
        //      2. there is new nodes, and the current parallel is less than the expected
        //    For each load
        //      a. calculate the new parallel as min(expectParallel, #availableNodes)
        //      b. remove the old unavailable nodes
        //      c. if the current number of allocated nodes not meets the new parallel,
        //         iterate from the node with the least number of loads, and choose
        //         those not containing this load id
        // The iterating order can allocate loads to the node with the least loads as
        // much as possible, but the result can be not balanced, and we will check
        // balance in balanceIfNeeded()
        long startTime = System.currentTimeMillis();
        NavigableSet<NodeMeta> newSortedNodeMetas = new TreeSet<>(NodeMetaComparator.INSTANCE);
        // the original available nodes
        newSortedNodeMetas.addAll(availableNodeMetas.values());
        // the new available nodes
        newAvailableNodes.forEach(node -> newSortedNodeMetas.add(new NodeMeta(node)));
        // Loads that is reassigned. load id -> new node ids
        Map<Long, Set<Long>> reassignedLoadIds = new HashMap<>();
        for (LoadMeta loadMeta : warehouseMeta.loadMetas.values()) {
            long loadId = loadMeta.loadId;
            int newParallel = Math.min(loadMeta.expectParallel, newSortedNodeMetas.size());
            Set<Long> assignedNodeIds = new HashSet<>();
            boolean findUnaviableNodes = false;
            // check if the original node is unavailable
            for (ComputeNode node : loadMeta.nodes) {
                if (currentAvailableNodes.containsKey(node.getId())) {
                    assignedNodeIds.add(node.getId());
                } else {
                    findUnaviableNodes = true;
                }
            }
            // if the current parallel is less than the new, assign new nodes to the load
            List<NodeMeta> newAssignedNodeMetas = new ArrayList<>(newParallel - assignedNodeIds.size());
            Iterator<NodeMeta> iterator = newSortedNodeMetas.iterator();
            // iterate from the node with the least number of loads, and assign the node to the load
            while (iterator.hasNext() && assignedNodeIds.size() < newParallel) {
                NodeMeta nodeMeta = iterator.next();
                long nodeId = nodeMeta.node.getId();
                // if the node is already assigned to the load, skip it
                if (assignedNodeIds.add(nodeId)) {
                    iterator.remove();
                    nodeMeta.loadIds.add(loadId);
                    newAssignedNodeMetas.add(nodeMeta);
                }
            }
            // put back the updated node metas
            newSortedNodeMetas.addAll(newAssignedNodeMetas);
            if (findUnaviableNodes || !newAssignedNodeMetas.isEmpty()) {
                reassignedLoadIds.put(loadId, assignedNodeIds);
            }
        }

        // 3. update node and load metas
        warehouseMeta.sortedNodeMetas.clear();
        warehouseMeta.sortedNodeMetas.addAll(newSortedNodeMetas);
        for (Map.Entry<Long, Set<Long>> entry : reassignedLoadIds.entrySet()) {
            long loadId = entry.getKey();
            Set<Long> newNodeIds = entry.getValue();
            LoadMeta loadMeta = warehouseMeta.loadMetas.get(loadId);
            List<ComputeNode> newNodes = new ArrayList<>(newNodeIds.size());
            newNodeIds.forEach(id -> newNodes.add(currentAvailableNodes.get(id)));
            List<ComputeNode> oldNodes = loadMeta.nodes;
            loadMeta.nodes = newNodes;
            LOG.info("Reassign nodes, load id: {}, warehouse: {}, {}, old parallel: {}, new parallel: {}, " +
                    "old nodes: {}, new nodes: {}", loadMeta.loadId, loadMeta.warehouseId, loadMeta.tableId,
                    oldNodes.size(), newNodes.size(), oldNodes, newNodes);
        }
        LOG.info("Finish the reassignment, warehouse: {}, num reassigned loads: {}, cost: {} ms",
                warehouseMeta.warehouseId, reassignedLoadIds.size(), System.currentTimeMillis() - startTime);
        logStatistics(warehouseMeta);
    }

    // Nodes can be unbalanced after unregistering loads or scaling out/in.
    // Try to balance if the balance factor exceed the threshold
    private void doBalanceIfNeeded(WarehouseMeta warehouseMeta, double balanceFactorThreshold) {
        double balanceFactor = calculateBalanceFactor(warehouseMeta.sortedNodeMetas);
        if (balanceFactor <= balanceFactorThreshold) {
            return;
        }

        LOG.info("Start to balance warehouse {}, expect balance factor: {}, current balance factor: {}",
                warehouseMeta.warehouseId, balanceFactorThreshold, balanceFactor);

        // All the nodes are unbalanced initially. The process includes many iterations, and each iteration
        // will pick one unbalanced node and make it balanced. After each iteration, we will calculate the
        // balance factor, and stop the following iterations if it is no more than the threshold.
        // The steps in each iteration are:
        //    1. calculate the expected average load number(parallel) per node
        //    2. pick the largest node with the most loads in the unbalanced node set
        //    3. try to move the loads to the node with fewer loads
        //    4. stop to move after the left loads on the largest node is no more that the expected average.
        //       We think the node is balanced, and remove it from the unbalanced node set
        //    5. calculate the balance factor. Go to step 1 if it's bigger than the threshold. Otherwise, skip
        //       the following iterations and finish the balance
        long startTime = System.currentTimeMillis();
        NavigableSet<NodeMeta> unbalancedNodeMetas = new TreeSet<>(warehouseMeta.sortedNodeMetas);
        NavigableSet<NodeMeta> balancedNodeMetas = new TreeSet<>(NodeMetaComparator.INSTANCE);
        int totalUnbalancedParalell = unbalancedNodeMetas.stream().mapToInt(meta -> meta.loadIds.size()).sum();
        // the loads that are moved between nodes, and need update the load meta after the balance
        Set<Long> movedLoadIds = new HashSet<>();
        while (unbalancedNodeMetas.size() > 1) {
            // each iteration will pick the largest node and try to make it balanced
            int expectedAvgParallelPerNode =
                    (totalUnbalancedParalell + unbalancedNodeMetas.size() - 1) / unbalancedNodeMetas.size();
            NodeMeta largestNodeMeta = unbalancedNodeMetas.pollLast();
            List<NodeMeta> changedNodeMetas = new ArrayList<>();
            // moves the loads from to the smaller nodes
            while (!unbalancedNodeMetas.isEmpty()) {
                NodeMeta smallestNodeMeta = unbalancedNodeMetas.pollFirst();
                changedNodeMetas.add(smallestNodeMeta);
                List<Long> changedLoadIds = new ArrayList<>();
                for (long loadId : largestNodeMeta.loadIds) {
                    if (largestNodeMeta.loadIds.size() - changedLoadIds.size() <= expectedAvgParallelPerNode) {
                        break;
                    }
                    if (smallestNodeMeta.loadIds.size() + changedLoadIds.size() >= expectedAvgParallelPerNode) {
                        break;
                    }
                    if (!smallestNodeMeta.loadIds.contains(loadId)) {
                        changedLoadIds.add(loadId);
                    }
                }
                changedLoadIds.forEach(largestNodeMeta.loadIds::remove);
                smallestNodeMeta.loadIds.addAll(changedLoadIds);
                movedLoadIds.addAll(changedLoadIds);
                // if the number of loads on the largest node is less than the expected average, stop to move
                if (largestNodeMeta.loadIds.size() <= expectedAvgParallelPerNode) {
                    break;
                }
            }
            unbalancedNodeMetas.addAll(changedNodeMetas);
            balancedNodeMetas.add(largestNodeMeta);
            totalUnbalancedParalell -= largestNodeMeta.loadIds.size();
            double factor = calculateBalanceFactor(balancedNodeMetas, unbalancedNodeMetas);
            if (factor <= balanceFactorThreshold) {
                break;
            }
        }

        // Update the node metas in the warehouse
        warehouseMeta.sortedNodeMetas.clear();
        warehouseMeta.sortedNodeMetas.addAll(balancedNodeMetas);
        warehouseMeta.sortedNodeMetas.addAll(unbalancedNodeMetas);
        // Update the node metas in the warehouse
        for (long loadId : movedLoadIds) {
            LoadMeta loadMeta = warehouseMeta.loadMetas.get(loadId);
            List<ComputeNode> newNodes = new ArrayList<>(loadMeta.expectParallel);
            for (NodeMeta nodeMeta : warehouseMeta.sortedNodeMetas) {
                if (nodeMeta.loadIds.contains(loadId)) {
                    newNodes.add(nodeMeta.node);
                }
            }
            List<ComputeNode> oldNodes = loadMeta.nodes;
            loadMeta.nodes = newNodes;
            LOG.info("Balance load, load id: {}, warehouse: {}, {}, num old nodes: {}, " +
                            "num new nodes: {}, old nodes: {}, new nodes: {}",
                    loadMeta.loadId, loadMeta.warehouseId, loadMeta.tableId,
                    oldNodes.size(), newNodes.size(), oldNodes, newNodes);
        }
        LOG.info("Finish balancing warehouse {}, num changed loads: {}, cost: {} ms",
                warehouseMeta.warehouseId, movedLoadIds.size(), System.currentTimeMillis() - startTime);
        logStatistics(warehouseMeta);
    }

    /**
     * Compute the balance factor of a set of nodes. This balance factor helps in
     * determining how evenly the load is distributed across the nodes.
     */
    private double calculateBalanceFactor(NavigableSet<NodeMeta> nodeMetas) {
        if (nodeMetas.size() <= 1) {
            return 0;
        }

        int minLoadNum = nodeMetas.first().loadIds.size();
        int maxLoadNum = nodeMetas.last().loadIds.size();
        if (maxLoadNum <= 1) {
            return 0;
        }
        return (maxLoadNum - minLoadNum) / (double) maxLoadNum;
    }

    /**
     * Compute the balance factor between two sets of nodes. This balance factor helps
     * in determining how evenly the load is distributed across the nodes.
     */
    private double calculateBalanceFactor(NavigableSet<NodeMeta> leftSet, NavigableSet<NodeMeta> rightSet) {
        if (leftSet.isEmpty()) {
            return calculateBalanceFactor(rightSet);
        }

        if (rightSet.isEmpty()) {
            return calculateBalanceFactor(leftSet);
        }

        int minLoadNum = Math.min(leftSet.first().loadIds.size(), rightSet.first().loadIds.size());
        int maxLoadNum = Math.max(leftSet.last().loadIds.size(), rightSet.last().loadIds.size());
        if (maxLoadNum <= 1) {
            return 0;
        }
        return (maxLoadNum - minLoadNum) / (double) maxLoadNum;
    }

    private void logStatistics(WarehouseMeta warehouseMeta) {
        StringBuilder nodeStat = new StringBuilder();
        NavigableSet<NodeMeta> nodeMetas = warehouseMeta.sortedNodeMetas;
        Map<Long, LoadMeta> loadMetas = warehouseMeta.loadMetas;
        int totalExpectParallel = loadMetas.values().stream().mapToInt(meta -> meta.expectParallel).sum();
        int totalActualParallel = loadMetas.values().stream().mapToInt(meta -> meta.nodes.size()).sum();
        int totalNodeLoadParallel = nodeMetas.stream().mapToInt(meta -> meta.loadIds.size()).sum();
        for (NodeMeta nodeMeta : nodeMetas) {
            if (nodeStat.length() > 0) {
                nodeStat.append(",");
            }
            ComputeNode node = nodeMeta.node;
            nodeStat.append("[");
            nodeStat.append(node.getId());
            nodeStat.append(",");
            nodeStat.append(node.getHost());
            nodeStat.append(",");
            nodeStat.append(nodeMeta.loadIds.size());
            nodeStat.append("]");
        }

        LOG.info("Statistics for warehouse {}, num nodes: {}, num loads: {}, total parallel expect/actual: {}/{}, " +
                        "node parallel: {}, balance factor: {}, load distribution [node id,host,#loads]: {}",
                warehouseMeta.warehouseId, nodeMetas.size(), loadMetas.size(), totalExpectParallel,
                totalActualParallel, totalNodeLoadParallel, calculateBalanceFactor(nodeMetas), nodeStat);

        if (LOG.isDebugEnabled()) {
            for (LoadMeta meta : loadMetas.values()) {
                LOG.debug("Load details, warehouse: {}, load id: {}, {}, parallel expect/actual: {}/{}, nodes: {}",
                        meta.warehouseId, meta.loadId, meta.tableId, meta.expectParallel,
                        meta.nodes.size(), meta.nodes);
            }
            for (NodeMeta nodeMeta : nodeMetas) {
                LOG.debug("Node details, warehouse: {}, node: {}, num load: {}, load ids: {}",
                        warehouseMeta.warehouseId, nodeMeta.node, nodeMeta.loadIds.size(), nodeMeta.loadIds);
            }
        }
    }

    // Note that warehouse id may be invalid after the warehouse is dropped, and an exception can be thrown
    List<ComputeNode> getAvailableNodes(long warehouseId) throws Exception {
        return LoadScanNode.getAvailableComputeNodes(warehouseId);
    }

    @VisibleForTesting
    boolean containLoad(long loadId) {
        return registeredLoadMetas.containsKey(loadId);
    }

    @VisibleForTesting
    long numScheduledTasks() {
        return numExecutedTasks.get();
    }

    @VisibleForTesting
    WarehouseMeta getWarehouseMeta(long warehouseId) {
        return warehouseMetas.get(warehouseId);
    }

    @VisibleForTesting
    void disablePeriodicalScheduleForTest() {
        while (!isPeriodicalCheckRunning.compareAndSet(false, true)) {
        }
    }

    @VisibleForTesting
    double currentLoadDiffRatio(long warehouseId) {
        WarehouseMeta whMeta = warehouseMetas.get(warehouseId);
        if (whMeta == null) {
            return 0;
        }
        return calculateBalanceFactor(whMeta.sortedNodeMetas);
    }

    // The meta for a load
    static class LoadMeta {
        final long loadId;
        final long warehouseId;
        final TableId tableId;
        // The expected number of coordinator backends
        final int expectParallel;
        // List of nodes assigned to the load. It's copy-on-write mode, and updated in the single schedule thread
        volatile List<ComputeNode> nodes;

        public LoadMeta(long loadId, long warehouseId, TableId tableId, int expectParallel) {
            this.loadId = loadId;
            this.warehouseId = warehouseId;
            this.tableId = tableId;
            this.expectParallel = expectParallel;
            this.nodes = new ArrayList<>();
        }
    }

    // The meta for a node
    static class NodeMeta  {
        ComputeNode node;
        // The load ids assigned to the node
        Set<Long> loadIds;

        public NodeMeta(ComputeNode node) {
            this.node = node;
            this.loadIds = new HashSet<>();
        }
    }

    // The comparator for the node. a node is larger if it has a larger number of loads and a larger node id
    static class NodeMetaComparator implements Comparator<NodeMeta> {

        public static final NodeMetaComparator INSTANCE = new NodeMetaComparator();

        @Override
        public int compare(NodeMeta node1, NodeMeta node2) {
            int cmp = Integer.compare(node1.loadIds.size(), node2.loadIds.size());
            if (cmp != 0) {
                return cmp;
            }
            return Long.compare(node1.node.getId(), node2.node.getId());
        }
    }

    // The meta for a warehouse
    static class WarehouseMeta {
        final long warehouseId;
        // A set of node metas sorted according to NodeMetaComparator
        final NavigableSet<NodeMeta> sortedNodeMetas;
        // Loads that have been assigned to this warehouse. load id -> LoadMeta.
        final Map<Long, LoadMeta> loadMetas;

        public WarehouseMeta(long warehouseId) {
            this.warehouseId = warehouseId;
            this.sortedNodeMetas = new TreeSet<>(NodeMetaComparator.INSTANCE);
            this.loadMetas = new HashMap<>();
        }
    }

    // The event type that trigger an assignment
    enum EventType {
        // register a load
        REGISTER_LOAD(0),
        // detect unavailable nodes
        DETECT_UNAVAILABLE_NODES(1),
        // unregister a load
        UNREGISTER_LOAD(2);

        // a smaller number is with a higher priority
        private final int priority;

        EventType(int priority) {
            this.priority = priority;
        }

        public int getPriority() {
            return priority;
        }
    }

    // The task created by an event to execute
    static class Task {

        final long taskId;
        final EventType eventType;
        final Runnable runnable;
        final CompletableFuture<Void> future;

        public Task(long taskId, EventType eventType, Runnable runnable) {
            this.taskId = taskId;
            this.eventType = eventType;
            this.runnable = runnable;
            this.future = new CompletableFuture<>();
        }

        public long getTaskId() {
            return taskId;
        }

        public EventType getScheduleType() {
            return eventType;
        }

        public Runnable getRunnable() {
            return runnable;
        }

        public void finish(Throwable throwable) {
            if (throwable == null) {
                future.complete(null);
            } else {
                future.completeExceptionally(throwable);
            }
        }
    }

    // Comparator for the task. A task is larger if it has a higher EventType priority and a smaller unique task id
    static class TaskComparator implements Comparator<Task> {

        static final TaskComparator INSTANCE = new TaskComparator();

        @Override
        public int compare(Task task1, Task task2) {
            int ret = Integer.compare(task1.getScheduleType().getPriority(), task2.getScheduleType().getPriority());
            if (ret != 0) {
                return ret;
            }

            // a smaller task id is with high priority
            return Long.compare(task1.getTaskId(), task2.getTaskId());
        }
    }
}
