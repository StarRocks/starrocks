// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.lock;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.starrocks.common.TreeNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.spark_project.jetty.util.ConcurrentHashSet;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LevelLockManager {
    private static final Logger LOG = LogManager.getLogger(LevelLockManager.class);

    private class Node extends TreeNode<Node> {
        private long pathId;
        // used for tryLock
        private boolean isExclusive;
        private ReentrantReadWriteLock readWriteLock;
        private ReentrantLock dataLock;
        private Set<LevelLock> lockSet;

        Node() {
            this.pathId = -1;
            this.isExclusive = false;
            this.readWriteLock = new ReentrantReadWriteLock();
            this.dataLock = new ReentrantLock();
        }

        Node(long pathId) {
            this.pathId = pathId;
            this.isExclusive = false;
            this.readWriteLock = new ReentrantReadWriteLock();
            this.dataLock = new ReentrantLock();
        }

        public long getPathId() {
            return pathId;
        }

        public boolean isExclusive() {
            return isExclusive;
        }

        public boolean hasLock() {
            return lockSet != null && !lockSet.isEmpty();
        }

        private void set(LevelLock lock, boolean isExclusive) {
            if (this.lockSet == null) {
                this.lockSet = new ConcurrentHashSet<>();
            }
            this.lockSet.add(lock);
            this.isExclusive = isExclusive;
        }

        private void unset(LevelLock lock) {
            this.lockSet.remove(lock);
        }
    }

    private Node root = new Node();

    public void writeLock(LevelLock lock) {
        if (lock.getPathIds() == null || lock.getPathIds().length == 0) {
            throw new RuntimeException("lock failed for invalid arguments");
        }
        lockInternal(lock, root, 0, true);
    }

    public void readLock(LevelLock lock) {
        if (lock.getPathIds() == null || lock.getPathIds().length == 0) {
            throw new RuntimeException("lock failed for invalid arguments");
        }
        lockInternal(lock, root, 0, false);
    }

    private void lockInternal(LevelLock lock, Node node, int index, boolean isExclusive) {
        if (index == lock.getPathIds().length) {
            // acquire write lock
            if (isExclusive) {
                node.readWriteLock.writeLock().lock();
            } else {
                node.readWriteLock.readLock().lock();
            }
            node.set(lock, isExclusive);
            return;
        }
        // acquire read lock of node
        node.readWriteLock.readLock().lock();
        long childPathId = lock.getPathIds()[index];
        Node child;
        node.dataLock.lock();
        try {
            if (node.hasChildren()) {
                Optional<Node> optional = node.getChildren().stream()
                        .filter(c -> c.getPathId() == childPathId).findFirst();
                if (!optional.isPresent()) {
                    child = new Node(childPathId);
                    node.addChild(child);
                } else {
                    child = optional.get();
                }
            } else {
                child = new Node(childPathId);
                node.addChild(child);
            }
        } finally {
            node.dataLock.unlock();
        }
        lockInternal(lock, child, index + 1, isExclusive);
    }

    public boolean tryWriteLock(LevelLock lock) {
        return tryLockInternal(lock, true);
    }

    public boolean tryReadLock(LevelLock lock) {
        return tryLockInternal(lock, false);
    }

    public boolean tryLockInternal(LevelLock lock, boolean isExclusive) {
        if (lock.getPathIds() == null || lock.getPathIds().length == 0) {
            return false;
        }
        List<Node> lockedNodes = Lists.newArrayList();
        boolean acquired = tryLockInternal(lock, root, 0, lockedNodes, isExclusive);
        if (!acquired) {
            for (int i = lockedNodes.size() - 1; i >= 0; i--) {
                lockedNodes.get(i).readWriteLock.readLock().unlock();
            }
        }
        return acquired;
    }

    public boolean tryLockInternal(LevelLock levelLock, Node node, int index, List<Node> lockedNodes, boolean isExclusive) {
        if (index == levelLock.getPathIds().length) {
            if (node.isExclusive) {
                return false;
            }
            if (isExclusive && node.hasLock()) {
                return false;
            }
            if (isExclusive && node.hasChildren()) {
                return false;
            }
            if (!isExclusive && node.contains((Predicate<Node>) n -> n.isExclusive())) {
                // if it is a read lock, but the children have been write locked, return false
                return false;
            }
            // acquire write lock
            boolean acquired = false;
            if (isExclusive) {
                acquired = node.readWriteLock.writeLock().tryLock();
            } else {
                acquired = node.readWriteLock.readLock().tryLock();
            }
            if (acquired) {
                node.set(levelLock, isExclusive);
            }
            return acquired;
        }
        // acquire read lock of node
        boolean ret = node.readWriteLock.readLock().tryLock();
        if (!ret) {
            return false;
        }
        lockedNodes.add(node);
        long childPathId = levelLock.getPathIds()[index];
        Node child;
        node.dataLock.lock();
        try {
            if (node.hasChildren()) {
                Optional<Node> optional = node.getChildren().stream().filter(c -> c.getPathId() == childPathId).findFirst();
                if (!optional.isPresent()) {
                    child = new Node(childPathId);
                    node.addChild(child);
                } else {
                    child = optional.get();
                    if (child.isExclusive) {
                        return false;
                    }
                    if (isExclusive && child.hasLock()) {
                        return false;
                    }
                }
            } else {
                child = new Node(childPathId);
                node.addChild(child);
            }
        } finally {
            node.dataLock.unlock();
        }
        return tryLockInternal(levelLock, child, index + 1, lockedNodes, isExclusive);
    }

    public void unlockWriteLock(LevelLock lock) {
        unlockLockInternal(lock, root, 0, true);
    }

    public void unlockReadLock(LevelLock lock) {
        unlockLockInternal(lock, root, 0, false);
    }

    private boolean unlockLockInternal(LevelLock lock, Node node, int index, boolean isExclusive) {
        if (lock.getPathIds() == null || lock.getPathIds().length == 0) {
            return false;
        }
        if (index == lock.getPathIds().length) {
            if (node.lockSet.contains(lock)) {
                node.unset(lock);
                if (isExclusive) {
                    node.readWriteLock.writeLock().unlock();
                } else {
                    node.readWriteLock.readLock().unlock();
                }
                return true;
            } else {
                return false;
            }
        }
        long childPathId = lock.getPathIds()[index];
        Node child = null;
        node.dataLock.lock();
        try {
            if (node.hasChildren()) {
                Optional<Node> optional = node.getChildren().stream()
                        .filter(c -> c.getPathId() == childPathId).findFirst();
                if (optional.isPresent()) {
                    child = optional.get();
                }
            }
        } finally {
            node.dataLock.unlock();
        }
        if (child == null) {
            LOG.warn("invalid lock state. path id:{}, node id:{}, index:{}",
                    Strings.join(Arrays.stream(lock.getPathIds()).iterator(), '.'), node.getPathId(), index);
            return false;
        }
        boolean ret = unlockLockInternal(lock, child, index + 1, isExclusive);
        if (ret) {
            node.readWriteLock.readLock().unlock();
            node.dataLock.lock();
            try {
                if (!child.hasLock() && !child.hasChildren()) {
                    node.reomveChild(child);
                }
            } finally {
                node.dataLock.unlock();
            }
        }
        return ret;
    }
}
