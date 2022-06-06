// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.lock;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.TreeNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

// LockManager with nonblocking tryLock
public class LockManager {
    private static final Logger LOG = LogManager.getLogger(LockManager.class);
    private class Node extends TreeNode<Node> {
        private long pathId;
        private boolean isExclusive;
        private Map<Long, LockContext> contexts;

        Node() {
            this.pathId = -1;
            this.isExclusive = false;
            this.contexts = null;
        }

        Node(long pathId) {
            this.pathId = pathId;
            this.isExclusive = false;
            this.contexts = null;
        }

        public long getPathId() {
            return pathId;
        }

        public boolean isExclusive() {
            return isExclusive;
        }

        public void removeContext(long id) {
            contexts.remove(id);
        }

        public boolean hasLock() {
            return contexts != null && !contexts.isEmpty();
        }

        private void set(LockContext context) {
            if (contexts == null) {
                contexts = Maps.newHashMapWithExpectedSize(3);
            }
            contexts.put(context.getRelatedId(), context);
            this.isExclusive = context.getLockMode() == LockMode.EXCLUSIVE;
        }
    }

    private Node root = new Node();
    private final ReentrantLock managerLock = new ReentrantLock();

    // null if failed
    public Lock tryLock(LockTarget lockTarget) {
        return tryLockInternal(lockTarget);
    }

    public void unlock(Lock lock) {
        LOG.debug("release lock for {} with mode {}, relatedId:{}",
                Arrays.stream(lock.getPathIds()).map(pathId -> pathId.toString()).collect(Collectors.joining(".")),
                lock.getLockMode(), lock.getLockContextId());
        unlockInternal(lock);
    }

    // lock multi target simultaneously
    public List<Lock> tryLock(List<LockTarget> lockTargets) {
        return tryLockInternal(lockTargets);
    }

    public void unlock(List<Lock> locks) {
        for (Lock lock : locks) {
            unlock(lock);
        }
    }

    private Lock tryLockInternal(LockTarget lockTarget) {
        LOG.debug("Acquiring lock for {}, relatedId:{}",
                lockTarget.getName(), lockTarget.getTargetContext().getRelatedId());
        managerLock.lock();
        try {
            boolean locked = tryLockInternal(root, lockTarget.getPathIds(), 0, lockTarget.getTargetContext());
            if (locked) {
                return new Lock(lockTarget.getPathIds(), lockTarget.getLockMode(),
                        lockTarget.getTargetContext().getRelatedId());
            }
            return null;
        } finally {
            managerLock.unlock();
        }
    }

    private boolean tryLockInternal(Node node, Long[] pathIds, int index, LockContext lockContext) {
        if (pathIds == null || pathIds.length == 0) {
            return false;
        }
        if (index < 0) {
            return false;
        }
        if (index == pathIds.length) {
            if (node.isExclusive()) {
                return false;
            }
            if (lockContext.isExclusive() && node.hasLock()) {
                return false;
            }
            if (lockContext.isExclusive() && node.hasChildren()) {
                // if node has child locked, can not be locked exclusive
                return false;
            }
            if (node.contains((Predicate<Node>) n -> n.isExclusive())) {
                // if child has been exclusive locked, return false
                return false;
            }
            node.set(lockContext);
            return true;
        }
        long childPathId = pathIds[index];
        Node child;
        if (node.hasChildren()) {
            Optional<Node> optional = node.getChildren().stream().filter(c -> c.getPathId() == childPathId).findFirst();
            if (!optional.isPresent()) {
                child = new Node(childPathId);
                node.addChild(child);
            } else {
                child = optional.get();
                if (child.isExclusive()) {
                    // if child exists and is exclusive locked, return false
                    return false;
                }
                if (lockContext.isExclusive() && child.hasLock()) {
                    // is parent has locks, the request for exclusive lock of children will fail
                    return false;
                }
            }
        } else {
            child = new Node(childPathId);
            node.addChild(child);
        }
        return tryLockInternal(child, pathIds, index + 1, lockContext);
    }

    private List<Lock> tryLockInternal(List<LockTarget> lockTargets) {
        sortLocks(lockTargets);
        if (LOG.isDebugEnabled()) {
            for (LockTarget target : lockTargets) {
                LOG.debug("Acquiring lock for {} with mode {}",
                        target.getName(), target.getLockMode());
            }
        }
        // return null if lock failed
        List<Lock> locks = Lists.newArrayListWithCapacity(lockTargets.size());
        for (LockTarget target : lockTargets) {
            Lock lock = tryLockInternal(target);
            if (lock == null) {
                unlock(locks);
                return null;
            }
            locks.add(lock);
        }
        return locks;
    }

    private void sortLocks(List<LockTarget> lockTargets) {
        Collections.sort(lockTargets, new Comparator<LockTarget>() {
            @Override
            public int compare(LockTarget o1, LockTarget o2) {
                int cmp = 0;
                int size = Math.min(o1.getPathIds().length, o2.getPathIds().length);
                for (int i = 0; i < size; ++i) {
                    cmp = o1.getPathIds()[i].compareTo(o2.getPathIds()[i]);
                    if (cmp != 0) {
                        return cmp;
                    }
                }
                cmp = o1.getPathIds().length - o2.getPathIds().length;
                if (cmp == 0) {
                    if (o1.getLockMode() == o2.getLockMode()) {
                        return cmp;
                    }
                    if (o1.getLockMode() == LockMode.EXCLUSIVE) {
                        return -1;
                    }
                    return 1;
                }
                return cmp;
            }
        });
    }

    private void unlockInternal(Lock lock) {
        managerLock.lock();
        try {
            unlockInternal(root, lock.getPathIds(), 0, lock.getLockContextId());
        } finally {
            managerLock.unlock();
        }
    }

    private void unlockInternal(Node node, Long[] pathIds, int index, long lockContextId) {
        if (pathIds == null || pathIds.length == 0) {
            return;
        }
        if (index < 0) {
            return;
        }
        if (index == pathIds.length) {
            if (node.hasLock()) {
                node.removeContext(lockContextId);
            }
            return;
        }
        long childPathId = pathIds[index];
        Node child = null;
        if (node.hasChildren()) {
            Optional<Node> optional = node.getChildren().stream().filter(c -> c.getPathId() == childPathId).findFirst();
            if (optional.isPresent()) {
                child = optional.get();
            }
        }
        if (child == null) {
            LOG.warn("invalid lock state. path id:{}, node id:{}, index:{}, txnId:{}.",
                    Strings.join(Arrays.stream(pathIds).iterator(), '.'), node.getPathId(), index, lockContextId);
            return;
        }
        unlockInternal(child, pathIds, index + 1, lockContextId);
        if (!child.hasLock() && !child.hasChildren()) {
            node.reomveChild(child);
        }
    }
}
