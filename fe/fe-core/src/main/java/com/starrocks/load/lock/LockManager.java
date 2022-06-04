// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.lock;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

// LockManager with nonblocking tryLock
// a blocking lock api maybe will be added in the future
public class LockManager {
    private static final Logger LOG = LogManager.getLogger(LockManager.class);
    private class Node {
        private boolean isExclusive;
        private Map<Long, Node> children;
        private Map<Long, LockTarget.TargetContext> contexts;
        private final ReentrantLock lock = new ReentrantLock();

        Node() {
            this.isExclusive = false;
            this.children = null;
            this.contexts = null;
        }

        public boolean tryLock(Long[] pathIds, LockTarget.TargetContext targetContext, boolean exclusive) {
            return tryLock(pathIds, 0, targetContext, exclusive);
        }

        private boolean isChildExclusiveLocked() {
            if (!hasChild()) {
                return false;
            }
            for (Node child : children.values()) {
                if (child.isExclusive || child.isChildExclusiveLocked()) {
                    return true;
                }
            }
            return false;
        }

        public boolean tryLock(Long[] pathIds, int index, LockTarget.TargetContext targetContext, boolean exclusive) {
            if (!lock.tryLock()) {
                return false;
            }
            try {
                if (index == pathIds.length) {
                    if (this.isExclusive) {
                        return false;
                    }
                    if (exclusive && hasLock()) {
                        return false;
                    }
                    if (exclusive && hasChild()) {
                        // if node has child locked, can not be locked exclusive
                        return false;
                    }
                    if (isChildExclusiveLocked()) {
                        // if child has been exclusive locked, return false
                        return false;
                    }

                    set(targetContext, exclusive);
                    return true;
                }
                Node child;
                if (children == null) {
                    children = Maps.newHashMapWithExpectedSize(3);
                    child = new Node();
                    children.put(pathIds[index], child);
                } else {
                    child = children.get(pathIds[index]);
                    if (child == null) {
                        child = new Node();
                        children.put(pathIds[index], child);
                    } else if (child.isExclusive) {
                        return false;
                    }
                }
                return child.tryLock(pathIds, index + 1, targetContext, exclusive);
            } finally {
                lock.unlock();
            }
        }

        public void unlock(Long[] pathids, LockTarget.TargetContext targetContext) {
            unlock(pathids, 0, targetContext);
        }

        private void unlock(Long[] pathids, int index, LockTarget.TargetContext targetContext) {
            lock.lock();
            try {
                if (index == pathids.length) {
                    if (hasLock()) {
                        contexts.remove(targetContext.getRelatedId());
                    }
                    return;
                }
                Node child = children == null ? null : children.get(pathids[index]);
                if (child == null) {
                    LOG.warn("invalid lock state. lock path id:{}, index:{}, txnId:{}. lockTimestamp:",
                            Strings.join(Arrays.stream(pathids).iterator(), '.'), index, targetContext.getRelatedId(),
                            targetContext.getLockTimestamp());
                    return;
                }
                child.unlock(pathids, index + 1, targetContext);
                if (!child.hasLock() && !child.hasChild()) {
                    children.remove(pathids[index]);
                }
            } finally {
                lock.unlock();
            }
        }

        private boolean hasLock() {
            return contexts != null && !contexts.isEmpty();
        }

        private boolean hasChild() {
            return children != null && !children.isEmpty();
        }

        private void set(LockTarget.TargetContext context, boolean isExclusive) {
            this.isExclusive = isExclusive;
            if (contexts == null) {
                contexts = Maps.newHashMapWithExpectedSize(3);
            }
            contexts.put(context.getRelatedId(), context);
        }
    }

    private Node root = new Node();

    // null if failed
    public Lock tryLock(LockTarget lockTarget, LockMode mode) {
        return tryLockInternal(lockTarget, mode);
    }

    public void unlock(Lock lock) {
        LOG.debug("release lock for {} with mode {}, relatedId:{}",
                lock.getLockTarget().getName(), lock.getLockMode(), lock.getLockTarget().getTargetContext().getRelatedId());
        root.unlock(lock.getLockTarget().getPathIds(), lock.getLockTarget().getTargetContext());
    }

    // lock multi target simultaneously
    public List<Lock> tryLock(List<LockTargetDesc> lockTargets) {
        return tryLockInternal(lockTargets);
    }

    public void unlock(List<Lock> locks) {
        for (Lock lock : locks) {
            unlock(lock);
        }
    }

    private Lock tryLockInternal(LockTarget lockTarget, LockMode mode) {
        LOG.debug("Acquiring lock for {} with mode {}, relatedId:{}",
                lockTarget.getName(), mode, lockTarget.getTargetContext().getRelatedId());
        if (root.tryLock(lockTarget.getPathIds(), lockTarget.getTargetContext(), mode == LockMode.EXCLUSIVE)) {
            return new Lock(lockTarget, mode);
        }
        return null;
    }

    private List<Lock> tryLockInternal(List<LockTargetDesc> lockTargets) {
        sortLocks(lockTargets);
        if (LOG.isDebugEnabled()) {
            for (LockTargetDesc targetDesc : lockTargets) {
                LOG.debug("Acquiring lock for {} with mode {}",
                        targetDesc.getLockTarget().getName(), targetDesc.getLockMode());
            }
        }
        // return null if lock failed
        List<Lock> locks = Lists.newArrayListWithCapacity(lockTargets.size());
        for (LockTargetDesc targetDesc : lockTargets) {
            Lock lock = tryLockInternal(targetDesc.getLockTarget(), targetDesc.getLockMode());
            if (lock == null) {
                unlock(locks);
                return null;
            }
            locks.add(lock);
        }
        return locks;
    }

    private void sortLocks(List<LockTargetDesc> lockTargets) {
        Collections.sort(lockTargets, new Comparator<LockTargetDesc>() {
            @Override
            public int compare(LockTargetDesc o1, LockTargetDesc o2) {
                int cmp = 0;
                int size = Math.min(o1.getLockTarget().getPathIds().length, o2.getLockTarget().getPathIds().length);
                for (int i = 0; i < size; ++i) {
                    cmp = o1.getLockTarget().getPathIds()[i].compareTo(o2.getLockTarget().getPathIds()[i]);
                    if (cmp != 0) {
                        return cmp;
                    }
                }
                cmp = o1.getLockTarget().getPathIds().length - o2.getLockTarget().getPathIds().length;
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
}
