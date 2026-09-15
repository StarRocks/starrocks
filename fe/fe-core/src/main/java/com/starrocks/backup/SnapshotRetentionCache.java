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

package com.starrocks.backup;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Remembers the retention policy read out of each snapshot's job info file.
 *
 * <p>This is memoization rather than a cache with a coherence problem to solve. A job info file is
 * written once and never rewritten, so its contents cannot drift. What can change is which backup
 * sits under a label, because the label may have been dropped and backed up again since -- by this
 * cluster or by another one -- and that is why every read confirms the remembered entry against the
 * backup timestamp the repository reports now. What is saved is the download and parse of the file,
 * not the listing that names it.
 *
 * <p>Every FE fills its own: the leader through ttl cleanup, the others through SHOW SNAPSHOT, which
 * runs where the client connected.
 */
public class SnapshotRetentionCache {
    private static final Logger LOG = LogManager.getLogger(SnapshotRetentionCache.class);

    private static final int MAX_ENTRIES = 10000;

    private final Cache<Key, SnapshotRetention> cache = Caffeine.newBuilder().maximumSize(MAX_ENTRIES).build();

    /**
     * The retention {@code label} carries in {@code repo}, or null when the repository does not
     * answer for it: there is no snapshot under that label, its job info file is not there yet (a
     * backup is running, or was interrupted before its last upload), or the read failed.
     *
     * <p>Costs one listing even on a hit: it is what says which backup sits under the label right
     * now, and therefore what makes the remembered entry safe to hand back.
     */
    public SnapshotRetention get(Repository repo, String label) {
        RemoteFile jobInfoFile = repo.findJobInfoFile(label);
        return jobInfoFile == null ? null : get(repo, label, jobInfoFile);
    }

    /** {@link #get(Repository, String)} for a caller that has already listed the job info file. */
    public SnapshotRetention get(Repository repo, String label, RemoteFile jobInfoFile) {
        String backupTimestamp = Repository.jobInfoBackupTimestamp(jobInfoFile);
        Key key = new Key(repo.getId(), label);
        SnapshotRetention cached = cache.getIfPresent(key);
        if (cached != null && backupTimestamp.equals(cached.getBackupTimestamp())) {
            return cached;
        }

        List<BackupJobInfo> infos = Lists.newArrayList();
        Status st = repo.readJobInfoFile(label, jobInfoFile, infos);
        if (!st.ok() || infos.isEmpty()) {
            LOG.warn("failed to read the job info file of snapshot {} (timestamp {}) in repo {}: {}",
                    label, backupTimestamp, repo.getName(), st.getErrMsg());
            return null;
        }

        BackupJobInfo info = infos.get(0);
        SnapshotRetention retention = new SnapshotRetention(backupTimestamp, info.clusterId,
                info.finishTime, info.ttl, info.expireTime);
        cache.put(key, retention);
        return retention;
    }

    /** Forgets what was remembered for {@code label}, for a caller that has just deleted it. */
    public void invalidate(long repoId, String label) {
        cache.invalidate(new Key(repoId, label));
    }

    /** What one snapshot's job info file says about who owns it and how long it is kept. */
    public static class SnapshotRetention {
        private final String backupTimestamp;
        private final Integer clusterId;
        private final Long finishTime;
        private final String ttl;
        private final Long expireTime;

        public SnapshotRetention(String backupTimestamp, Integer clusterId, Long finishTime, String ttl,
                                 Long expireTime) {
            this.backupTimestamp = backupTimestamp;
            this.clusterId = clusterId;
            this.finishTime = finishTime;
            this.ttl = ttl;
            this.expireTime = expireTime;
        }

        public String getBackupTimestamp() {
            return backupTimestamp;
        }

        public Integer getClusterId() {
            return clusterId;
        }

        /** When the backup wrapped up, which is what {@link #getExpireTime} was measured from. */
        public Long getFinishTime() {
            return finishTime;
        }

        public String getTtl() {
            return ttl;
        }

        public Long getExpireTime() {
            return expireTime;
        }

        /**
         * Whether the snapshot was written by the given cluster. A snapshot from before this field
         * existed belongs to nobody, which is what keeps automatic cleanup off it.
         */
        public boolean isOwnedBy(int clusterId) {
            return this.clusterId != null && this.clusterId == clusterId;
        }

        public boolean isExpired(long now) {
            return expireTime != null && expireTime <= now;
        }
    }

    private record Key(long repoId, String label) {
    }
}
