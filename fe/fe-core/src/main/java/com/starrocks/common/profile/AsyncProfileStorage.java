// This file is made available under Elastic License 2.0.
package com.starrocks.common.profile;

import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @Author ikaruga4600
 * @Date 2021/9/15 15:04
 */
public abstract class AsyncProfileStorage implements Runnable {
    private static final Logger LOG = LogManager.getLogger(AsyncProfileStorage.class);

    protected final ArrayBlockingQueue<Pair<String, String>> profileQueue;
    private int profileQueueSize = 1000;

    public AsyncProfileStorage() {
        this.profileQueue = new ArrayBlockingQueue<>(profileQueueSize);
        Thread t = new Thread(this);
        t.start();
    }

    public static AsyncProfileStorage getInstance() {
        switch (Config.profile_storage_type) {
            case "kafka":
                return new KafkaProfileStorage();
            default:
                throw new RuntimeException("init ProfileStorage failed, unknown type:"
                        + Config.profile_storage_type);
        }
    }

    public void collect(String queryId, String profileContent) {
        if (profileQueue.size() >= profileQueueSize) {
            LOG.warn("profile queue is full, discard query {}", queryId);
            return;
        }
        profileQueue.offer(new Pair<>(queryId, profileContent));
    }

    protected abstract void persist(List<Pair<String, String>> profiles);

    @Override
    public void run() {
        while (true) {
            List<Pair<String, String>> batchProfiles = new ArrayList<>();
            Pair<String, String> profile;
            boolean hasMore = false;
            while ((profile = profileQueue.poll()) != null) {
                batchProfiles.add(profile);
                if (batchProfiles.size() == Config.profile_storage_batch_size) {
                    hasMore = true;
                    break;
                }
            }
            try {
                persist(batchProfiles);
            } catch (Exception e) {
                LOG.error("uncaught profile persist failed", e);
            }
            if (!hasMore) {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
