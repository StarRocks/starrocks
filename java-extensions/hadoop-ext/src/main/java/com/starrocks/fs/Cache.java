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

package com.starrocks.fs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_AUTOMATIC_CLOSE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_AUTOMATIC_CLOSE_KEY;
import static org.apache.hadoop.fs.FileSystem.SHUTDOWN_HOOK_PRIORITY;

/**
 * Caching FileSystem objects
 */
public class Cache {
    public static final Log LOG = LogFactory.getLog(Cache.class);

    private final Cache.ClientFinalizer clientFinalizer = new Cache.ClientFinalizer();

    private final Map<Cache.Key, FileSystem> map = new HashMap<Cache.Key, FileSystem>();
    private final Set<Cache.Key> toAutoClose = new HashSet<Cache.Key>();
    /**
     * A variable that makes all objects in the cache unique
     */
    private static AtomicLong unique = new AtomicLong(1);

    static Cache.Key buildCacheKey(URI uri, Configuration conf) throws IOException {
        return new Cache.Key(uri, conf);
    }

    FileSystem get(URI uri, Configuration conf) throws IOException {
        Cache.Key key = new Cache.Key(uri, conf);
        return getInternal(uri, conf, key);
    }

    FileSystem get(URI uri, Configuration conf, Cache.Key key) throws IOException {
        return getInternal(uri, conf, key);
    }

    /**
     * The objects inserted into the cache using this method are all unique
     */
    FileSystem getUnique(URI uri, Configuration conf) throws IOException {
        Cache.Key key = new Cache.Key(uri, conf, unique.getAndIncrement());
        return getInternal(uri, conf, key);
    }

    private FileSystem getInternal(URI uri, Configuration conf, Cache.Key key) throws IOException {
        FileSystem fs;
        synchronized (this) {
            fs = map.get(key);
        }
        if (fs != null) {
            return fs;
        }

        fs = CacheFileSystem.createRealFileSystem(uri, conf);
        synchronized (this) { // refetch the lock again
            FileSystem oldfs = map.get(key);
            if (oldfs != null) { // a file system is created while lock is releasing
                fs.close(); // close the new file system
                return oldfs;  // return the old file system
            }

            // now insert the new file system into the map
            if (map.isEmpty()
                    && !ShutdownHookManager.get().isShutdownInProgress()) {
                ShutdownHookManager.get().addShutdownHook(clientFinalizer, SHUTDOWN_HOOK_PRIORITY);
            }
            map.put(key, fs);
            if (conf.getBoolean(
                    FS_AUTOMATIC_CLOSE_KEY, FS_AUTOMATIC_CLOSE_DEFAULT)) {
                toAutoClose.add(key);
            }
            return fs;
        }
    }

    synchronized void remove(Cache.Key key, FileSystem fs) {
        FileSystem cachedFs = map.remove(key);
        if (fs == cachedFs) {
            toAutoClose.remove(key);
        } else if (cachedFs != null) {
            map.put(key, cachedFs);
        }
    }

    synchronized void closeAll() throws IOException {
        closeAll(false);
    }

    /**
     * Close all FileSystem instances in the Cache.
     *
     * @param onlyAutomatic only close those that are marked for automatic closing
     */
    synchronized void closeAll(boolean onlyAutomatic) throws IOException {
        List<IOException> exceptions = new ArrayList<IOException>();

        // Make a copy of the keys in the map since we'll be modifying
        // the map while iterating over it, which isn't safe.
        List<Cache.Key> keys = new ArrayList<Cache.Key>();
        keys.addAll(map.keySet());

        for (Cache.Key key : keys) {
            final FileSystem fs = map.get(key);

            if (onlyAutomatic && !toAutoClose.contains(key)) {
                continue;
            }

            //remove from cache
            map.remove(key);
            toAutoClose.remove(key);

            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException ioe) {
                    exceptions.add(ioe);
                }
            }
        }

        if (!exceptions.isEmpty()) {
            throw MultipleIOException.createIOException(exceptions);
        }
    }

    private class ClientFinalizer implements Runnable {
        @Override
        public synchronized void run() {
            try {
                closeAll(true);
            } catch (IOException e) {
                LOG.info("Cache.closeAll() threw an exception:\n" + e);
            }
        }
    }

    synchronized void closeAll(UserGroupInformation ugi) throws IOException {
        List<FileSystem> targetFSList = new ArrayList<FileSystem>();
        //Make a pass over the list and collect the filesystems to close
        //we cannot close inline since close() removes the entry from the Map
        for (Map.Entry<Cache.Key, FileSystem> entry : map.entrySet()) {
            final Cache.Key key = entry.getKey();
            final FileSystem fs = entry.getValue();
            if (ugi.equals(key.ugi) && fs != null) {
                targetFSList.add(fs);
            }
        }
        List<IOException> exceptions = new ArrayList<IOException>();
        //now make a pass over the target list and close each
        for (FileSystem fs : targetFSList) {
            try {
                fs.close();
            } catch (IOException ioe) {
                exceptions.add(ioe);
            }
        }
        if (!exceptions.isEmpty()) {
            throw MultipleIOException.createIOException(exceptions);
        }
    }

    /**
     * Cache.Key
     */
    static class Key {
        final String scheme;
        final String authority;
        final UserGroupInformation ugi;
        final long unique;   // an artificial way to make a key unique

        final String cloudConf;

        Key(URI uri, Configuration conf) throws IOException {
            this(uri, conf, 0);
        }

        Key(URI uri, Configuration conf, long unique) throws IOException {
            scheme = uri.getScheme() == null ?
                    "" : StringUtils.toLowerCase(uri.getScheme());
            authority = uri.getAuthority() == null ?
                    "" : StringUtils.toLowerCase(uri.getAuthority());
            this.unique = unique;

            this.ugi = UserGroupInformation.getCurrentUser();

            this.cloudConf = HadoopExt.getCloudConfString(conf);
        }

        @Override
        public int hashCode() {
            return (scheme + authority + cloudConf).hashCode() + ugi.hashCode() + (int) unique;
        }

        static boolean isEqual(Object a, Object b) {
            return a == b || (a != null && a.equals(b));
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof Key) {
                Key that = (Key) obj;
                return isEqual(this.scheme, that.scheme)
                        && isEqual(this.authority, that.authority)
                        && isEqual(this.cloudConf, that.cloudConf)
                        && isEqual(this.ugi, that.ugi)
                        && (this.unique == that.unique);
            }
            return false;
        }

        @Override
        public String toString() {
            return "(ugi = " + ugi.toString() + ", cloudConf = " + cloudConf + ")@" + scheme + "://" + authority;
        }
    }
}
