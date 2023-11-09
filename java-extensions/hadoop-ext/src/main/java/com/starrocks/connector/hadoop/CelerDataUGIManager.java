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

package com.starrocks.connector.hadoop;

import com.starrocks.connector.hadoop.authentication.CachingKerberosAuthentication;
import com.starrocks.connector.hadoop.authentication.KerberosAuthentication;
import com.starrocks.connector.hadoop.authentication.KerberosConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.security.auth.Subject;

import static java.util.Objects.requireNonNull;

public class CelerDataUGIManager {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CelerDataUGIManager.class);

    static class Key {
        final String keytab;
        final String principal;

        Key(String keytab, String principal) {
            this.keytab = keytab;
            this.principal = principal;
        }

        @Override
        public int hashCode() {
            return (keytab + principal).hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof Key) {
                Key that = (Key) obj;
                return keytab.equals(that.keytab) && principal.equals(that.principal);
            }
            return false;
        }

        @Override
        public String toString() {
            return "(keytab = " + keytab + ", principal = " + principal + ")";
        }
    }

    static class Value {
        final UserGroupInformation ugi;
        final Subject subject;
        final CachingKerberosAuthentication auth;

        public Value(UserGroupInformation ugi, Subject subject, CachingKerberosAuthentication auth) {
            this.ugi = ugi;
            this.subject = subject;
            this.auth = auth;
        }
    }

    private final ConcurrentHashMap<Key, Value> cache;
    private final ExecutorService executorService;

    public CelerDataUGIManager() {
        cache = new ConcurrentHashMap<>();
        executorService = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r);
            thread.setName(CelerDataUGIManager.class.getName());
            return thread;
        });
        executorService.submit(() -> {
            this.runBackgroundJob();
        });
    }

    private Value create(String keytab, String principal) throws IOException {
        KerberosConfiguration.Builder builder = new KerberosConfiguration.Builder();
        builder.withKerberosPrincipal(principal).withKeytabLocation(keytab);
        KerberosConfiguration conf = builder.build();

        KerberosAuthentication basicAuth = new KerberosAuthentication(conf);
        CachingKerberosAuthentication auth = new CachingKerberosAuthentication(basicAuth);

        Subject subject = auth.getSubject();
        UserGroupInformation ugi = UserGroupInformation.getUGIFromSubject(subject);
        return new Value(ugi, subject, auth);
    }

    public UserGroupInformation getOrCreate(String keytab, String principal) throws IOException {
        requireNonNull(keytab);
        requireNonNull(principal);
        Key k = new Key(keytab, principal);
        Value v = cache.get(k);
        if (v != null) {
            return v.ugi;
        }
        v = create(keytab, principal);
        cache.putIfAbsent(k, v);
        return v.ugi;
    }

    public void runBackgroundJob() {
        final int CHECK_INTERVAL_MS = 300 * 1000; // 5 min;
        while (true) {
            refreshTickets();
            try {
                Thread.sleep(CHECK_INTERVAL_MS);
            } catch (InterruptedException e) {
                break;
            }
        }
        LOGGER.info("run background job quits");
    }

    public void refreshTickets() {
        LOGGER.debug("refresh tickets");
        cache.forEach((key, value) -> {
            CachingKerberosAuthentication auth = value.auth;
            auth.reauthenticateIfSoonWillBeExpired();
        });
    }
}
