// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.util;

import com.google.common.collect.Sets;
import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class PrintableMap<K, V> {
    private Map<K, V> map;
    private String keyValueSaperator;
    private boolean withQuotation;
    private boolean wrap;
    private boolean hidePassword;
    private String entryDelimiter = ",";

    public static final Set<String> SENSITIVE_KEY;

    static {
        SENSITIVE_KEY = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        SENSITIVE_KEY.add("password");
        SENSITIVE_KEY.add("kerberos_keytab_content");
        SENSITIVE_KEY.add("bos_secret_accesskey");
        SENSITIVE_KEY.add("fs.s3a.access.key");
        SENSITIVE_KEY.add("fs.s3a.secret.key");
        SENSITIVE_KEY.add("fs.oss.accessKeyId");
        SENSITIVE_KEY.add("fs.oss.accessKeySecret");
        SENSITIVE_KEY.add("fs.cosn.userinfo.secretId");
        SENSITIVE_KEY.add("fs.cosn.userinfo.secretKey");
        SENSITIVE_KEY.add("property.sasl.password");
        SENSITIVE_KEY.add("broker.password");
        SENSITIVE_KEY.add("confluent.schema.registry.url");
    }

    public PrintableMap(Map<K, V> map, String keyValueSaperator,
                        boolean withQuotation, boolean wrap, String entryDelimiter) {
        this.map = map;
        this.keyValueSaperator = keyValueSaperator;
        this.withQuotation = withQuotation;
        this.wrap = wrap;
        this.hidePassword = false;
        this.entryDelimiter = entryDelimiter;
    }

    public PrintableMap(Map<K, V> map, String keyValueSaperator,
                        boolean withQuotation, boolean wrap) {
        this(map, keyValueSaperator, withQuotation, wrap, ",");
    }

    public PrintableMap(Map<K, V> map, String keyValueSaperator,
                        boolean withQuotation, boolean wrap, boolean hidePassword) {
        this(map, keyValueSaperator, withQuotation, wrap);
        this.hidePassword = hidePassword;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<K, V>> iter = map.entrySet().iterator();

        Escapers.Builder builder = Escapers.builder();
        builder.addEscape('"', "\\\"");
        Escaper escaper = builder.build();

        while (iter.hasNext()) {
            Map.Entry<K, V> entry = iter.next();
            if (withQuotation) {
                sb.append("\"");
            }
            sb.append(entry.getKey());
            if (withQuotation) {
                sb.append("\"");
            }
            sb.append(" ").append(keyValueSaperator).append(" ");
            if (withQuotation) {
                sb.append("\"");
            }
            if (hidePassword && SENSITIVE_KEY.contains(entry.getKey())) {
                sb.append("***");
            } else {
                String text = entry.getValue().toString();
                if (withQuotation) {
                    text = escaper.escape(text);
                }
                sb.append(text);
            }
            if (withQuotation) {
                sb.append("\"");
            }
            if (iter.hasNext()) {
                sb.append(entryDelimiter);
                if (wrap) {
                    sb.append("\n");
                } else {
                    sb.append(" ");
                }
            }
        }
        return sb.toString();
    }
}
