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

package com.starrocks.authentication;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.StarRocksFE;
import com.starrocks.common.DdlException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FileGroupProvider extends GroupProvider {
    private static final Logger LOG = LogManager.getLogger(FileGroupProvider.class);

    public static final String TYPE = "file";

    public static final String GROUP_FILE_URL = "group_file_url";

    public static final Set<String> REQUIRED_PROPERTIES = new HashSet<>(List.of(
            FileGroupProvider.GROUP_FILE_URL));

    private final Map<String, Set<String>> userGroups;

    public FileGroupProvider(String name, Map<String, String> properties) {
        super(name, properties);
        this.userGroups = new HashMap<>();
    }

    @Override
    public void init() throws DdlException {
        String groupFileUrl = properties.get(GROUP_FILE_URL);

        try {
            InputStream fileInputStream = null;
            try {
                fileInputStream = getPath(groupFileUrl);

                String s = readInputStreamToString(fileInputStream, StandardCharsets.UTF_8);
                for (String line : s.split("\r?\n")) {
                    if (line.trim().isEmpty()) {
                        continue;
                    }

                    String[] parts = line.split(":");
                    String groupName = parts[0];
                    String[] users = parts[1].split(",");

                    for (String user : users) {
                        user = user.trim();
                        userGroups.putIfAbsent(user, new HashSet<>());
                        userGroups.get(user).add(groupName);
                    }
                }
            } finally {
                if (fileInputStream != null) {
                    fileInputStream.close();
                }
            }
        } catch (IOException e) {
            throw new DdlException(e.getMessage());
        }
    }

    @Override
    public Set<String> getGroup(UserIdentity userIdentity) {
        return userGroups.getOrDefault(userIdentity.getUser(), new HashSet<>());
    }

    @Override
    public void checkProperty() throws SemanticException {
        REQUIRED_PROPERTIES.forEach(s -> {
            if (!properties.containsKey(s)) {
                throw new SemanticException("missing required property: " + s);
            }
        });
    }

    @VisibleForTesting
    public InputStream getPath(String groupFileUrl) throws IOException {
        if (groupFileUrl.startsWith("http://") || groupFileUrl.startsWith("https://")) {
            return new URL(groupFileUrl).openStream();
        } else {
            String filePath = StarRocksFE.STARROCKS_HOME_DIR + "/conf/" + groupFileUrl;
            return new FileInputStream(filePath);
        }
    }

    public static String readInputStreamToString(final InputStream stream, final Charset charset) throws IOException {
        final int bufferSize = 1024;
        final char[] buffer = new char[bufferSize];
        final StringBuilder out = new StringBuilder();

        try (Reader in = new InputStreamReader(stream, charset)) {
            while (true) {
                int rsz = in.read(buffer, 0, buffer.length);
                if (rsz < 0) {
                    break;
                }
                out.append(buffer, 0, rsz);
            }
            return out.toString();
        }
    }
}
