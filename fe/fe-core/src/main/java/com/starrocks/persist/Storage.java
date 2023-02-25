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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/Storage.java

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

package com.starrocks.persist;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.ha.FrontendNodeType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

// VERSION file contains clusterId. eg:
//      clusterId=123456
// ROLE file contains FrontendNodeType and NodeName. eg:
//      hostType=IP(FQDN)
//      role=OBSERVER
//      name=172.0.0.1_1234_DNwid284dasdwd

public class Storage {
    private static final Logger LOG = LogManager.getLogger(Storage.class);

    public static final String IMAGE_NEW = "image.ckpt";
    public static final String IMAGE = "image";
    public static final String VERSION_FILE = "VERSION";
    public static final String ROLE_FILE = "ROLE";

    // version file props keys
    private static final String VERSION_PROP_CLUSTER_ID = "clusterId";
    private static final String VERSION_PROP_TOKEN = "token";
    private static final String VERSION_PROP_RUN_MODE = "runMode";
    // role file props keys
    private static final String ROLE_PROP_FRONTEND_ROLE = "role";
    private static final String ROLE_PROP_NODE_NAME = "name";
    private static final String ROLE_PROP_HOST_TYPE = "hostType";

    // version file props values
    private int clusterID = 0;
    private String token;
    private String runMode;
    // role file props values
    private FrontendNodeType role = FrontendNodeType.UNKNOWN;
    private String nodeName;
    private String hostType = "";

    private long imageJournalId;
    private String metaDir;

    public Storage(int clusterID, String token, String metaDir) {
        this.clusterID = clusterID;
        this.token = token;
        this.metaDir = metaDir;
    }

    public Storage(int clusterID, String token, long imageJournalId, String metaDir) {
        this.clusterID = clusterID;
        this.token = token;
        this.imageJournalId = imageJournalId;
        this.metaDir = metaDir;
    }

    public Storage(String metaDir) throws IOException {
        this.metaDir = metaDir;

        reload();
    }

    public void reload() throws IOException {
        // Read version file info
        Properties prop = new Properties();
        File versionFile = getVersionFile();
        if (versionFile.isFile()) {
            try (FileInputStream in = new FileInputStream(versionFile)) {
                prop.load(in);
                clusterID = Integer.parseInt(prop.getProperty(VERSION_PROP_CLUSTER_ID));
                if (prop.getProperty(VERSION_PROP_TOKEN) != null) {
                    token = prop.getProperty(VERSION_PROP_TOKEN);
                }
                if (prop.getProperty(VERSION_PROP_RUN_MODE) != null) {
                    runMode = prop.getProperty(VERSION_PROP_RUN_MODE);
                }
            }

        }

        File roleFile = getRoleFile();
        if (roleFile.isFile()) {
            try (FileInputStream in = new FileInputStream(roleFile)) {
                prop.load(in);
                role = FrontendNodeType.valueOf(prop.getProperty(ROLE_PROP_FRONTEND_ROLE));
                // For compatibility, NODE_NAME may not exist in ROLE file, set nodeName to null
                nodeName = prop.getProperty(ROLE_PROP_NODE_NAME, null);
                hostType = prop.getProperty(ROLE_PROP_HOST_TYPE, "");
            }
        }

        // Find the latest image
        File dir = new File(metaDir);
        File[] children = dir.listFiles();
        if (children != null) {
            for (File child : children) {
                String name = child.getName();
                try {
                    if (!name.equals(IMAGE_NEW) && name.startsWith(IMAGE) && name.contains(".")) {
                        imageJournalId =
                                Math.max(Long.parseLong(name.substring(name.lastIndexOf('.') + 1)), imageJournalId);
                    }
                } catch (Exception e) {
                    LOG.warn(name + " is not a validate meta file, ignore it");
                }
            }
        }
    }

    public int getClusterID() {
        return clusterID;
    }

    public void setClusterID(int clusterID) {
        this.clusterID = clusterID;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getRunMode() {
        return runMode;
    }

    public void setRunMode(String runMode) {
        this.runMode = runMode;
    }

    public FrontendNodeType getRole() {
        return role;
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getMetaDir() {
        return metaDir;
    }

    public void setMetaDir(String metaDir) {
        this.metaDir = metaDir;
    }

    public long getImageJournalId() {
        return imageJournalId;
    }

    public void setImageJournalId(long imageJournalId) {
        this.imageJournalId = imageJournalId;
    }

    public static int newClusterID() {
        int newID = 0;
        while (newID == 0) {
            newID = ThreadLocalRandom.current().nextInt(0x7FFFFFFF);
        }
        return newID;
    }

    public static String newToken() {
        return UUID.randomUUID().toString();
    }

    public void writeVersionFile() throws IOException {
        Properties properties = new Properties();
        Preconditions.checkState(clusterID > 0);
        properties.setProperty(VERSION_PROP_CLUSTER_ID, String.valueOf(clusterID));
        if (!Strings.isNullOrEmpty(token)) {
            properties.setProperty(VERSION_PROP_TOKEN, token);
        }
        properties.setProperty(VERSION_PROP_RUN_MODE, runMode);
        try (RandomAccessFile file = new RandomAccessFile(new File(metaDir, VERSION_FILE), "rws")) {
            file.seek(0);
            try (FileOutputStream out = new FileOutputStream(file.getFD())) {
                properties.store(out, null);
                file.setLength(out.getChannel().position());
            }
        }
    }

    // note: if you want to use this func, please make sure that properties that have stored in role file
    // could not be delete
    public void writeFrontendRoleAndNodeName(FrontendNodeType role, String nodeName) throws IOException {
        Preconditions.checkState(!Strings.isNullOrEmpty(nodeName));
        this.role = role;
        this.nodeName = nodeName;
        writeRoleFile();
    }

    public void writeFeStartFeHostType(String hostType) throws IOException {
        Preconditions.checkState(!Strings.isNullOrEmpty(hostType));
        this.hostType = hostType;
        writeRoleFile();
    }

    private void writeRoleFile() throws IOException {
        Properties properties = new Properties();
        properties.setProperty(ROLE_PROP_FRONTEND_ROLE, this.role.name());
        properties.setProperty(ROLE_PROP_NODE_NAME, this.nodeName);
        properties.setProperty(ROLE_PROP_HOST_TYPE, this.hostType);
        try (RandomAccessFile file = new RandomAccessFile(new File(metaDir, ROLE_FILE), "rws")) {
            file.seek(0);
            try (FileOutputStream out = new FileOutputStream(file.getFD())) {
                properties.store(out, null);
                file.setLength(out.getChannel().position());
            }
        }
    }

    public void clear() throws IOException {
        File metaFile = new File(metaDir);
        if (metaFile.exists()) {
            String[] children = metaFile.list();
            if (children != null) {
                for (String child : children) {
                    File file = new File(metaFile, child);
                    if (!file.delete()) {
                        LOG.warn("Failed to delete file, filepath={}", file.getAbsolutePath());
                    }
                }
            }
            if (!metaFile.delete()) {
                LOG.warn("Failed to delete file, filepath={}", metaFile.getAbsolutePath());
            }
        }

        if (!metaFile.mkdirs()) {
            throw new IOException("Cannot create directory " + metaFile);
        }
    }

    public static void rename(File from, File to) throws IOException {
        if (!from.renameTo(to)) {
            throw new IOException("Failed to rename  " + from.getCanonicalPath()
                    + " to " + to.getCanonicalPath());
        }
    }

    public File getCurrentImageFile() {
        return getImageFile(imageJournalId);
    }

    public File getImageFile(long version) {
        return getImageFile(new File(metaDir), version);
    }

    public static File getImageFile(File dir, long version) {
        return new File(dir, IMAGE + "." + version);
    }

    public final File getVersionFile() {
        return new File(metaDir, VERSION_FILE);
    }

    public final File getRoleFile() {
        return new File(metaDir, ROLE_FILE);
    }
}

