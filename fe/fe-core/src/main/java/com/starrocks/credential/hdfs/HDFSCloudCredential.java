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

package com.starrocks.credential.hdfs;

import autovalue.shaded.com.google.common.common.base.Preconditions;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.HDFSFileStoreInfo;
import com.starrocks.credential.CloudCredential;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public class HDFSCloudCredential implements CloudCredential {
    public static final String EMPTY = "empty";

    private String authentication;
    private String userName;
    private String password;
    private String principal;
    private String keyTab;
    private String keyContent;
    private Map<String, String> haConfigurations;

    protected HDFSCloudCredential(String authentication, String userName, String password, String principal,
                                  String keyTab, String keyContent, Map<String, String> haConfigurations) {
        Preconditions.checkNotNull(authentication);
        Preconditions.checkNotNull(userName);
        Preconditions.checkNotNull(password);
        Preconditions.checkNotNull(principal);
        Preconditions.checkNotNull(keyTab);
        Preconditions.checkNotNull(keyContent);
        Preconditions.checkNotNull(haConfigurations);
        this.authentication = authentication;
        this.userName = userName;
        this.password = password;
        this.principal = principal;
        this.keyTab = keyTab;
        this.keyContent = keyContent;
        this.haConfigurations = haConfigurations;
    }

    public String getAuthentication() {
        return authentication;
    }

    public Map<String, String> getHaConfigurations() {
        return haConfigurations;
    }

    @Override
    public void applyToConfiguration(Configuration configuration) {
        // TODO
    }

    @Override
    public boolean validate() {
        if (authentication.equals(EMPTY)) {
            return true;
        }

        if (authentication.equals("simple")) {
            return true;
        }

        if (authentication.equals("kerberos")) {
            if (principal.isEmpty()) {
                return false;
            }
            return !(keyContent.isEmpty() && keyTab.isEmpty());
        }

        return false;
    }

    @Override
    public void toThrift(Map<String, String> properties) {
        // TODO
    }

    @Override
    public String getCredentialString() {
        return "HDFSCloudCredential{" +
                "authentication=" + authentication +
                ", userName='" + userName + '\'' +
                ", password='" + password + '\'' +
                ", principal='" + principal + '\'' +
                ", keyTab='" + keyTab + '\'' +
                ", keyContent='" + keyContent + '\'' +
                '}';
    }

    @Override
    public FileStoreInfo toFileStoreInfo() {
        FileStoreInfo.Builder fileStore = FileStoreInfo.newBuilder();
        fileStore.setFsType(FileStoreType.HDFS);
        HDFSFileStoreInfo.Builder hdfsFileStoreInfo = HDFSFileStoreInfo.newBuilder();
        fileStore.setHdfsFsInfo(hdfsFileStoreInfo.build());
        return fileStore.build();
    }
}
