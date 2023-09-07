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
    private String krbPrincipal;
    private String krbKeyTabFile;
    private String krbKeyTabData;
    private Map<String, String> hadoopConfiguration;

    protected HDFSCloudCredential(String authentication, String username, String password, String krbPrincipal,
                                  String krbKeyTabFile, String krbKeyTabData, Map<String, String> hadoopConfiguration) {
        Preconditions.checkNotNull(authentication);
        Preconditions.checkNotNull(username);
        Preconditions.checkNotNull(password);
        Preconditions.checkNotNull(krbPrincipal);
        Preconditions.checkNotNull(krbKeyTabFile);
        Preconditions.checkNotNull(krbKeyTabData);
        Preconditions.checkNotNull(hadoopConfiguration);
        this.authentication = authentication;
        this.userName = username;
        this.password = password;
        this.krbPrincipal = krbPrincipal;
        this.krbKeyTabFile = krbKeyTabFile;
        this.krbKeyTabData = krbKeyTabData;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    public String getAuthentication() {
        return authentication;
    }

    public Map<String, String> getHadoopConfiguration() {
        return hadoopConfiguration;
    }

    @Override
    public void applyToConfiguration(Configuration configuration) {
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
            if (krbPrincipal.isEmpty()) {
                return false;
            }
            return !(krbKeyTabData.isEmpty() && krbKeyTabFile.isEmpty());
        }

        return false;
    }

    @Override
    public void toThrift(Map<String, String> properties) {
    }

    @Override
    public String toCredString() {
        return "HDFSCloudCredential{" +
                "authentication='" + authentication + '\'' +
                ", username='" + userName + '\'' +
                ", password='" + password + '\'' +
                ", krbPrincipal='" + krbPrincipal + '\'' +
                ", krbKeyTabFile='" + krbKeyTabFile + '\'' +
                ", krbKeyTabData='" + krbKeyTabData + '\'' +
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
