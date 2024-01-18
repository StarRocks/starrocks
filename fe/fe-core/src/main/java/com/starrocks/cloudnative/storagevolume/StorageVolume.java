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

package com.starrocks.cloudnative.storagevolume;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.AzBlobCredentialInfo;
import com.staros.proto.AzBlobFileStoreInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.HDFSFileStoreInfo;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorageVolume implements Writable, GsonPostProcessable {
    public enum StorageVolumeType {
        UNKNOWN,
        S3,
        HDFS,
        AZBLOB
    }

    // Without id, the scenario like "create storage volume 'a', drop storage volume 'a', create storage volume 'a'"
    // can not be handled. They will be treated as the same storage volume.
    @SerializedName("i")
    private String id;

    @SerializedName("n")
    private String name;

    @SerializedName("s")
    private StorageVolumeType svt;

    @SerializedName("l")
    private List<String> locations;

    private CloudConfiguration cloudConfiguration;

    @SerializedName("p")
    private Map<String, String> params;

    @SerializedName("c")
    private String comment;

    @SerializedName("e")
    private boolean enabled;

    public static String CREDENTIAL_MASK = "******";

    public StorageVolume(String id, String name, String svt, List<String> locations,
                         Map<String, String> params, boolean enabled, String comment) {
        this.id = id;
        this.name = name;
        this.svt = toStorageVolumeType(svt);
        this.locations = new ArrayList<>(locations);
        this.comment = comment;
        this.enabled = enabled;
        this.params = new HashMap<>(params);
        Map<String, String> configurationParams = new HashMap<>(params);
        preprocessAuthenticationIfNeeded(configurationParams);
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(configurationParams, true);
        if (!isValidCloudConfiguration()) {
            Gson gson = new Gson();
            throw new SemanticException("Storage params is not valid " + gson.toJson(params));
        }
    }

    public StorageVolume(StorageVolume sv) {
        this.id = sv.id;
        this.name = sv.name;
        this.svt = sv.svt;
        this.locations = new ArrayList<>(sv.locations);
        this.comment = sv.comment;
        this.enabled = sv.enabled;
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(sv.params, true);
        this.params = new HashMap<>(sv.params);
    }

    public void setCloudConfiguration(Map<String, String> params) {
        Map<String, String> newParams = new HashMap<>(this.params);
        newParams.putAll(params);
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(newParams, true);
        if (!isValidCloudConfiguration()) {
            Gson gson = new Gson();
            throw new SemanticException("Storage params is not valid " + gson.toJson(newParams));
        }
        this.params = newParams;
    }

    public CloudConfiguration getCloudConfiguration() {
        return cloudConfiguration;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }

    public String getType() {
        return svt.toString();
    }

    private StorageVolumeType toStorageVolumeType(String svt) {
        switch (svt.toLowerCase()) {
            case "s3":
                return StorageVolumeType.S3;
            case "hdfs":
                return StorageVolumeType.HDFS;
            case "azblob":
                return StorageVolumeType.AZBLOB;
            default:
                return StorageVolumeType.UNKNOWN;
        }
    }

    private boolean isValidCloudConfiguration() {
        switch (svt) {
            case S3:
                return cloudConfiguration.getCloudType() == CloudType.AWS;
            case HDFS:
                return cloudConfiguration.getCloudType() == CloudType.HDFS;
            case AZBLOB:
                return cloudConfiguration.getCloudType() == CloudType.AZURE;
            default:
                return false;
        }
    }

    public static void addMaskForCredential(Map<String, String> params) {
        params.computeIfPresent(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, (key, value) -> CREDENTIAL_MASK);
        params.computeIfPresent(CloudConfigurationConstants.AWS_S3_SECRET_KEY, (key, value) -> CREDENTIAL_MASK);
        params.computeIfPresent(CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY, (key, value) -> CREDENTIAL_MASK);
        params.computeIfPresent(CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN, (key, value) -> CREDENTIAL_MASK);
    }

    public void getProcNodeData(BaseProcResult result) {
        Gson gson = new Gson();
        Map<String, String> p = new HashMap<>(params);
        addMaskForCredential(p);
        result.addRow(Lists.newArrayList(name,
                svt.name(),
                String.valueOf(GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                        .getDefaultStorageVolumeId().equals(id)),
                Joiner.on(", ").join(locations),
                String.valueOf(gson.toJson(p)),
                String.valueOf(enabled),
                String.valueOf(comment)));
    }

    public static FileStoreInfo createFileStoreInfo(String name, String svt,
                                                    List<String> locations, Map<String, String> params,
                                                    boolean enabled, String comment) {
        StorageVolume sv = new StorageVolume("", name, svt, locations, params, enabled, comment);
        return sv.toFileStoreInfo();
    }

    public FileStoreInfo toFileStoreInfo() {
        FileStoreInfo fsInfo = cloudConfiguration.toFileStoreInfo();
        FileStoreInfo.Builder builder = fsInfo.toBuilder();
        builder.setFsKey(id).setFsName(this.name).setComment(this.comment).setEnabled(this.enabled)
                .addAllLocations(locations).build();
        return builder.build();
    }

    public static StorageVolume fromFileStoreInfo(FileStoreInfo fsInfo) {
        String svt = fsInfo.getFsType().toString();
        Map<String, String> params = getParamsFromFileStoreInfo(fsInfo);
        return new StorageVolume(fsInfo.getFsKey(), fsInfo.getFsName(), svt,
                fsInfo.getLocationsList(), params, fsInfo.getEnabled(), fsInfo.getComment());
    }

    public static Map<String, String> getParamsFromFileStoreInfo(FileStoreInfo fsInfo) {
        Map<String, String> params = new HashMap<>();
        switch (fsInfo.getFsType()) {
            case S3:
                S3FileStoreInfo s3FileStoreInfo = fsInfo.getS3FsInfo();
                params.put(CloudConfigurationConstants.AWS_S3_REGION, s3FileStoreInfo.getRegion());
                params.put(CloudConfigurationConstants.AWS_S3_ENDPOINT, s3FileStoreInfo.getEndpoint());
                AwsCredentialInfo credentialInfo = s3FileStoreInfo.getCredential();
                if (credentialInfo.hasSimpleCredential()) {
                    params.put(CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE, "false");
                    params.put(CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false");
                    params.put(CloudConfigurationConstants.AWS_S3_ACCESS_KEY,
                            credentialInfo.getSimpleCredential().getAccessKey());
                    params.put(CloudConfigurationConstants.AWS_S3_SECRET_KEY,
                            credentialInfo.getSimpleCredential().getAccessKeySecret());
                } else if (credentialInfo.hasAssumeRoleCredential()) {
                    params.put(CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE, "true");
                    params.put(CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false");
                    params.put(CloudConfigurationConstants.AWS_S3_IAM_ROLE_ARN,
                            credentialInfo.getAssumeRoleCredential().getIamRoleArn());
                    params.put(CloudConfigurationConstants.AWS_S3_EXTERNAL_ID,
                            credentialInfo.getAssumeRoleCredential().getExternalId());
                } else if (credentialInfo.hasProfileCredential()) {
                    params.put(CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE, "true");
                    params.put(CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false");
                } else if (credentialInfo.hasDefaultCredential()) {
                    params.put(CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
                }
                return params;
            case HDFS:
                HDFSFileStoreInfo hdfsFileStoreInfo = fsInfo.getHdfsFsInfo();
                params.putAll(hdfsFileStoreInfo.getConfigurationMap());
                String userName = hdfsFileStoreInfo.getUsername();
                if (!Strings.isNullOrEmpty(userName)) {
                    params.put(CloudConfigurationConstants.HDFS_USERNAME_DEPRECATED, userName);
                }
                return params;
            case AZBLOB:
                AzBlobFileStoreInfo azBlobFileStoreInfo = fsInfo.getAzblobFsInfo();
                params.put(CloudConfigurationConstants.AZURE_BLOB_ENDPOINT, azBlobFileStoreInfo.getEndpoint());
                AzBlobCredentialInfo azBlobcredentialInfo = azBlobFileStoreInfo.getCredential();
                String sharedKey = azBlobcredentialInfo.getSharedKey();
                if (!Strings.isNullOrEmpty(sharedKey)) {
                    params.put(CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY, sharedKey);
                }
                String sasToken = azBlobcredentialInfo.getSasToken();
                if (!Strings.isNullOrEmpty(sasToken)) {
                    params.put(CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN, sasToken);
                }
                return params;
            default:
                return params;
        }
    }

    private void preprocessAuthenticationIfNeeded(Map<String, String> params) {
        if (svt == StorageVolumeType.AZBLOB) {
            String container = locations.get(0).split("/")[0];
            params.put(CloudConfigurationConstants.AZURE_BLOB_CONTAINER, container);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static StorageVolume read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, StorageVolume.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(params);
    }
}
