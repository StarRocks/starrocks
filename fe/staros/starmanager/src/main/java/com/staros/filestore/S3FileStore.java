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


package com.staros.filestore;

import com.staros.credential.AwsCredential;
import com.staros.credential.AwsCredentialMgr;
import com.staros.exception.InvalidArgumentStarException;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.staros.util.Constant;
import com.staros.util.LockCloseable;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class S3FileStore extends AbstractFileStore {
    private static final int DEFAULT_NUM_PARTITIONED_PREFIX = 256;
    private String region;
    private String endpoint;
    private boolean partitionedPrefixEnabled;
    private int numPartitionedPrefix;
    private AwsCredential credential;
    private int pathStyleAccess;

    public S3FileStore(String key, String name, String bucket, String region,
                       String endpoint, AwsCredential credential, String pathPrefix) {
        this(key, name, bucket, region, endpoint, credential, pathPrefix, false, 0, 0);
    }

    public S3FileStore(String key, String name, String bucket, String region,
                       String endpoint, AwsCredential credential, String pathPrefix, int pathStyleAccess) {
        this(key, name, bucket, region, endpoint, credential, pathPrefix, false, 0, pathStyleAccess);
    }

    public S3FileStore(String key, String name, String bucket, String region,
                       String endpoint, AwsCredential credential, String pathPrefix, boolean partitionedPrefixEnabled,
                       int numPartitionedPrefix, int pathStyleAccess) {
        super(key, name);
        this.locations.add(Constant.S3_PREFIX + Paths.get(bucket, pathPrefix));
        this.region = region;
        this.endpoint = endpoint;
        this.credential = credential;
        this.partitionedPrefixEnabled = partitionedPrefixEnabled;
        this.numPartitionedPrefix = numPartitionedPrefix;
        if (this.partitionedPrefixEnabled && this.numPartitionedPrefix == 0) {
            this.numPartitionedPrefix = DEFAULT_NUM_PARTITIONED_PREFIX;
        }
        if (pathStyleAccess < 0 || pathStyleAccess > 2) {
            throw new InvalidArgumentStarException("Invalid path style access: " + pathStyleAccess);
        }
        this.pathStyleAccess = pathStyleAccess;
    }

    public S3FileStore(FileStoreInfo fsInfo, String region,
                       String endpoint, AwsCredential credential, boolean partitionedPrefixEnabled,
                       int numPartitionedPrefix, int pathStyleAccess) {
        super(fsInfo);
        this.region = region;
        this.endpoint = endpoint;
        this.credential = credential;
        this.partitionedPrefixEnabled = partitionedPrefixEnabled;
        this.numPartitionedPrefix = numPartitionedPrefix;
        if (this.partitionedPrefixEnabled && this.numPartitionedPrefix == 0) {
            this.numPartitionedPrefix = DEFAULT_NUM_PARTITIONED_PREFIX;
        }
        if (pathStyleAccess < 0 || pathStyleAccess > 2) {
            throw new InvalidArgumentStarException("Invalid path style access: " + pathStyleAccess);
        }
        this.pathStyleAccess  = pathStyleAccess;
    }

    // return file store type, 'S3','HDFS' etc
    @Override
    public FileStoreType type() {
        return FileStoreType.S3;
    }

    // return file store root path
    @Override
    public String rootPath() {
        return locations.get(0);
    }

    @Override
    public boolean isValid() {
        assert (!locations.isEmpty());

        String[] bucketAndPrefix = getBucketAndPrefix(locations.get(0));
        String bucket = bucketAndPrefix[0];
        if (bucket.isEmpty()) {
            return false;
        }

        if (credential == null) {
            return false;
        }

        if (partitionedPrefixEnabled) {
            String subPath = bucketAndPrefix[1];
            if (!subPath.isEmpty()) {
                return false;
            }
            if (numPartitionedPrefix <= 0) {
                return false;
            }
        }
        return true;
    }

    private FileStoreInfo toProtobufInternal(boolean includeSecret) {
        try (LockCloseable lockCloseable = new LockCloseable(lock.readLock())) {
            assert (!locations.isEmpty());

            String[] bucketAndPrefix = getBucketAndPrefix(locations.get(0));
            S3FileStoreInfo.Builder s3fsBuilder = S3FileStoreInfo.newBuilder()
                    .setBucket(bucketAndPrefix[0])
                    .setRegion(region)
                    .setEndpoint(endpoint)
                    .setPathPrefix(bucketAndPrefix[1])
                    .setPartitionedPrefixEnabled(partitionedPrefixEnabled)
                    .setNumPartitionedPrefix(numPartitionedPrefix)
                    .setPathStyleAccess(pathStyleAccess);
            if (includeSecret) {
                s3fsBuilder.setCredential(AwsCredentialMgr.toProtobuf(credential));
            }

            return toProtobufBuilder()
                    .setFsType(FileStoreType.S3)
                    .setS3FsInfo(s3fsBuilder)
                    .build();
        }
    }

    @Override
    public FileStoreInfo toProtobuf() {
        return toProtobufInternal(true /* includeSecret */);
    }

    @Override
    public FileStoreInfo toDebugProtobuf() {
        return toProtobufInternal(false /* includeSecret */);
    }

    @Override
    public void mergeFrom(FileStore other) {
        assert other.type() == FileStoreType.S3;
        super.mergeFrom(other);
        S3FileStore s3fs = (S3FileStore) other;
        if (!s3fs.getRegion().isEmpty()) {
            this.region = s3fs.region;
        }
        if (!s3fs.getEndpoint().isEmpty()) {
            this.endpoint = s3fs.endpoint;
        }
        if (s3fs.getCredential() != null) {
            this.credential = s3fs.credential;
        }
        // NOTE: the following two fields are not expected to be updated.
        if (s3fs.partitionedPrefixEnabled) {
            this.partitionedPrefixEnabled = true;
        }
        if (s3fs.numPartitionedPrefix != 0) {
            this.numPartitionedPrefix = s3fs.numPartitionedPrefix;
        }
        if (s3fs.pathStyleAccess != 0) {
            this.pathStyleAccess = s3fs.pathStyleAccess;
        }
    }

    @Override
    public boolean isPartitionedPrefixEnabled() {
        return partitionedPrefixEnabled;
    }

    @Override
    public int numOfPartitionedPrefix() {
        return numPartitionedPrefix;
    }

    public static FileStore fromProtobuf(FileStoreInfo fsInfo) {
        S3FileStoreInfo s3FsInfo = fsInfo.getS3FsInfo();
        String bucket = s3FsInfo.getBucket();
        String region = s3FsInfo.getRegion();
        String endpoint = s3FsInfo.getEndpoint();
        String pathPrefix = s3FsInfo.getPathPrefix();
        if (fsInfo.getLocationsList().isEmpty()) {
            List<String> locations = new ArrayList<>(Arrays.asList(Constant.S3_PREFIX + Paths.get(bucket, pathPrefix)));
            fsInfo = fsInfo.toBuilder().addAllLocations(locations).build();
        }
        AwsCredential credential = AwsCredentialMgr.fromProtobuf(s3FsInfo.getCredential());
        boolean partitionedPrefixEnabled = s3FsInfo.getPartitionedPrefixEnabled();
        int numPartitionedPrefix = s3FsInfo.getNumPartitionedPrefix();
        int pathStyleAccess = s3FsInfo.getPathStyleAccess();
        return new S3FileStore(fsInfo, region, endpoint, credential, partitionedPrefixEnabled, numPartitionedPrefix,
                pathStyleAccess);
    }

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public int getPathStyleAccess() {
        return pathStyleAccess;
    }

    public AwsCredential getCredential() {
        return credential;
    }

    private String[] getBucketAndPrefix(String location) {
        // location pattern s3://xxx/xxx
        String path = location.substring(Constant.S3_PREFIX.length());
        int index = path.indexOf('/');
        if (index < 0) {
            return new String[] {path, ""};
        }

        return new String[] {path.substring(0, index), path.substring(index + 1)};
    }
}
