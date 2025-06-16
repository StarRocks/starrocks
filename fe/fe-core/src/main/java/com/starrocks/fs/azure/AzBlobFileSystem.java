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

package com.starrocks.fs.azure;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.PagedResponse;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.azure.AzureCloudConfigurationProvider;
import com.starrocks.fs.FileSystem;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.THdfsProperties;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AzBlobFileSystem implements FileSystem {
    private static Logger LOG = LogManager.getLogger(AzBlobFileSystem.class);

    private static final String CONTAINER_URL_FORMAT = "https://%s.%s/%s";
    private static final String CONTAINER_URL_WITH_SAS_TOKEN_FORMAT = "https://%s.%s/%s?%s";
    private static final String SEPARATOR = "/";

    private static int MAX_RESULTS_PER_PAGE = 1000;
    private static long LIST_TIMEOUT_SECONDS = 60L;

    private final Map<String, String> properties;

    public AzBlobFileSystem(Map<String, String> properties) {
        this.properties = properties;
    }

    BlobContainerClient createBlobContainerClient(AzBlobURI uri) {
        String accountName = uri.getAccount();
        String endpointSuffix = uri.getEndpointSuffix();
        String containerName = uri.getContainer();
        String containerUrl = String.format(CONTAINER_URL_FORMAT, accountName, endpointSuffix, containerName);

        // shared key
        String sharedKey = properties.get(CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY);
        if (!Strings.isNullOrEmpty(sharedKey)) {
            StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, sharedKey);
            return new BlobContainerClientBuilder().endpoint(containerUrl).credential(credential).buildClient();
        }

        // sas token
        String sasToken = properties.get(CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN);
        if (!Strings.isNullOrEmpty(sasToken)) {
            containerUrl =
                    String.format(CONTAINER_URL_WITH_SAS_TOKEN_FORMAT, accountName, endpointSuffix, containerName, sasToken);
            return new BlobContainerClientBuilder().endpoint(containerUrl).buildClient();
        }

        // client secret service principal
        String clientId = properties.get(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_ID);
        String clientSecret = properties.get(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_SECRET);
        String tenantId = properties.get(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_TENANT_ID);
        if (!Strings.isNullOrEmpty(clientId) && !Strings.isNullOrEmpty(clientSecret) && !Strings.isNullOrEmpty(tenantId)) {
            ClientSecretCredential credential =
                    new ClientSecretCredentialBuilder().clientId(clientId).clientSecret(clientSecret).tenantId(tenantId).build();
            return new BlobContainerClientBuilder().endpoint(containerUrl).credential(credential).buildClient();
        }

        // user assigned managed identity
        if (!Strings.isNullOrEmpty(clientId)) {
            ManagedIdentityCredential credential = new ManagedIdentityCredentialBuilder().clientId(clientId).build();
            return new BlobContainerClientBuilder().endpoint(containerUrl).credential(credential).buildClient();
        }

        // default
        DefaultAzureCredential defaultCredential = new DefaultAzureCredentialBuilder().build();
        return new BlobContainerClientBuilder().endpoint(containerUrl).credential(defaultCredential).buildClient();
    }

    // TODO: make listDir as public method in FileSystem interface
    private List<FileStatus> listDir(String path) throws StarRocksException {
        AzBlobURI uri = AzBlobURI.parse(path);
        String blobPath = uri.getBlobPath();
        ListBlobsOptions options = new ListBlobsOptions();
        options.setPrefix(String.format("%s%s", blobPath, blobPath.isEmpty() || blobPath.endsWith(SEPARATOR) ? "" : SEPARATOR));
        options.setMaxResultsPerPage(MAX_RESULTS_PER_PAGE);

        List<FileStatus> fileStatusList = Lists.newArrayList();
        try {
            BlobContainerClient containerClient = createBlobContainerClient(uri);
            PagedIterable<BlobItem> blobs = containerClient.listBlobsByHierarchy(
                    SEPARATOR, options, Duration.ofSeconds(LIST_TIMEOUT_SECONDS));
            for (PagedResponse<BlobItem> page : blobs.iterableByPage()) {
                for (BlobItem blobItem : page.getValue()) {
                    LOG.debug("azure blob: {}, is prefix: {}", blobItem.getName(), blobItem.isPrefix());

                    AzBlobURI itemUri = new AzBlobURI(
                            uri.getScheme(), uri.getAccount(), uri.getEndpointSuffix(), uri.getContainer(), blobItem.getName());
                    String itemPath = itemUri.getBlobUri();

                    boolean isDir = blobItem.isPrefix();
                    long itemSize = isDir ? 0L : blobItem.getProperties().getContentLength();
                    long modifiedTime = isDir ? 0L : blobItem.getProperties().getLastModified().toEpochSecond() * 1000L;

                    fileStatusList.add(new FileStatus(itemSize, isDir, 1, 1, modifiedTime, new Path(itemPath)));
                }
            }
        } catch (Exception e) {
            throw new StarRocksException(String.format("Failed to list azure directory %s", path), e);
        }

        return fileStatusList;
    }

    // Translate an absolute path into a list of path components.
    // We merge double slashes into a single slash here.
    // For example, /a/*/c would be broken into the list [a, *, c]
    private static List<String> getPathComponents(String path) {
        List<String> ret = Lists.newArrayList();
        for (String component : path.split(SEPARATOR)) {
            if (!component.isEmpty()) {
                ret.add(component);
            }
        }
        return ret;
    }

    @Override
    public List<FileStatus> globList(String path, boolean skipDir) throws StarRocksException {
        AzBlobURI uri = AzBlobURI.parse(path);

        // Split path into components
        String blobName = uri.getBlobPath();
        List<String> components = getPathComponents(blobName);

        List<FileStatus> candidates = Lists.newArrayList();
        AzBlobURI rootUri = new AzBlobURI(uri.getScheme(), uri.getAccount(), uri.getEndpointSuffix(), uri.getContainer(), "");
        FileStatus rootPlaceholder = new FileStatus(0, true, 0, 0, 0, new Path(rootUri.getBlobUri()));
        candidates.add(rootPlaceholder);

        try {
            for (int componentIdx = 0; componentIdx < components.size(); ++componentIdx) {
                List<FileStatus> newCandidates = Lists.newArrayList();
                String component = components.get(componentIdx);
                GlobFilter globFilter = new GlobFilter(component);
                LOG.debug("AzBlobSystem glob list. component: {}, has pattern: {}", component, globFilter.hasPattern());

                if (candidates.isEmpty()) {
                    break;
                }

                if (componentIdx < components.size() - 1 && !globFilter.hasPattern()) {
                    // Optimization: if this is not the terminal path component, and we
                    // are not matching against a glob, assume that it exists.  If it
                    // doesn't exist, we'll find out later when resolving a later glob
                    // or the terminal path component.
                    for (FileStatus candidate : candidates) {
                        candidate.setPath(new Path(candidate.getPath(), component));
                    }
                    continue;
                }

                for (FileStatus candidate : candidates) {
                    List<FileStatus> children = listDir(candidate.getPath().toString());
                    for (FileStatus child : children) {
                        if (componentIdx < components.size() - 1) {
                            // Don't try to recurse into non-directories.
                            if (!child.isDirectory()) {
                                continue;
                            }
                        }

                        // Set the child path based on the parent path.
                        if (globFilter.accept(child.getPath())) {
                            child.setPath(new Path(candidate.getPath(), child.getPath().getName()));
                            newCandidates.add(child);
                        }
                    }
                }

                candidates = newCandidates;
            }

            return skipDir ? candidates.stream().filter(c -> !c.isDirectory()).collect(Collectors.toList()) : candidates;
        } catch (Exception e) {
            throw new StarRocksException(String.format("Failed to glob list blobs, path: %s", path), e);
        }
    }

    @Override
    public THdfsProperties getHdfsProperties(String path) throws StarRocksException {
        Map<String, String> copied = Maps.newHashMap(properties);

        // Put path into properties, so that we can get storage account and container from path
        copied.put(AzureCloudConfigurationProvider.AZURE_PATH_KEY, path);

        CloudConfiguration cloudConfiguration = new AzureCloudConfigurationProvider().build(copied);
        if (cloudConfiguration == null) {
            throw new StarRocksException("Failed to build azure configuration from properties");
        }

        // Init hadoop related configs as empty string
        cloudConfiguration.loadCommonFields(copied);

        TCloudConfiguration tCloudConfiguration = new TCloudConfiguration();
        cloudConfiguration.toThrift(tCloudConfiguration);

        // Set use azure native sdk as true
        tCloudConfiguration.setAzure_use_native_sdk(true);

        THdfsProperties hdfsProperties = new THdfsProperties();
        hdfsProperties.setCloud_configuration(tCloudConfiguration);
        return hdfsProperties;
    }

}
