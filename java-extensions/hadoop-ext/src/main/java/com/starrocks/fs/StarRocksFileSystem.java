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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class StarRocksFileSystem extends FileSystem {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(StarRocksFileSystem.class);

    FileSystem fs;
    FileSystemCacheManager.Key key;

    static final FileSystemCacheManager CACHE = new FileSystemCacheManager();

    static FileSystem createRealFileSystem(URI uri, Configuration conf) throws IOException {
        String scheme = parseSchemeFromURI(uri, conf);
        String implKey = String.format(HadoopExt.FS_IMPL_FMT, scheme);
        String implValue = conf.get(implKey);
        String disableKey = String.format(HadoopExt.FS_IMPL_DISABLE_CACHE_FMT, scheme);
        String disableValue = conf.get(disableKey);

        // set default implementation
        if (HadoopExt.isS3Scheme(scheme)) {
            conf.set(implKey, HadoopExt.FS_S3A_FILESYSTEM);
        } else {
            conf.unset(implKey);
        }
        // if user has specified impl in config file
        String implSavedKey = String.format(HadoopExt.FS_IMPL_FMT_SAVED, scheme);
        String implSavedValue = conf.get(implSavedKey);
        if (implSavedValue != null) {
            LOGGER.info(HadoopExt.LOGGER_MESSAGE_PREFIX + " Create FileSystem Impl = " + implSavedValue);
            conf.set(implKey, implSavedValue);
        }

        conf.setBoolean(disableKey, true);
        try {
            return FileSystem.get(uri, conf);
        } finally {
            if (implValue != null) {
                conf.set(implKey, implValue);
            }
            if (disableValue != null) {
                conf.set(disableKey, disableValue);
            }
        }
    }

    public StarRocksFileSystem() {
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        key = FileSystemCacheManager.buildCacheKey(uri, conf);
        fs = CACHE.get(uri, conf, key);
        LOGGER.info(HadoopExt.LOGGER_MESSAGE_PREFIX + " Get FileSystem Instance = " + this);
    }

    public static String parseSchemeFromURI(URI name, Configuration conf) {
        final String scheme;
        if (name.getScheme() == null || name.getScheme().isEmpty()) {
            scheme = getDefaultUri(conf).getScheme();
        } else {
            scheme = name.getScheme();
        }
        return scheme;
    }

    @Override
    public void close() throws IOException {
        LOGGER.info(HadoopExt.LOGGER_MESSAGE_PREFIX + " Close FileSystem Instance = " + this);
        fs.close();
        CACHE.remove(key, fs);
    }

    @Override
    public String toString() {
        return String.format("StarRocksFileSystem(key = %s, fs = %s)", key == null ? "null" : key, fs == null ? "null" : fs);
    }

    // =================== proxy methods ======================
    @Override
    public java.net.URI getUri() {
        return fs.getUri();
    }

    @Override
    public long getUsed(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.getUsed(arg0);
    }

    @Override
    public long getUsed() throws java.io.IOException {
        return fs.getUsed();
    }

    @Override
    public void setOwner(org.apache.hadoop.fs.Path arg0, java.lang.String arg1, java.lang.String arg2)
            throws java.io.IOException {
        fs.setOwner(arg0, arg1, arg2);
    }

    @Override
    public void setTimes(org.apache.hadoop.fs.Path arg0, long arg1, long arg2) throws java.io.IOException {
        fs.setTimes(arg0, arg1, arg2);
    }

    @Override
    public void setAcl(org.apache.hadoop.fs.Path arg0, java.util.List arg1) throws java.io.IOException {
        fs.setAcl(arg0, arg1);
    }

    @Override
    public void setXAttr(org.apache.hadoop.fs.Path arg0, java.lang.String arg1, byte[] arg2) throws java.io.IOException {
        fs.setXAttr(arg0, arg1, arg2);
    }

    @Override
    public void setXAttr(org.apache.hadoop.fs.Path arg0, java.lang.String arg1, byte[] arg2, java.util.EnumSet arg3)
            throws java.io.IOException {
        fs.setXAttr(arg0, arg1, arg2, arg3);
    }

    @Override
    public byte[] getXAttr(org.apache.hadoop.fs.Path arg0, java.lang.String arg1) throws java.io.IOException {
        return fs.getXAttr(arg0, arg1);
    }

    @Override
    public java.lang.String getCanonicalServiceName() {
        return fs.getCanonicalServiceName();
    }

    @Override
    public org.apache.hadoop.fs.Path makeQualified(org.apache.hadoop.fs.Path arg0) {
        return fs.makeQualified(arg0);
    }

    @Override
    public org.apache.hadoop.security.token.Token getDelegationToken(java.lang.String arg0) throws java.io.IOException {
        return fs.getDelegationToken(arg0);
    }

    @Override
    public org.apache.hadoop.security.token.Token[] addDelegationTokens(java.lang.String arg0,
                                                                        org.apache.hadoop.security.Credentials arg1)
            throws java.io.IOException {
        return fs.addDelegationTokens(arg0, arg1);
    }

    @Override
    public org.apache.hadoop.fs.FileSystem[] getChildFileSystems() {
        return fs.getChildFileSystems();
    }

    @Override
    public org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.Path arg0, long arg1, long arg2)
            throws java.io.IOException {
        return fs.getFileBlockLocations(arg0, arg1, arg2);
    }

    @Override
    public org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.FileStatus arg0, long arg1, long arg2)
            throws java.io.IOException {
        return fs.getFileBlockLocations(arg0, arg1, arg2);
    }

    @Override
    public org.apache.hadoop.fs.FsServerDefaults getServerDefaults(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.getServerDefaults(arg0);
    }

    @Override
    public org.apache.hadoop.fs.FsServerDefaults getServerDefaults() throws java.io.IOException {
        return fs.getServerDefaults();
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path arg0, boolean arg1, int arg2,
                                                                      short arg3, long arg4,
                                                                      org.apache.hadoop.util.Progressable arg5)
            throws java.io.IOException {
        return fs.createNonRecursive(arg0, arg1, arg2, arg3, arg4, arg5);
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path arg0,
                                                                      org.apache.hadoop.fs.permission.FsPermission arg1,
                                                                      boolean arg2, int arg3, short arg4, long arg5,
                                                                      org.apache.hadoop.util.Progressable arg6)
            throws java.io.IOException {
        return fs.createNonRecursive(arg0, arg1, arg2, arg3, arg4, arg5, arg6);
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path arg0,
                                                                      org.apache.hadoop.fs.permission.FsPermission arg1,
                                                                      java.util.EnumSet arg2, int arg3, short arg4, long arg5,
                                                                      org.apache.hadoop.util.Progressable arg6)
            throws java.io.IOException {
        return fs.createNonRecursive(arg0, arg1, arg2, arg3, arg4, arg5, arg6);
    }

    @Override
    public short getReplication(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.getReplication(arg0);
    }

    @Override
    public boolean setReplication(org.apache.hadoop.fs.Path arg0, short arg1) throws java.io.IOException {
        return fs.setReplication(arg0, arg1);
    }

    @Override
    public boolean cancelDeleteOnExit(org.apache.hadoop.fs.Path arg0) {
        return fs.cancelDeleteOnExit(arg0);
    }

    @Override
    public org.apache.hadoop.fs.ContentSummary getContentSummary(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.getContentSummary(arg0);
    }

    @Override
    public org.apache.hadoop.fs.QuotaUsage getQuotaUsage(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.getQuotaUsage(arg0);
    }

    @Override
    public org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path arg0, org.apache.hadoop.fs.PathFilter arg1)
            throws java.io.FileNotFoundException, java.io.IOException {
        return fs.listStatus(arg0, arg1);
    }

    @Override
    public org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path[] arg0)
            throws java.io.FileNotFoundException, java.io.IOException {
        return fs.listStatus(arg0);
    }

    @Override
    public org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path arg0)
            throws java.io.FileNotFoundException, java.io.IOException {
        return fs.listStatus(arg0);
    }

    @Override
    public org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path[] arg0, org.apache.hadoop.fs.PathFilter arg1)
            throws java.io.FileNotFoundException, java.io.IOException {
        return fs.listStatus(arg0, arg1);
    }

    @Override
    public org.apache.hadoop.fs.RemoteIterator listCorruptFileBlocks(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.listCorruptFileBlocks(arg0);
    }

    @Override
    public org.apache.hadoop.fs.FileStatus[] globStatus(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.globStatus(arg0);
    }

    @Override
    public org.apache.hadoop.fs.FileStatus[] globStatus(org.apache.hadoop.fs.Path arg0, org.apache.hadoop.fs.PathFilter arg1)
            throws java.io.IOException {
        return fs.globStatus(arg0, arg1);
    }

    @Override
    public org.apache.hadoop.fs.RemoteIterator listLocatedStatus(org.apache.hadoop.fs.Path arg0)
            throws java.io.FileNotFoundException, java.io.IOException {
        return fs.listLocatedStatus(arg0);
    }

    @Override
    public org.apache.hadoop.fs.RemoteIterator listStatusIterator(org.apache.hadoop.fs.Path arg0)
            throws java.io.FileNotFoundException, java.io.IOException {
        return fs.listStatusIterator(arg0);
    }

    @Override
    public org.apache.hadoop.fs.Path getHomeDirectory() {
        return fs.getHomeDirectory();
    }

    @Override
    public void setWorkingDirectory(org.apache.hadoop.fs.Path arg0) {
        fs.setWorkingDirectory(arg0);
    }

    @Override
    public org.apache.hadoop.fs.Path getWorkingDirectory() {
        return fs.getWorkingDirectory();
    }

    @Override
    public void copyFromLocalFile(boolean arg0, boolean arg1, org.apache.hadoop.fs.Path arg2, org.apache.hadoop.fs.Path arg3)
            throws java.io.IOException {
        fs.copyFromLocalFile(arg0, arg1, arg2, arg3);
    }

    @Override
    public void copyFromLocalFile(boolean arg0, org.apache.hadoop.fs.Path arg1, org.apache.hadoop.fs.Path arg2)
            throws java.io.IOException {
        fs.copyFromLocalFile(arg0, arg1, arg2);
    }

    @Override
    public void copyFromLocalFile(boolean arg0, boolean arg1, org.apache.hadoop.fs.Path[] arg2, org.apache.hadoop.fs.Path arg3)
            throws java.io.IOException {
        fs.copyFromLocalFile(arg0, arg1, arg2, arg3);
    }

    @Override
    public void copyFromLocalFile(org.apache.hadoop.fs.Path arg0, org.apache.hadoop.fs.Path arg1) throws java.io.IOException {
        fs.copyFromLocalFile(arg0, arg1);
    }

    @Override
    public void moveFromLocalFile(org.apache.hadoop.fs.Path[] arg0, org.apache.hadoop.fs.Path arg1) throws java.io.IOException {
        fs.moveFromLocalFile(arg0, arg1);
    }

    @Override
    public void moveFromLocalFile(org.apache.hadoop.fs.Path arg0, org.apache.hadoop.fs.Path arg1) throws java.io.IOException {
        fs.moveFromLocalFile(arg0, arg1);
    }

    @Override
    public void copyToLocalFile(org.apache.hadoop.fs.Path arg0, org.apache.hadoop.fs.Path arg1) throws java.io.IOException {
        fs.copyToLocalFile(arg0, arg1);
    }

    @Override
    public void copyToLocalFile(boolean arg0, org.apache.hadoop.fs.Path arg1, org.apache.hadoop.fs.Path arg2)
            throws java.io.IOException {
        fs.copyToLocalFile(arg0, arg1, arg2);
    }

    @Override
    public void copyToLocalFile(boolean arg0, org.apache.hadoop.fs.Path arg1, org.apache.hadoop.fs.Path arg2, boolean arg3)
            throws java.io.IOException {
        fs.copyToLocalFile(arg0, arg1, arg2, arg3);
    }

    @Override
    public void moveToLocalFile(org.apache.hadoop.fs.Path arg0, org.apache.hadoop.fs.Path arg1) throws java.io.IOException {
        fs.moveToLocalFile(arg0, arg1);
    }

    @Override
    public org.apache.hadoop.fs.Path startLocalOutput(org.apache.hadoop.fs.Path arg0, org.apache.hadoop.fs.Path arg1)
            throws java.io.IOException {
        return fs.startLocalOutput(arg0, arg1);
    }

    @Override
    public void completeLocalOutput(org.apache.hadoop.fs.Path arg0, org.apache.hadoop.fs.Path arg1) throws java.io.IOException {
        fs.completeLocalOutput(arg0, arg1);
    }

    @Override
    public long getBlockSize(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.getBlockSize(arg0);
    }

    @Override
    public long getDefaultBlockSize() {
        return fs.getDefaultBlockSize();
    }

    @Override
    public long getDefaultBlockSize(org.apache.hadoop.fs.Path arg0) {
        return fs.getDefaultBlockSize(arg0);
    }

    @Override
    public short getDefaultReplication() {
        return fs.getDefaultReplication();
    }

    @Override
    public short getDefaultReplication(org.apache.hadoop.fs.Path arg0) {
        return fs.getDefaultReplication(arg0);
    }

    @Override
    public org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.getFileStatus(arg0);
    }

    @Override
    public void createSymlink(org.apache.hadoop.fs.Path arg0, org.apache.hadoop.fs.Path arg1, boolean arg2)
            throws org.apache.hadoop.security.AccessControlException, org.apache.hadoop.fs.FileAlreadyExistsException,
            java.io.FileNotFoundException, org.apache.hadoop.fs.ParentNotDirectoryException,
            org.apache.hadoop.fs.UnsupportedFileSystemException, java.io.IOException {
        fs.createSymlink(arg0, arg1, arg2);
    }

    @Override
    public org.apache.hadoop.fs.FileStatus getFileLinkStatus(org.apache.hadoop.fs.Path arg0)
            throws org.apache.hadoop.security.AccessControlException, java.io.FileNotFoundException,
            org.apache.hadoop.fs.UnsupportedFileSystemException, java.io.IOException {
        return fs.getFileLinkStatus(arg0);
    }

    @Override
    public boolean supportsSymlinks() {
        return fs.supportsSymlinks();
    }

    @Override
    public org.apache.hadoop.fs.Path getLinkTarget(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.getLinkTarget(arg0);
    }

    @Override
    public org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.getFileChecksum(arg0);
    }

    @Override
    public org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path arg0, long arg1)
            throws java.io.IOException {
        return fs.getFileChecksum(arg0, arg1);
    }

    @Override
    public void setVerifyChecksum(boolean arg0) {
        fs.setVerifyChecksum(arg0);
    }

    @Override
    public void setWriteChecksum(boolean arg0) {
        fs.setWriteChecksum(arg0);
    }

    @Override
    public org.apache.hadoop.fs.FsStatus getStatus() throws java.io.IOException {
        return fs.getStatus();
    }

    @Override
    public org.apache.hadoop.fs.FsStatus getStatus(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.getStatus(arg0);
    }

    @Override
    public org.apache.hadoop.fs.Path createSnapshot(org.apache.hadoop.fs.Path arg0, java.lang.String arg1)
            throws java.io.IOException {
        return fs.createSnapshot(arg0, arg1);
    }

    @Override
    public void renameSnapshot(org.apache.hadoop.fs.Path arg0, java.lang.String arg1, java.lang.String arg2)
            throws java.io.IOException {
        fs.renameSnapshot(arg0, arg1, arg2);
    }

    @Override
    public void deleteSnapshot(org.apache.hadoop.fs.Path arg0, java.lang.String arg1) throws java.io.IOException {
        fs.deleteSnapshot(arg0, arg1);
    }

    @Override
    public void modifyAclEntries(org.apache.hadoop.fs.Path arg0, java.util.List arg1) throws java.io.IOException {
        fs.modifyAclEntries(arg0, arg1);
    }

    @Override
    public void removeAclEntries(org.apache.hadoop.fs.Path arg0, java.util.List arg1) throws java.io.IOException {
        fs.removeAclEntries(arg0, arg1);
    }

    @Override
    public void removeDefaultAcl(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        fs.removeDefaultAcl(arg0);
    }

    @Override
    public void removeAcl(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        fs.removeAcl(arg0);
    }

    @Override
    public org.apache.hadoop.fs.permission.AclStatus getAclStatus(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.getAclStatus(arg0);
    }

    @Override
    public java.util.Map getXAttrs(org.apache.hadoop.fs.Path arg0, java.util.List arg1) throws java.io.IOException {
        return fs.getXAttrs(arg0, arg1);
    }

    @Override
    public java.util.Map getXAttrs(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.getXAttrs(arg0);
    }

    @Override
    public java.util.List listXAttrs(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.listXAttrs(arg0);
    }

    @Override
    public void removeXAttr(org.apache.hadoop.fs.Path arg0, java.lang.String arg1) throws java.io.IOException {
        fs.removeXAttr(arg0, arg1);
    }

    @Override
    public void setStoragePolicy(org.apache.hadoop.fs.Path arg0, java.lang.String arg1) throws java.io.IOException {
        fs.setStoragePolicy(arg0, arg1);
    }

    @Override
    public void unsetStoragePolicy(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        fs.unsetStoragePolicy(arg0);
    }

    @Override
    public org.apache.hadoop.fs.BlockStoragePolicySpi getStoragePolicy(org.apache.hadoop.fs.Path arg0)
            throws java.io.IOException {
        return fs.getStoragePolicy(arg0);
    }

    @Override
    public java.util.Collection getAllStoragePolicies() throws java.io.IOException {
        return fs.getAllStoragePolicies();
    }

    @Override
    public org.apache.hadoop.fs.Path getTrashRoot(org.apache.hadoop.fs.Path arg0) {
        return fs.getTrashRoot(arg0);
    }

    @Override
    public java.util.Collection getTrashRoots(boolean arg0) {
        return fs.getTrashRoots(arg0);
    }

    @Override
    public org.apache.hadoop.fs.StorageStatistics getStorageStatistics() {
        return fs.getStorageStatistics();
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.append(arg0);
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path arg0, int arg1) throws java.io.IOException {
        return fs.append(arg0, arg1);
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path arg0, int arg1,
                                                          org.apache.hadoop.util.Progressable arg2) throws java.io.IOException {
        return fs.append(arg0, arg1, arg2);
    }

    @Override
    public long getLength(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.getLength(arg0);
    }

    @Override
    public java.lang.String getName() {
        return fs.getName();
    }

    @Override
    public void concat(org.apache.hadoop.fs.Path arg0, org.apache.hadoop.fs.Path[] arg1) throws java.io.IOException {
        fs.concat(arg0, arg1);
    }

    @Override
    public boolean delete(org.apache.hadoop.fs.Path arg0, boolean arg1) throws java.io.IOException {
        return fs.delete(arg0, arg1);
    }

    @Override
    public boolean delete(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.delete(arg0);
    }

    @Override
    public void access(org.apache.hadoop.fs.Path arg0, org.apache.hadoop.fs.permission.FsAction arg1)
            throws org.apache.hadoop.security.AccessControlException, java.io.FileNotFoundException, java.io.IOException {
        fs.access(arg0, arg1);
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path arg0,
                                                          org.apache.hadoop.util.Progressable arg1) throws java.io.IOException {
        return fs.create(arg0, arg1);
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.create(arg0);
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path arg0, boolean arg1)
            throws java.io.IOException {
        return fs.create(arg0, arg1);
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path arg0, short arg1) throws java.io.IOException {
        return fs.create(arg0, arg1);
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path arg0, boolean arg1, int arg2, short arg3,
                                                          long arg4, org.apache.hadoop.util.Progressable arg5)
            throws java.io.IOException {
        return fs.create(arg0, arg1, arg2, arg3, arg4, arg5);
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path arg0,
                                                          org.apache.hadoop.fs.permission.FsPermission arg1, boolean arg2,
                                                          int arg3, short arg4, long arg5,
                                                          org.apache.hadoop.util.Progressable arg6) throws java.io.IOException {
        return fs.create(arg0, arg1, arg2, arg3, arg4, arg5, arg6);
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path arg0,
                                                          org.apache.hadoop.fs.permission.FsPermission arg1,
                                                          java.util.EnumSet arg2, int arg3, short arg4, long arg5,
                                                          org.apache.hadoop.util.Progressable arg6,
                                                          org.apache.hadoop.fs.Options.ChecksumOpt arg7)
            throws java.io.IOException {
        return fs.create(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7);
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path arg0,
                                                          org.apache.hadoop.fs.permission.FsPermission arg1,
                                                          java.util.EnumSet arg2, int arg3, short arg4, long arg5,
                                                          org.apache.hadoop.util.Progressable arg6) throws java.io.IOException {
        return fs.create(arg0, arg1, arg2, arg3, arg4, arg5, arg6);
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path arg0, short arg1,
                                                          org.apache.hadoop.util.Progressable arg2) throws java.io.IOException {
        return fs.create(arg0, arg1, arg2);
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path arg0, boolean arg1, int arg2)
            throws java.io.IOException {
        return fs.create(arg0, arg1, arg2);
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path arg0, boolean arg1, int arg2,
                                                          org.apache.hadoop.util.Progressable arg3) throws java.io.IOException {
        return fs.create(arg0, arg1, arg2, arg3);
    }

    @Override
    public org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path arg0, boolean arg1, int arg2, short arg3,
                                                          long arg4) throws java.io.IOException {
        return fs.create(arg0, arg1, arg2, arg3, arg4);
    }

    @Override
    public boolean exists(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.exists(arg0);
    }

    @Override
    public boolean isDirectory(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.isDirectory(arg0);
    }

    @Override
    public boolean isFile(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.isFile(arg0);
    }

    @Override
    public boolean createNewFile(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.createNewFile(arg0);
    }

    @Override
    public boolean deleteOnExit(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.deleteOnExit(arg0);
    }

    @Override
    public org.apache.hadoop.fs.RemoteIterator listFiles(org.apache.hadoop.fs.Path arg0, boolean arg1)
            throws java.io.FileNotFoundException, java.io.IOException {
        return fs.listFiles(arg0, arg1);
    }

    @Override
    public boolean mkdirs(org.apache.hadoop.fs.Path arg0, org.apache.hadoop.fs.permission.FsPermission arg1)
            throws java.io.IOException {
        return fs.mkdirs(arg0, arg1);
    }

    @Override
    public boolean mkdirs(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.mkdirs(arg0);
    }

    @Override
    public java.lang.String getScheme() {
        return fs.getScheme();
    }

    @Override
    public boolean rename(org.apache.hadoop.fs.Path arg0, org.apache.hadoop.fs.Path arg1) throws java.io.IOException {
        return fs.rename(arg0, arg1);
    }

    @Override
    public void setPermission(org.apache.hadoop.fs.Path arg0, org.apache.hadoop.fs.permission.FsPermission arg1)
            throws java.io.IOException {
        fs.setPermission(arg0, arg1);
    }

    @Override
    public boolean truncate(org.apache.hadoop.fs.Path arg0, long arg1) throws java.io.IOException {
        return fs.truncate(arg0, arg1);
    }

    @Override
    public org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.open(arg0);
    }

    @Override
    public org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path arg0, int arg1) throws java.io.IOException {
        return fs.open(arg0, arg1);
    }

    @Override
    public org.apache.hadoop.fs.Path resolvePath(org.apache.hadoop.fs.Path arg0) throws java.io.IOException {
        return fs.resolvePath(arg0);
    }
}
