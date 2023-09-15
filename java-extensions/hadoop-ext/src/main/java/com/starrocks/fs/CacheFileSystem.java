package com.starrocks.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

public class CacheFileSystem extends FileSystem {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CacheFileSystem.class);

    FileSystem fs;
    Cache.Key key;

    static final Cache CACHE = new Cache();

    static FileSystem createRealFileSystem(URI uri, Configuration conf) throws IOException {
        String scheme = parseSchemeFromURI(uri, conf);
        String implKey = String.format(HadoopExt.FS_IMPL_FMT, scheme);
        String implValue = conf.get(implKey);
        String disableKey = String.format(HadoopExt.FS_IMPL_DISABLE_CACHE_FMT, scheme);
        String disableValue = conf.get(disableKey);

        if (HadoopExt.isS3Scheme(scheme)) {
            conf.set(implKey, HadoopExt.FS_S3A_FILESYSTEM);
        } else {
            conf.unset(implKey);
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

    public CacheFileSystem(Configuration conf) {
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        key = Cache.buildCacheKey(uri, conf);
        fs = CACHE.get(uri, conf, key);
        LOGGER.info(String.format("%s Create CacheFileSystem = %s", HadoopExt.LOGGER_MESSAGE_PREFIX, this));
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
        fs.close();
        CACHE.remove(key, fs);
    }

    @Override
    public String toString() {
        return String.format("CacheFileSystem(key = %s, fs = %s)", key, fs);
    }

    // =================== proxy methods ======================
    @Override
    public URI getUri() {
        return fs.getUri();
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return fs.open(f, bufferSize);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication,
                                     long blockSize, Progressable progress) throws IOException {
        return fs.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        return fs.append(f, bufferSize, progress);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return fs.rename(src, dst);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return fs.delete(f, recursive);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        return fs.listStatus(f);
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        fs.setWorkingDirectory(new_dir);
    }

    @Override
    public Path getWorkingDirectory() {
        return fs.getWorkingDirectory();
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        return fs.mkdirs(f, permission);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return fs.getFileStatus(f);
    }
}
