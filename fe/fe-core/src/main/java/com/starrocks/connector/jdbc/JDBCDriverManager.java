package com.starrocks.connector.jdbc;

import com.starrocks.catalog.JDBCResource;
import com.starrocks.common.JDBCDriverException;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;


public class JDBCDriverManager {

    private static Logger LOG = LogManager.getLogger(JDBCDriverManager.class);

    private static final JDBCDriverManager INSTANCE = new JDBCDriverManager();

    private JDBCDriverManager() {
    }

    public static JDBCDriverManager getInstance() {
        return INSTANCE;
    }

    private String driverDir;

    private final Lock lock = new ReentrantLock();

    private static final String TMP_FILE_SUFFIX = ".tmp";
    private static final String JAR_FILE_SUFFIX = ".jar";

    public void init(String driverDir) throws JDBCDriverException {
        lock.lock();
        this.driverDir = driverDir;
        File dir = new File(driverDir);
        if (!dir.exists()) {
            dir.mkdirs();
            LOG.info("jdbc driver dir is not exist, create it");
        }

        try (Stream<Path> stream = Files.walk(dir.toPath())) {
            stream.filter(each -> !Files.isDirectory(each))
                    .forEach(each -> {
                        if (each.getFileName().toString().endsWith(TMP_FILE_SUFFIX)) {
                            // delete tmp file
                            LOG.info("try to remove temporary file: {}", each.getFileName());
                            try {
                                Files.delete(each);
                            } catch (IOException e) {
                                LOG.error("failed to delete temporary file: {}", each.getFileName(), e);
                                throw new RuntimeException(e);
                            }
                        }
                    });
        } catch (IOException e) {
            LOG.error("Fail to initialize JDBC driver manger", e);
            throw new JDBCDriverException();
        } finally {
            lock.unlock();
        }

    }


    // ${name}.jar
    private String parseName(String fileName) {
        return fileName.substring(fileName.lastIndexOf("/") + 1, fileName.lastIndexOf(JAR_FILE_SUFFIX));
    }

    private String getLocalFullPath(String fileName, String suffix) {
        return suffix + "/" + fileName;
    }

    /**
     * 1. download from url and record checksum
     * 2. compare checksum with local file
     * 3. if eq, remove tmp file, else rename local file
     * 4. set checksum
     * @param properties
     * @throws JDBCDriverException
     * @throws IOException
     */
    public void createNewDriver(Map<String, String> properties)
            throws JDBCDriverException, IOException {
        String driverUrl = properties.get(JDBCResource.DRIVER_URL);
        String name = parseName(driverUrl);
        // download tmp file
        String tmpFile = name + "_" + UUID.randomUUID() + TMP_FILE_SUFFIX;
        String checksum = doDownload(driverUrl, getLocalFullPath(tmpFile, driverDir));
        String fileName = name + JAR_FILE_SUFFIX;
        String localChecksum = computeChecksumFromLocal(getLocalFullPath(fileName,driverDir));
        if (!Objects.equals(checksum, localChecksum)) {
            Files.move(Paths.get(getLocalFullPath(tmpFile, driverDir)), Paths.get(getLocalFullPath(fileName,driverDir)));
        } else {
            Files.deleteIfExists(Paths.get(getLocalFullPath(tmpFile, driverDir)));
        }
        properties.put(JDBCResource.CHECK_SUM, checksum);
    }

    /**
     * 1. compare checksum from local file with properties
     * 2. if eq, return driver location. else download from url and rename local file
     * @param properties
     * @return driver location
     * @throws JDBCDriverException
     * @throws IOException
     */
    public String checkDriver(Map<String, String> properties)
            throws JDBCDriverException, IOException {
        String driverUrl = properties.get(JDBCResource.DRIVER_URL);
        String name = parseName(driverUrl);
        String fileName = name + JAR_FILE_SUFFIX;
        String checksum = properties.get(JDBCResource.CHECK_SUM);
        String localChecksum = computeChecksumFromLocal(getLocalFullPath(fileName, driverDir));
        String tmpFile = name + "_" + UUID.randomUUID() + TMP_FILE_SUFFIX;
        if (!Objects.equals(checksum, localChecksum)) {
            doDownload(driverUrl, getLocalFullPath(tmpFile, driverDir));
            Files.move(Paths.get(getLocalFullPath(tmpFile, driverDir)), Paths.get(getLocalFullPath(fileName, driverDir)));
        }
        return getLocalFullPath(fileName,driverDir);
    }


    private String computeChecksumFromLocal(String fullPath) throws JDBCDriverException {
        if(!Files.exists(Paths.get(fullPath))) {
            return "";
        }
        try {
            URL url = new URL("file://" + fullPath);
            URLConnection urlConnection = url.openConnection();
            InputStream inputStream = urlConnection.getInputStream();

            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] buf = new byte[4096];
            int bytesRead = 0;
            do {
                bytesRead = inputStream.read(buf);
                if (bytesRead < 0) {
                    break;
                }
                digest.update(buf, 0, bytesRead);
            } while (true);

            return Hex.encodeHexString(digest.digest());
        } catch (Exception e) {
            LOG.error("Cannot get driver from url: {}", e);
            throw new JDBCDriverException();
        }
    }


    // down file and return check sum
    private String doDownload(String url, String targetFile) throws JDBCDriverException {
        File file = new File(targetFile);
        String actualChecksum;
        try (BufferedInputStream in = new BufferedInputStream(new URL(url).openStream());
                FileOutputStream out = new FileOutputStream(file)) {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer, 0, 1024)) != -1) {
                out.write(buffer, 0, bytesRead);
                digest.update(buffer, 0, bytesRead);
            }
            actualChecksum = Hex.encodeHexString(digest.digest());
        } catch (IOException | NoSuchAlgorithmException e) {
            LOG.error("do download error", e);
            file.delete();
            throw new JDBCDriverException();
        }
        return actualChecksum;
    }

}
