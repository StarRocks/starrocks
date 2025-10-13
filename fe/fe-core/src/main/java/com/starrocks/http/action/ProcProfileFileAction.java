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

package com.starrocks.http.action;

import com.google.common.base.Strings;
import com.google.re2j.Pattern;
import com.starrocks.common.Config;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ProcProfileFileAction extends WebBaseAction {
    private static final Logger LOG = LogManager.getLogger(ProcProfileFileAction.class);
    
    private static final String CPU_FILE_NAME_PREFIX = "cpu-profile-";
    private static final String MEM_FILE_NAME_PREFIX = "mem-profile-";
    private static final Pattern INSECURE_FILENAME = Pattern.compile(".*[<>&\"].*");

    public ProcProfileFileAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/proc_profile/file", new ProcProfileFileAction(controller));
    }

    @Override
    public void executeGet(BaseRequest request, BaseResponse response) {
        String filename = request.getSingleParameter("filename");
        
        if (Strings.isNullOrEmpty(filename)) {
            LOG.error("Missing filename parameter");
            writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        // Validate filename security
        if (!isValidFilename(filename)) {
            LOG.error("Invalid filename: {}", filename);
            writeResponse(request, response, HttpResponseStatus.FORBIDDEN);
            return;
        }

        String profileLogDir = Config.sys_log_dir + "/proc_profile";
        File profileFile = new File(profileLogDir, filename);
        
        if (!profileFile.exists() || !profileFile.isFile()) {
            LOG.error("Profile file not found: {}", profileFile.getAbsolutePath());
            writeResponse(request, response, HttpResponseStatus.NOT_FOUND);
            return;
        }

        try {
            String htmlContent = extractHtmlFromTarGz(profileFile);
            if (htmlContent == null) {
                LOG.error("Failed to extract HTML content from: {}", filename);
                writeResponse(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                return;
            }

            response.updateHeader(HttpHeaderNames.CONTENT_TYPE.toString(), "text/html; charset=utf-8");
            response.appendContent(htmlContent);
            writeResponse(request, response);
            
        } catch (Exception e) {
            LOG.error("Error serving profile file: {}", filename, e);
            writeResponse(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private boolean isValidFilename(String filename) {
        if (Strings.isNullOrEmpty(filename)) {
            return false;
        }

        // Check for directory traversal attempts
        if (filename.contains("..") || filename.contains("/") || filename.contains("\\")) {
            return false;
        }

        // Check for dangerous characters
        if (INSECURE_FILENAME.matcher(filename).matches()) {
            return false;
        }

        // Must be a valid profile file
        if (!filename.endsWith(".tar.gz")) {
            return false;
        }

        // Must start with known prefixes
        if (!filename.startsWith(CPU_FILE_NAME_PREFIX) && !filename.startsWith(MEM_FILE_NAME_PREFIX)) {
            return false;
        }

        return true;
    }

    private String extractHtmlFromTarGz(File tarGzFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(tarGzFile);
             GzipCompressorInputStream gzis = new GzipCompressorInputStream(fis);
             TarArchiveInputStream tis = new TarArchiveInputStream(gzis)) {

            TarArchiveEntry entry;
            while ((entry = tis.getNextTarEntry()) != null) {
                if (!entry.isDirectory() && entry.getName().endsWith(".html")) {
                    // Found the HTML file, read its content
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    byte[] buffer = new byte[8192];
                    int len;
                    while ((len = tis.read(buffer)) > 0) {
                        baos.write(buffer, 0, len);
                    }
                    return baos.toString(StandardCharsets.UTF_8.name());
                }
            }
        }
        
        return null;
    }
}