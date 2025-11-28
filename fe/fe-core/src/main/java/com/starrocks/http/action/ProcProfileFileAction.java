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
import org.apache.http.HttpStatus;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

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
        String nodeParam = request.getSingleParameter("node");

        // If BE node is specified, proxy the request to BE
        if (!Strings.isNullOrEmpty(nodeParam) && nodeParam.startsWith("BE:")) {
            proxyRequestToBE(request, response, nodeParam, filename);
            return;
        }

        // Otherwise, serve local FE file
        String profileLogDir = Config.sys_log_dir + "/proc_profile";
        File profileFile = new File(profileLogDir, filename);

        if (Strings.isNullOrEmpty(filename) || !isValidFilename(filename) || !profileFile.exists()
                || !profileFile.isFile()) {
            LOG.error("invalid filename: {}", filename);
            writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
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

    private void proxyRequestToBE(BaseRequest request, BaseResponse response, String beNodeId, String filename) {
        // Parse BE node: "BE:host:port"
        String[] parts = beNodeId.substring(3).split(":");
        if (parts.length != 2) {
            LOG.warn("Invalid BE node format: {}", beNodeId);
            writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        String host = parts[0];
        int port;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            LOG.warn("Invalid BE port: {}", parts[1]);
            writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        try {
            // URL encode the filename to handle special characters
            String encodedFilename = java.net.URLEncoder.encode(filename, StandardCharsets.UTF_8.toString());
            String url = "http://" + host + ":" + port + "/proc_profile/file?filename=" + encodedFilename;

            // Use a longer timeout for proc profile file requests since BE may need to convert
            // pprof files to flame graphs, which can take longer than the default 5 seconds
            // Set timeout to 60 seconds (60000ms) for proc profile file requests
            RequestConfig requestConfig =
                    RequestConfig.custom()
                            .setCookieSpec(CookieSpecs.IGNORE_COOKIES)
                            .setExpectContinueEnabled(Boolean.TRUE)
                            .setTargetPreferredAuthSchemes(
                                    Arrays.asList(AuthSchemes.NTLM, AuthSchemes.DIGEST, AuthSchemes.SPNEGO))
                            .setProxyPreferredAuthSchemes(Arrays.asList(AuthSchemes.BASIC, AuthSchemes.SPNEGO))
                            .setConnectTimeout(10000) // 10 seconds for connection
                            .setSocketTimeout(60000) // 60 seconds for socket read (for pprof conversion)
                            .setConnectionRequestTimeout(10000) // 10 seconds for connection request
                            .setRedirectsEnabled(true)
                            .build();

            try (CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(requestConfig).build()) {
                HttpGet httpGet = new HttpGet(url);
                try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
                    int code = httpResponse.getStatusLine().getStatusCode();
                    String result = EntityUtils.toString(httpResponse.getEntity(), StandardCharsets.UTF_8);

                    if (code == HttpStatus.SC_OK) {
                        // Forward the response from BE
                        // BE returns HTML for pprof files (flame graph) or binary for tar.gz files
                        // We'll set content type based on filename
                        if (filename.endsWith(".pprof.gz") || filename.contains("-pprof")) {
                            // Pprof files are converted to HTML flame graphs by BE
                            response.updateHeader(HttpHeaderNames.CONTENT_TYPE.toString(), "text/html; charset=utf-8");
                        } else {
                            // Other files (tar.gz) are served as binary
                            response.updateHeader(HttpHeaderNames.CONTENT_TYPE.toString(), "application/octet-stream");
                        }

                        response.appendContent(result);
                        writeResponse(request, response);
                    } else {
                        LOG.error("BE returned error code {} for file: {}", code, filename);
                        writeResponse(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                    }
                }
            }

        } catch (Exception e) {
            LOG.error("Error proxying request to BE {}:{} for file: {}", host, port, filename, e);
            writeResponse(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private boolean isValidFilename(String filename) {
        return !Strings.isNullOrEmpty(filename)
                && !filename.contains("..")
                && !filename.contains("/")
                && !filename.contains("\\")
                && !INSECURE_FILENAME.matcher(filename).matches()
                && filename.endsWith(".tar.gz")
                && (filename.startsWith(CPU_FILE_NAME_PREFIX) || filename.startsWith(MEM_FILE_NAME_PREFIX));
    }

    private String extractHtmlFromTarGz(File tarGzFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(tarGzFile);
                GzipCompressorInputStream gzis = new GzipCompressorInputStream(fis);
                TarArchiveInputStream tis = new TarArchiveInputStream(gzis)) {

            TarArchiveEntry entry;
            while ((entry = tis.getNextEntry()) != null) {
                if (!entry.isDirectory() && entry.getName().endsWith(".html")) {
                    // Found the HTML file, read its content
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    byte[] buffer = new byte[8192];
                    int len;
                    while ((len = tis.read(buffer)) > 0) {
                        baos.write(buffer, 0, len);
                    }
                    return baos.toString(StandardCharsets.UTF_8);
                }
            }
        }

        return null;
    }
}