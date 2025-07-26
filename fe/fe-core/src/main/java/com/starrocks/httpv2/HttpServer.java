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

package com.starrocks.httpv2;


import com.starrocks.service.FrontendOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@EnableConfigurationProperties
@ServletComponentScan
public class HttpServer extends SpringBootServletInitializer {

    private static final Logger LOG = LogManager.getLogger(HttpServer.class);

    private int port;

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(HttpServer.class);
    }

    public Map<String, Object> setup() {
        Map<String, Object> properties = new HashMap<>();

        String anyLocalAddr = FrontendOptions.isBindIPV6() ? "::0" : "0.0.0.0";

        properties.put("server.address", anyLocalAddr);
        properties.put("server.port", this.port);

        properties.put("server.servlet.context-path", "/");
        properties.put("spring.resources.static-locations", "classpath:/static");
        properties.put("spring.http.encoding.charset", "UTF-8");
        properties.put("spring.http.encoding.enabled", true);
        properties.put("spring.http.encoding.force", true);

        return properties;
    }

    public void start() {

        Map<String, Object> properties = setup();
        LOG.info("Starting HTTP server with properties: {}", properties);
        new SpringApplicationBuilder()
                .sources(HttpServer.class)
                .properties(properties)
                .run();
    }
}

