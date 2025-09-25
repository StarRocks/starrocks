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


package com.staros.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.staros.exception.ExceptionCode;
import com.staros.exception.NotImplementedStarException;
import com.staros.exception.StarException;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.AsciiString;
import io.netty.util.CharsetUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class HttpService {
    public static final Logger LOG = LogManager.getLogger(HttpService.class);
    private HttpDispatcher httpDispatcher;

    public HttpService(StarManager manager) {
        this.httpDispatcher = new HttpDispatcher(manager);
    }

    public HttpResponse starmgrHttpService(HttpRequest request) {
        HttpResponseStatus status = HttpResponseStatus.OK;
        Object object = null;
        try {
            object = httpDispatcher.getObject(request.uri());
        } catch (StarException e) {
            if (e.getExceptionCode() == ExceptionCode.INVALID_ARGUMENT) {
                status = HttpResponseStatus.BAD_REQUEST;
            } else if (e.getExceptionCode() == ExceptionCode.NOT_EXIST) {
                status = HttpResponseStatus.NOT_FOUND;
            } else {
                status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            }
        }

        return createResponse(object, request, status);
    }

    public static HttpResponse createResponse(Object object, HttpRequest request, HttpResponseStatus status) {
        AsciiString contentType = HttpHeaderValues.APPLICATION_JSON;
        try {
            contentType = getContentTypeFromHeader(request);
        } catch (NotImplementedStarException e) {
            status = HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE;
        }

        if (object == null) {
            return createEmptyResponse(contentType, status);
        }

        String content = "";
        try {
            if (contentType == HttpHeaderValues.APPLICATION_JSON) {
                content = serializeToJson(object);
            } else {
                content = serializeToText(object);
            }
        } catch (Exception e) {
            content = "Error serializing object: " + e.getMessage();
            status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
        }

        return createResponse(content, contentType, status);
    }

    public static AsciiString getContentTypeFromHeader(HttpRequest request) throws StarException {
        String acceptHeader = request.headers().get(HttpHeaderNames.ACCEPT);
        if (acceptHeader != null) {
            acceptHeader = acceptHeader.toLowerCase();
            if (acceptHeader.contains("application/json") || acceptHeader.contains("*/*")) {
                return HttpHeaderValues.APPLICATION_JSON;
            } else if (acceptHeader.contains("text/plain")) {
                return HttpHeaderValues.TEXT_PLAIN;
            }
            throw new NotImplementedStarException("Unsupported Accept header value");
        }
        return HttpHeaderValues.APPLICATION_JSON;
    }

    public static HttpResponse createEmptyResponse(AsciiString contentType, HttpResponseStatus status) {
        String content = contentType == HttpHeaderValues.APPLICATION_JSON ? "{}" : "";
        return createResponse(content, contentType, status);
    }

    public static HttpResponse createResponse(String content, AsciiString contentType, HttpResponseStatus status) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                status,
                Unpooled.copiedBuffer(content, CharsetUtil.UTF_8));

        response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
        return response;
    }

    public static String serializeToJson(Object object) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode arrayNode = mapper.createArrayNode();

        if (object instanceof List) {
            List<?> objectList = (List<?>) object;
            for (Object item : objectList) {
                String jsonString = JsonFormat.printer().includingDefaultValueFields().print((Message) item);
                arrayNode.add(mapper.readTree(jsonString));
            }
        } else {
            String jsonString = JsonFormat.printer().includingDefaultValueFields().print((Message) object);
            arrayNode.add(mapper.readTree(jsonString));
        }

        return arrayNode.toString();
    }

    public static String serializeToText(Object object) throws Exception {
        // Warning! Protobuf serialization to text will not print default values.
        StringBuilder contentBuilder = new StringBuilder();
        if (object instanceof List) {
            List<?> objectList = (List<?>) object;
            for (Object item : objectList) {
                // Items are separated by \n.
                contentBuilder.append(item.toString());
            }
        } else {
            contentBuilder.append(object.toString());
        }
        return contentBuilder.toString();
    }
}