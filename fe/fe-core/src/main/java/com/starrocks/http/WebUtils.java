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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/WebUtils.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.http;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.path.PathTrie;
import io.netty.handler.codec.http.QueryStringDecoder;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WebUtils {
    public static final PathTrie.Decoder REST_DECODER = WebUtils::decodeComponent;

    private static final Set<String> SANITIZED_HTTP_PARAMS =
            ImmutableSet.of("file_id", "cluster_id", "token");

    // The query parameters of this path are config names and values; credential ones are masked as in
    // ConfigBase.maskIfSensitive.
    private static final String SET_CONFIG_PATH = "/api/_set_config";

    /**
     * Decodes a bit of an URL encoded by a browser.
     * <p/>
     * This is equivalent to calling {@link #decodeComponent(String, Charset)}
     * with the UTF-8 charset (recommended to comply with RFC 3986, Section 2).
     *
     * @param s The string to decode (can be empty).
     * @return The decoded string, or {@code s} if there's nothing to decode.
     * If the string to decode is {@code null}, returns an empty string.
     * @throws IllegalArgumentException if the string contains a malformed
     *                                  escape sequence.
     */
    public static String decodeComponent(final String s) {
        return decodeComponent(s, Charsets.UTF_8);
    }

    /**
     * Decodes a bit of an URL encoded by a browser.
     * <p/>
     * The string is expected to be encoded as per RFC 3986, Section 2.
     * This is the encoding used by JavaScript functions {@code encodeURI}
     * and {@code encodeURIComponent}, but not {@code escape}.  For example
     * in this encoding, &eacute; (in Unicode {@code U+00E9} or in UTF-8
     * {@code 0xC3 0xA9}) is encoded as {@code %C3%A9} or {@code %c3%a9}.
     * <p/>
     * This is essentially equivalent to calling
     * <code>{@link java.net.URLDecoder URLDecoder}.{@link
     * java.net.URLDecoder#decode(String, String)}</code>
     * except that it's over 2x faster and generates less garbage for the GC.
     * Actually this function doesn't allocate any memory if there's nothing
     * to decode, the argument itself is returned.
     *
     * @param s       The string to decode (can be empty).
     * @param charset The charset to use to decode the string (should really
     *                be {@link Charsets#UTF_8}.
     * @return The decoded string, or {@code s} if there's nothing to decode.
     * If the string to decode is {@code null}, returns an empty string.
     * @throws IllegalArgumentException if the string contains a malformed
     *                                  escape sequence.
     */
    @SuppressWarnings("fallthrough")
    public static String decodeComponent(final String s, final Charset charset) {
        if (s == null) {
            return "";
        }
        final int size = s.length();
        boolean modified = false;
        for (int i = 0; i < size; i++) {
            final char c = s.charAt(i);
            switch (c) {
                case '%':
                    i++;  // We can skip at least one char, e.g. `%%'.
                    // Fall through.
                case '+':
                    modified = true;
                    break;
            }
        }
        if (!modified) {
            return s;
        }
        final byte[] buf = new byte[size];
        int pos = 0;  // position in `buf'.
        for (int i = 0; i < size; i++) {
            char c = s.charAt(i);
            switch (c) {
                case '+':
                    buf[pos++] = ' ';  // "+" -> " "
                    break;
                case '%':
                    if (i == size - 1) {
                        throw new IllegalArgumentException("unterminated escape"
                                + " sequence at end of string: " + s);
                    }
                    c = s.charAt(++i);
                    if (c == '%') {
                        buf[pos++] = '%';  // "%%" -> "%"
                        break;
                    } else if (i == size - 1) {
                        throw new IllegalArgumentException("partial escape"
                                + " sequence at end of string: " + s);
                    }
                    c = decodeHexNibble(c);
                    final char c2 = decodeHexNibble(s.charAt(++i));
                    if (c == Character.MAX_VALUE || c2 == Character.MAX_VALUE) {
                        throw new IllegalArgumentException(
                                "invalid escape sequence `%" + s.charAt(i - 1)
                                        + s.charAt(i) + "' at index " + (i - 2)
                                        + " of: " + s);
                    }
                    c = (char) (c * 16 + c2);
                    // Fall through.
                default:
                    buf[pos++] = (byte) c;
                    break;
            }
        }
        return new String(buf, 0, pos, charset);
    }

    /**
     * Helper to decode half of a hexadecimal number from a string.
     *
     * @param c The ASCII character of the hexadecimal number to decode.
     *          Must be in the range {@code [0-9a-fA-F]}.
     * @return The hexadecimal value represented in the ASCII character
     * given, or {@link Character#MAX_VALUE} if the character is invalid.
     */
    private static char decodeHexNibble(final char c) {
        if ('0' <= c && c <= '9') {
            return (char) (c - '0');
        } else if ('a' <= c && c <= 'f') {
            return (char) (c - 'a' + 10);
        } else if ('A' <= c && c <= 'F') {
            return (char) (c - 'A' + 10);
        } else {
            return Character.MAX_VALUE;
        }
    }

    /**
     * This method sanitizes the input URI by replacing the values of certain query parameters with "*".
     * The parameters to be sanitized are listed in `SANITIZED_HTTP_PARAMS`. For /api/_set_config, the values of
     * credential configs are also replaced, with {@link ConfigBase#maskIfSensitive(String, String)}.
     * <p>
     * The query is parsed the same way as {@link BaseRequest} parses it for the actions, and rebuilt from the parsed
     * parameters: a value an action reads as a sanitized parameter is always masked, however the query is written
     * (e.g. with ';' as a separator, or an escaped '&' in a value), and what is not parsed as a parameter is left
     * out. If the URI has no query, it is returned as is, except that anything after a '?' is hidden.
     *
     * @param uri The input URI to be sanitized.
     * @return The sanitized URI with the values of certain query parameters replaced.
     * @throws URISyntaxException If the input string can't be parsed as a URI reference.
     */
    public static String sanitizeHttpReqUri(String uri) throws URISyntaxException {
        URI inputUri = new URI(uri);
        if (inputUri.getRawQuery() == null) {
            // What follows a '?' may still not be a query to java.net.URI, e.g. in an opaque uri (http:foo?...) or a
            // fragment (/api#frag?...), but it would be logged as is: hide it.
            return uri.indexOf('?') < 0 ? uri : hideHttpReqQuery(uri);
        }
        boolean isSetConfig = SET_CONFIG_PATH.equals(inputUri.getPath());

        QueryStringDecoder decoder = BaseRequest.newQueryStringDecoder(uri);
        StringBuilder sanitized = new StringBuilder(decoder.rawPath());
        char separator = '?';
        for (Map.Entry<String, List<String>> param : decoder.parameters().entrySet()) {
            String key = param.getKey();
            for (String value : param.getValue()) {
                if (SANITIZED_HTTP_PARAMS.contains(key)) {
                    value = "*";
                } else if (isSetConfig) {
                    value = ConfigBase.maskIfSensitive(key, value);
                }
                sanitized.append(separator).append(URLEncoder.encode(key, StandardCharsets.UTF_8))
                        .append('=').append(URLEncoder.encode(value, StandardCharsets.UTF_8));
                separator = '&';
            }
        }
        return sanitized.toString();
    }

    /**
     * {@link #sanitizeHttpReqUri(String)} for a log line. If the uri cannot be parsed, only its path is returned,
     * since the query may hold a credential.
     */
    public static String sanitizeHttpReqUriForLog(String uri) {
        try {
            return sanitizeHttpReqUri(uri);
        } catch (URISyntaxException e) {
            // Not e.getMessage(): it quotes the whole input.
            return hideHttpReqQuery(uri) + " (failed to sanitize uri: " + e.getReason() + ")";
        }
    }

    // The uri with its query, if any, replaced, for when it cannot be sanitized.
    public static String hideHttpReqQuery(String uri) {
        int queryStart = uri.indexOf('?');
        return queryStart < 0 ? uri : uri.substring(0, queryStart) + "?<query not shown>";
    }
}
