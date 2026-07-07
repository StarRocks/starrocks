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

package com.starrocks.context.embedding;

import com.starrocks.context.error.ContextErrorCode;
import com.starrocks.context.error.ContextException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link FeEmbeddingClient#sendWithRetry}, the recovery path for a stale pooled
 * connection that the embedding provider/NAT silently closed while idle (manifested as
 * {@code VECTOR_NOT_READY: FE embedding HTTP send failed: Connection reset} on the first
 * bulk-import after idle).
 */
public class FeEmbeddingClientTest {

    @SuppressWarnings("unchecked")
    private static HttpResponse<String> okResponse() {
        HttpResponse<String> resp = Mockito.mock(HttpResponse.class);
        when(resp.statusCode()).thenReturn(200);
        return resp;
    }

    private static HttpRequest dummyRequest() {
        return HttpRequest.newBuilder().uri(URI.create("http://127.0.0.1:1/v1/embeddings")).GET().build();
    }

    @Test
    public void testRetriesThenSucceeds() throws Exception {
        HttpClient client = Mockito.mock(HttpClient.class);
        when(client.<String>send(any(), any()))
                .thenThrow(new IOException("Connection reset"))
                .thenReturn(okResponse());

        HttpResponse<String> resp = FeEmbeddingClient.sendWithRetry(client, dummyRequest());

        Assertions.assertEquals(200, resp.statusCode());
        verify(client, times(2)).send(any(), any());
    }

    @Test
    public void testAllAttemptsFail() throws Exception {
        HttpClient client = Mockito.mock(HttpClient.class);
        when(client.<String>send(any(), any())).thenThrow(new IOException("Connection reset"));

        ContextException ex = Assertions.assertThrows(ContextException.class,
                () -> FeEmbeddingClient.sendWithRetry(client, dummyRequest()));

        Assertions.assertEquals(ContextErrorCode.VECTOR_NOT_READY, ex.getCode());
        Assertions.assertTrue(ex.getMessage().contains("after " + FeEmbeddingClient.MAX_SEND_ATTEMPTS + " attempts"),
                ex.getMessage());
        verify(client, times(FeEmbeddingClient.MAX_SEND_ATTEMPTS)).send(any(), any());
    }

    @Test
    public void testInterruptedDoesNotRetry() throws Exception {
        HttpClient client = Mockito.mock(HttpClient.class);
        when(client.<String>send(any(), any())).thenThrow(new InterruptedException("interrupted"));

        ContextException ex = Assertions.assertThrows(ContextException.class,
                () -> FeEmbeddingClient.sendWithRetry(client, dummyRequest()));

        Assertions.assertEquals(ContextErrorCode.VECTOR_NOT_READY, ex.getCode());
        Assertions.assertTrue(ex.getMessage().contains("interrupted"), ex.getMessage());
        // The interrupt flag must be restored, and there must be no retry on interruption.
        Assertions.assertTrue(Thread.interrupted());
        verify(client, times(1)).send(any(), any());
    }

    @Test
    public void testTimeoutDoesNotRetry() throws Exception {
        HttpClient client = Mockito.mock(HttpClient.class);
        when(client.<String>send(any(), any())).thenThrow(new HttpTimeoutException("request timed out"));

        ContextException ex = Assertions.assertThrows(ContextException.class,
                () -> FeEmbeddingClient.sendWithRetry(client, dummyRequest()));

        Assertions.assertEquals(ContextErrorCode.VECTOR_NOT_READY, ex.getCode());
        Assertions.assertTrue(ex.getMessage().contains("timed out"), ex.getMessage());
        // A timeout is the provider being slow/black-holing — fail fast, do NOT retry.
        verify(client, times(1)).send(any(), any());
    }
}
