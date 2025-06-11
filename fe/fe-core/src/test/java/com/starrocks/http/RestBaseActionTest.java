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

import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.thrift.TNetworkAddress;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestBaseActionTest {

    private RestBaseAction restBaseAction;
    private BaseRequest mockRequest;
    private BaseResponse mockResponse;
    private TNetworkAddress mockAddr;

    static class TestableRestBaseAction extends RestBaseAction {
        public TestableRestBaseAction() {
            super(null);
        }
    }

    @Before
    public void setUp() {
        restBaseAction = spy(new TestableRestBaseAction());
        mockRequest = mock(BaseRequest.class);
        mockResponse = mock(BaseResponse.class);
        mockAddr = mock(TNetworkAddress.class);

        when(mockAddr.getHostname()).thenReturn("127.0.0.1");
        when(mockAddr.getPort()).thenReturn(8030);

        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        when(mockHttpRequest.uri()).thenReturn("/api/mydb/testStreamLoad%E6%B5%8B%E8%AF%95/_stream_load");
        when(mockHttpRequest.method()).thenReturn(HttpMethod.GET);
        when(mockHttpRequest.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);

        HttpHeaders mockHeaders = mock(HttpHeaders.class);
        when(mockHttpRequest.headers()).thenReturn(mockHeaders);
        when(mockHeaders.containsValue(
            eq(HttpHeaderNames.CONNECTION),
            eq(HttpHeaderValues.KEEP_ALIVE),
            eq(true)
        )).thenReturn(false);  // or true

        when(mockRequest.getRequest()).thenReturn(mockHttpRequest);
        when(mockResponse.getContent()).thenReturn(new StringBuilder());

        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        when(mockRequest.getContext()).thenReturn(mockCtx);
    }

    @Test
    public void testRedirectTo() throws Exception {
        URI expectedUri = new URI("http", null, "127.0.0.1", 8030, "/api/mydb/testStreamLoad测试/_stream_load", null, null);
        String asciiUri = expectedUri.toASCIIString();

        restBaseAction.redirectTo(mockRequest, mockResponse, mockAddr);
        verify(mockResponse).updateHeader(HttpHeaderNames.LOCATION.toString(), asciiUri);
    }
}
