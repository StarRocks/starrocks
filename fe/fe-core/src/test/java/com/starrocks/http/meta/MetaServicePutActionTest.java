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

package com.starrocks.http.meta;

import com.starrocks.common.Config;
import com.starrocks.http.BaseAction;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.action.WebBaseAction;
import com.starrocks.leader.MetaHelper;
import com.starrocks.persist.ImageFormatVersion;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.MockImageSource;
import com.starrocks.utframe.UtFrameUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * /put is how a follower fetches the image the leader has just published. A damaged image must be refused
 * there too, or one bad copy on the leader reaches every follower.
 */
public class MetaServicePutActionTest {
    private final List<HttpResponseStatus> statuses = new ArrayList<>();
    private String savedMetaDir;
    private String metaDir;
    private File imageDir;

    @BeforeAll
    public static void setUpClass() {
        UtFrameUtils.setUpForPersistTest();
    }

    @BeforeEach
    public void setUp() {
        savedMetaDir = Config.meta_dir;
        metaDir = "meta_dir_test_" + UUID.randomUUID();
        Config.meta_dir = metaDir;
        imageDir = new File(MetaHelper.getImageFileDir(true));
        Assertions.assertTrue(imageDir.mkdirs());
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return false;
            }

            @Mock
            public Long getMaxJournalId() {
                return 1000L;
            }
        };
        // the real ones write to the netty channel of the request
        new MockUp<WebBaseAction>() {
            @Mock
            public void writeResponse(BaseRequest request, BaseResponse response) {
                statuses.add(HttpResponseStatus.OK);
            }
        };
        new MockUp<BaseAction>() {
            @Mock
            public void writeResponse(BaseRequest request, BaseResponse response, HttpResponseStatus status) {
                statuses.add(status);
            }
        };
    }

    @AfterEach
    public void tearDown() {
        Config.meta_dir = savedMetaDir;
        FileUtils.deleteQuietly(new File(metaDir));
    }

    @Test
    public void testPutStoresAV2ImageThatMatchesItsChecksum() throws Exception {
        byte[] image = MockImageSource.imageBytes();
        try (MockImageSource leader = new MockImageSource(image, MockImageSource.checksum(image), 100,
                ImageFormatVersion.v2)) {
            put(leader);
        }
        Assertions.assertEquals(List.of(HttpResponseStatus.OK), statuses);
        Assertions.assertArrayEquals(image, Files.readAllBytes(new File(imageDir, "image.100").toPath()));
    }

    @Test
    public void testPutRefusesADamagedV2Image() throws Exception {
        byte[] image = MockImageSource.imageBytes();
        try (MockImageSource leader = new MockImageSource(MockImageSource.damaged(image),
                MockImageSource.checksum(image), 100, ImageFormatVersion.v2)) {
            put(leader);
        }
        Assertions.assertEquals(List.of(HttpResponseStatus.INTERNAL_SERVER_ERROR), statuses);
        Assertions.assertFalse(new File(imageDir, "image.100").exists());
    }

    private void put(MockImageSource leader) {
        Map<String, String> params = new HashMap<>();
        params.put("port", String.valueOf(leader.port()));
        params.put("version", "100");
        params.put("image_format_version", ImageFormatVersion.v2.name());
        BaseRequest request = mock(BaseRequest.class);
        when(request.getHostString()).thenReturn(leader.host());
        when(request.getSingleParameter(anyString())).thenAnswer(inv -> params.get(inv.getArgument(0, String.class)));
        new MetaService.PutAction(null).executeGet(request, mock(BaseResponse.class));
    }
}
