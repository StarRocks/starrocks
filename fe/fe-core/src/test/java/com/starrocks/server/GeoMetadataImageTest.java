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

package com.starrocks.server;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.View;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.type.GeoType;
import com.starrocks.type.GeoTypeDescriptor;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

class GeoMetadataImageTest {
    private static final long IMAGE_ID = 42;

    @TempDir
    Path directory;

    private String previousMetaDir;
    private String previousPluginDir;
    private boolean previousAsyncReload;
    private GlobalStateMgr state;
    private MockedStatic<GlobalStateMgr> globalState;
    private Path imageDirectory;
    private Path imageFile;
    private Path checksumFile;

    @BeforeEach
    void setUp() throws Exception {
        previousMetaDir = Config.meta_dir;
        previousPluginDir = Config.plugin_dir;
        previousAsyncReload = Config.enable_mv_post_image_reload_cache;
        Config.meta_dir = directory.resolve("meta").toString();
        Config.plugin_dir = directory.resolve("plugins").toString();
        Config.enable_mv_post_image_reload_cache = false;
        imageDirectory = directory.resolve("image/v2");
        Files.createDirectories(imageDirectory);
        imageFile = imageDirectory.resolve("image." + IMAGE_ID);
        checksumFile = imageDirectory.resolve("checksum." + IMAGE_ID);
        state = new GlobalStateMgr(new NodeMgr());
        globalState = mockStatic(GlobalStateMgr.class);
        globalState.when(GlobalStateMgr::getCurrentState).thenAnswer(invocation -> state);
        globalState.when(GlobalStateMgr::getServingState).thenAnswer(invocation -> state);
        globalState.when(GlobalStateMgr::getImageDirPath).thenReturn(imageDirectory.getParent().toString());
    }

    @AfterEach
    void tearDown() {
        globalState.close();
        Config.meta_dir = previousMetaDir;
        Config.plugin_dir = previousPluginDir;
        Config.enable_mv_post_image_reload_cache = previousAsyncReload;
    }

    @ParameterizedTest
    @EnumSource(value = PrimitiveType.class, names = {"GEOGRAPHY", "GEOMETRY"})
    void testFullImagePreservesConcreteDescriptor(PrimitiveType kind) throws Exception {
        ScalarType original = writeImage(kind);
        assertChecksumMatches();
        loadIntoFreshState();
        assertEquals(original.getGeoDescriptor(), loadedType().getGeoDescriptor());
        assertEquals(kind, loadedType().getPrimitiveType());
        assertEquals(IMAGE_ID, state.getImageJournalId());
        assertEquals(IMAGE_ID, state.getReplayedJournalId());
    }

    @ParameterizedTest
    @EnumSource(value = PrimitiveType.class, names = {"GEOGRAPHY", "GEOMETRY"})
    void testFullImageLegacyMissingRemainsUnresolvedAfterCheckpoint(PrimitiveType kind) throws Exception {
        ScalarType original = writeImage(kind);
        replaceType(original, GsonUtils.GSON.toJson(new ScalarType(kind), Type.class));
        assertChecksumMatches();
        loadIntoFreshState();
        assertEquals(kind, loadedType().getPrimitiveType());
        assertSame(ScalarType.class, loadedType().getClass());
        assertNull(loadedType().getGeoDescriptor());

        state.saveImage(new ImageWriter(imageDirectory.toString(), IMAGE_ID), imageFile.toFile());
        assertChecksumMatches();
        assertFalse(imageJson().contains("\"geo\":"));
        loadIntoFreshState();
        assertEquals(kind, loadedType().getPrimitiveType());
        assertNull(loadedType().getGeoDescriptor());
    }

    @Test
    void testTruncatedImageRejectsMissingFirstBlock() throws Exception {
        writeImage(PrimitiveType.GEOMETRY);
        assertChecksumMatches();
        byte[] completeImage = Files.readAllBytes(imageFile);
        // With no JSON body, Gson returns a null header. The existing loader rejects this with NPE,
        // before reaching checksum validation; this is distinct from a malformed GEO descriptor.
        Files.write(imageFile, Arrays.copyOf(completeImage, headerLength(completeImage)));
        NullPointerException error = assertThrows(NullPointerException.class, this::loadIntoFreshState);
        assertEquals(GlobalStateMgr.class.getName(), error.getStackTrace()[0].getClassName());
        assertEquals("loadImage", error.getStackTrace()[0].getMethodName());
        assertNull(state.getLocalMetastore().getDb("geo_image"));
    }

    @Test
    void testCompleteImageRejectsIncorrectChecksum() throws Exception {
        writeImage(PrimitiveType.GEOMETRY);
        assertChecksumMatches();
        long checksum = Long.parseLong(Files.readString(checksumFile));
        Files.writeString(checksumFile, Long.toString(checksum ^ 1L));
        IOException error = assertThrows(IOException.class, this::loadIntoFreshState);
        assertTrue(error.getMessage().startsWith("checksum mismatch!"), error.getMessage());
    }

    private ScalarType writeImage(PrimitiveType kind) throws Exception {
        GeoTypeDescriptor descriptor = kind == PrimitiveType.GEOMETRY ? GeoTypeDescriptor.geometry("EPSG:3857")
                : new GeoTypeDescriptor(GeoTypeDescriptor.LogicalType.GEOGRAPHY,
                        GeoTypeDescriptor.EdgeAlgorithm.SPHERICAL,
                        "OGC:CRS84", 4326);
        ScalarType type = new GeoType(kind, descriptor);
        Database db = new Database(10001, "geo_image");
        View view = new View(10002, "geo_view", List.of(new Column("g", type, true)));
        String sql = kind == PrimitiveType.GEOMETRY
                ? "SELECT ST_GeomFromText('POINT (1 2)', 'EPSG:3857') AS g"
                : "SELECT ST_GeogFromText('POINT (1 2)') AS g";
        view.setInlineViewDefWithSqlMode(sql, 0);
        view.setOriginalViewDef(sql);
        view.setComment("geo image view");
        view.setSecurity(true);
        db.registerTableUnlocked(view);
        state.getLocalMetastore().unprotectCreateDb(db);
        ImageWriter writer = new ImageWriter(imageDirectory.toString(), IMAGE_ID);
        state.saveImage(writer, imageFile.toFile());
        return type;
    }

    private void loadIntoFreshState() throws IOException {
        state = new GlobalStateMgr(new NodeMgr());
        state.loadImage();
    }

    private ScalarType loadedType() {
        Database db = state.getLocalMetastore().getDb("geo_image");
        assertNotNull(db);
        View view = (View) db.getTable("geo_view");
        assertNotNull(view);
        assertEquals("geo image view", view.getComment());
        assertTrue(view.isSecurity());
        assertTrue(view.getInlineViewDef().contains("POINT (1 2)"));
        return (ScalarType) view.getColumn("g").getType();
    }

    private void replaceType(ScalarType original, String replacement) throws IOException {
        byte[] bytes = Files.readAllBytes(imageFile);
        int headerLength = headerLength(bytes);
        String json = new String(bytes, headerLength, bytes.length - headerLength, StandardCharsets.UTF_8);
        String field = GsonUtils.GSON.toJson(original, Type.class);
        assertTrue(json.contains(field), json);
        assertEquals(json.indexOf(field), json.lastIndexOf(field));
        ByteArrayOutputStream changed = new ByteArrayOutputStream();
        changed.write(bytes, 0, headerLength);
        changed.write(json.replace(field, replacement).getBytes(StandardCharsets.UTF_8));
        byte[] newBytes = changed.toByteArray();
        Files.write(imageFile, newBytes);
        CRC32 crc = new CRC32();
        crc.update(newBytes);
        Files.writeString(checksumFile, Long.toString(crc.getValue()));
    }

    private String imageJson() throws IOException {
        byte[] bytes = Files.readAllBytes(imageFile);
        int headerLength = headerLength(bytes);
        return new String(bytes, headerLength, bytes.length - headerLength, StandardCharsets.UTF_8);
    }

    private static int headerLength(byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream data = new DataInputStream(in);
        data.readInt();
        Text.readString(data);
        return bytes.length - in.available();
    }

    private void assertChecksumMatches() throws IOException {
        CRC32 crc = new CRC32();
        crc.update(Files.readAllBytes(imageFile));
        assertEquals(Long.toString(crc.getValue()), Files.readString(checksumFile));
    }
}
