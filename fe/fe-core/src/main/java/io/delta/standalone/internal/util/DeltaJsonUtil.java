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
package io.delta.standalone.internal.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.internal.actions.SingleAction;
import scala.reflect.ManifestFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DeltaJsonUtil {
    public static ObjectMapper mapper = new ObjectMapper();

    public static Metadata convertJsonToMetadataJ(String metadata) {

        SingleAction frommedJson = JsonUtils.fromJson(metadata,
                ManifestFactory.classType(SingleAction.class));
        return ConversionUtils.convertMetadata((io.delta.standalone.internal.actions.Metadata) frommedJson.unwrap());
    }


    public static String convertMetadataJToJson(Metadata metadataJ) {
        return ConversionUtils.convertMetadataJ(metadataJ).json();
    }


    public static String convertAddFileJToJson(AddFile addFileJ) {
        return JsonUtils.toJson(addFileJ, ManifestFactory.classType(AddFile.class));
    }

    public static AddFile convertJsonToAddFileJ(String addFile) {
        io.delta.standalone.internal.actions.AddFile fromJson = JsonUtils
                .fromJson(addFile,
                        ManifestFactory
                                .classType(io.delta.standalone.internal.actions.AddFile.class));

        return ConversionUtils
                .convertAddFile(fromJson);
    }


    public static <T> String toJson(T object) throws IOException {
        return mapper.writeValueAsString(object);
    }


    public static List<AddFile> readerAddFilesFromJson(String str) throws IOException {

        JsonNode jsonNode = mapper.readTree(str);

        List<AddFile> addFileList = new ArrayList<>(jsonNode.size());
        Iterator<JsonNode> elements = jsonNode.elements();
        String text;
        while (elements.hasNext()) {
            text = elements.next().textValue();
            addFileList.add(convertJsonToAddFileJ(text));
        }
        return addFileList;
    }

}
