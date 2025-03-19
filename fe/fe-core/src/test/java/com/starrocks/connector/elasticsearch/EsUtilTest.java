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

package com.starrocks.connector.elasticsearch;

import com.starrocks.common.AnalysisException;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static com.starrocks.connector.elasticsearch.EsUtil.getFromJSONArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class EsUtilTest {

    private String jsonStr = "{\"settings\": {\n"
            + "               \"index\": {\n"
            + "                  \"bpack\": {\n"
            + "                     \"partition\": {\n"
            + "                        \"upperbound\": \"12\"\n"
            + "                     }\n"
            + "                  },\n"
            + "                  \"number_of_shards\": \"5\",\n"
            + "                  \"provided_name\": \"indexa\",\n"
            + "                  \"creation_date\": \"1539328532060\",\n"
            + "                  \"number_of_replicas\": \"1\",\n"
            + "                  \"uuid\": \"plNNtKiiQ9-n6NpNskFzhQ\",\n"
            + "                  \"version\": {\n"
            + "                     \"created\": \"5050099\"\n"
            + "                  }\n"
            + "               }\n"
            + "            }}";

    String jsonArray = "[{\n" +
            "\t\"index\": \".kibana_1\"\n" +
            "}, {\n" +
            "\t\"index\": \".opendistro_security\"\n" +
            "}, {\n" +
            "\t\"index\": \"kibana_sample_data_ecommerce\"\n" +
            "}, {\n" +
            "\t\"index\": \"kibana_sample_data_ecommerce_2\"\n" +
            "}, {\n" +
            "\t\"index\": \"kibana_sample_data_flights\"\n" +
            "}, {\n" +
            "\t\"index\": \"kibana_sample_data_logs\"\n" +
            "}, {\n" +
            "\t\"index\": \"media_account\"\n" +
            "}, {\n" +
            "\t\"index\": \"toutiao_ad_info\"\n" +
            "}, {\n" +
            "\t\"index\": \"toutiao_ad_info_ext\"\n" +
            "}, {\n" +
            "\t\"index\": \"toutiao_campaign_info\"\n" +
            "}, {\n" +
            "\t\"index\": \"toutiao_strategy_rel\"\n" +
            "}]";

    @Test
    public void testGetJsonObject() {
        JSONObject json = new JSONObject(jsonStr);
        JSONObject upperBoundSetting = EsUtil.getJsonObject(json, "settings.index.bpack.partition", 0);
        assertTrue(upperBoundSetting.has("upperbound"));
        assertEquals("12", upperBoundSetting.get("upperbound"));

        JSONObject unExistKey = EsUtil.getJsonObject(json, "set", 0);
        assertNull(unExistKey);

        JSONObject singleKey = EsUtil.getJsonObject(json, "settings", 0);
        assertTrue(singleKey.has("index"));
    }

    @Test(expected = ClassCastException.class)
    public void testGetJsonObjectWithException() {
        JSONObject json = new JSONObject(jsonStr);
        // only support json object could not get string value directly from this api, exception will be threw
        EsUtil.getJsonObject(json, "settings.index.bpack.partition.upperbound", 0);
    }

    @Test
    public void testGetJsonArray() {
        EsRestClient.EsIndex[] esIndices = getFromJSONArray(jsonArray, EsRestClient.EsIndex[].class);
        System.out.println(JSONObject.valueToString(esIndices));
    }

    @Test
    public void testParseProperties_8x() throws AnalysisException {
        String mappings = "{\n" +
                "  \"idx\": {\n" +
                "    \"mappings\": {\n" +
                "      \"dynamic\": \"strict\",\n" +
                "      \"properties\": {\n" +
                "        \"id\": { \"type\": \"long\" },\n" +
                "        \"name\": { \"type\": \"keyword\" },\n" +
                "        \"description\": { \"type\": \"text\" },\n" +
                "        \"price\": { \"type\": \"double\" },\n" +
                "        \"created\": { \"type\": \"date\" },\n" +
                "        \"tags\": { \"type\": \"keyword\" },\n" +
                "        \"location\": { \"type\": \"geo_point\" }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        JSONObject props = EsUtil.parseProperties("idx", mappings);

        assertNotNull(props);

        assertEquals("long", props.getJSONObject("id").getString("type"));
        assertEquals("keyword", props.getJSONObject("name").getString("type"));
        assertEquals("text", props.getJSONObject("description").getString("type"));
        assertEquals("double", props.getJSONObject("price").getString("type"));
        assertEquals("date", props.getJSONObject("created").getString("type"));
        assertEquals("keyword", props.getJSONObject("tags").getString("type"));
        assertEquals("geo_point", props.getJSONObject("location").getString("type"));
    }

    @Test
    public void testParseProperties_7x() throws AnalysisException {
        String mappings = "{\n" +
                "  \"idx\": {\n" +
                "    \"mappings\": {\n" +
                "      \"properties\": {\n" +
                "        \"id\": { \"type\": \"long\" },\n" +
                "        \"name\": { \"type\": \"keyword\" },\n" +
                "        \"description\": { \"type\": \"text\" },\n" +
                "        \"nested_field\": {\n" +
                "          \"type\": \"nested\",\n" +
                "          \"properties\": {\n" +
                "            \"inner_field\": { \"type\": \"keyword\" }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        JSONObject props = EsUtil.parseProperties("idx", mappings);

        assertEquals("long", props.getJSONObject("id").getString("type"));
        assertEquals("keyword", props.getJSONObject("name").getString("type"));
        assertEquals("text", props.getJSONObject("description").getString("type"));

        JSONObject nested = props.getJSONObject("nested_field");
        assertEquals("nested", nested.getString("type"));
        assertEquals("keyword", nested.getJSONObject("properties").getJSONObject("inner_field").getString("type"));
    }

    @Test
    public void testParseProperties_6x() throws AnalysisException {
        String mappings = "{\n" +
                "  \"idx\": {\n" +
                "    \"mappings\": {\n" +
                "      \"_doc\": {\n" +
                "        \"properties\": {\n" +
                "          \"id\": { \"type\": \"long\" },\n" +
                "          \"name\": { \"type\": \"keyword\" },\n" +
                "          \"description\": { \"type\": \"text\" },\n" +
                "          \"object_field\": {\n" +
                "            \"type\": \"object\",\n" +
                "            \"properties\": {\n" +
                "              \"sub_field\": { \"type\": \"integer\" }\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        JSONObject props = EsUtil.parseProperties("idx", mappings);

        assertEquals("long", props.getJSONObject("id").getString("type"));
        assertEquals("keyword", props.getJSONObject("name").getString("type"));
        assertEquals("text", props.getJSONObject("description").getString("type"));

        JSONObject objField = props.getJSONObject("object_field");
        assertEquals("object", objField.getString("type"));
        assertEquals("integer", objField.getJSONObject("properties").getJSONObject("sub_field").getString("type"));
    }

    @Test(expected = JSONException.class)
    public void testParseProperties_InvalidMapping() throws AnalysisException {
        String mappings = "{\n" +
                "  \"idx\": {\n" +
                "    \"invalid_field\": {\n" + // Invalid mapping structure
                "      \"properties\": {\n" +
                "        \"id\": { \"type\": \"long\" }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        EsUtil.parseProperties("idx", mappings);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseProperties_NoProperties() throws AnalysisException {
        String mappings = "{\n" +
                "  \"idx\": {\n" +
                "    \"mappings\": {\n" +
                "      \"_doc\": {\n" +
                "        \"type\": \"long\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        EsUtil.parseProperties("idx", mappings);
    }
    
}
