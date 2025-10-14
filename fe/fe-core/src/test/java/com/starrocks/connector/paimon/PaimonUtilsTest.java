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

package com.starrocks.connector.paimon;

import com.starrocks.thrift.TIcebergSchema;
import com.starrocks.thrift.TIcebergSchemaField;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.junit.Assert;
import org.junit.Test;


public class PaimonUtilsTest {

    @Test
    public void testGetTPaimonSchemaWithSimpleTypes() {
        // Create a simple schema with basic types
        Schema.Builder builder = new Schema.Builder();
        builder.column("id", new IntType());
        builder.column("name", new VarCharType(255));
        builder.column("score", new DoubleType());
        Schema paimonSchema = builder.build();
        
        RowType rowType = paimonSchema.rowType();
        TIcebergSchema schema = PaimonUtils.getTPaimonSchema(rowType);
        
        Assert.assertNotNull(schema);
        Assert.assertEquals(3, schema.getFields().size());
        
        // Verify field details
        TIcebergSchemaField idField = schema.getFields().get(0);
        Assert.assertEquals(0, idField.getField_id());
        Assert.assertEquals("id", idField.getName());
        
        TIcebergSchemaField nameField = schema.getFields().get(1);
        Assert.assertEquals(1, nameField.getField_id());
        Assert.assertEquals("name", nameField.getName());
        
        TIcebergSchemaField scoreField = schema.getFields().get(2);
        Assert.assertEquals(2, scoreField.getField_id());
        Assert.assertEquals("score", scoreField.getName());
    }
    
    @Test
    public void testGetTPaimonSchemaWithMapType() {
        // Create a schema with a map type
        Schema.Builder builder = new Schema.Builder();
        builder.column("id", new IntType());
        builder.column("tags", new MapType(new VarCharType(50), new IntType()));
        Schema paimonSchema = builder.build();

        RowType rowType = paimonSchema.rowType();
        TIcebergSchema schema = PaimonUtils.getTPaimonSchema(rowType);
        
        Assert.assertNotNull(schema);
        Assert.assertEquals(2, schema.getFields().size());

        TIcebergSchemaField idField = schema.getFields().get(0);
        Assert.assertEquals(0, idField.getField_id());
        Assert.assertEquals("id", idField.getName());
        
        // Verify map field and its children
        TIcebergSchemaField mapField = schema.getFields().get(1);
        Assert.assertEquals(1, mapField.getField_id());
        Assert.assertEquals("tags", mapField.getName());
        Assert.assertNotNull(mapField.getChildren());
        Assert.assertEquals(2, mapField.getChildren().size());
        
        // Verify map key and value fields
        TIcebergSchemaField keyField = mapField.getChildren().get(0);
        Assert.assertEquals("key", keyField.getName());
        Assert.assertEquals(536869886, keyField.getField_id());
        
        TIcebergSchemaField valueField = mapField.getChildren().get(1);
        Assert.assertEquals("value", valueField.getName());
        Assert.assertEquals(536871936, valueField.getField_id());
    }
    
    @Test
    public void testGetTPaimonSchemaWithArrayType() {
        // Create a schema with an array type
        Schema.Builder builder = new Schema.Builder();
        builder.column("id", new IntType());
        builder.column("properties", new ArrayType(new VarCharType(100)));
        Schema paimonSchema = builder.build();

        RowType rowType = paimonSchema.rowType();
        TIcebergSchema schema = PaimonUtils.getTPaimonSchema(rowType);
        
        Assert.assertNotNull(schema);
        Assert.assertEquals(2, schema.getFields().size());

        TIcebergSchemaField idField = schema.getFields().get(0);
        Assert.assertEquals(0, idField.getField_id());
        Assert.assertEquals("id", idField.getName());
        
        // Verify array field and its children
        TIcebergSchemaField arrayField = schema.getFields().get(1);
        Assert.assertEquals(1, arrayField.getField_id());
        Assert.assertEquals("properties", arrayField.getName());
        Assert.assertNotNull(arrayField.getChildren());
        Assert.assertEquals(1, arrayField.getChildren().size());
        
        // Verify array element field
        TIcebergSchemaField elementField = arrayField.getChildren().get(0);
        Assert.assertEquals("element", elementField.getName());
        Assert.assertEquals(536871936, elementField.getField_id());
    }
    
    @Test
    public void testGetTPaimonSchemaWithNestedStruct1() {
        // Create a schema with a nested struct
        Schema.Builder builder = new Schema.Builder();
        builder.column("id", new IntType());

        Schema.Builder innerBuilder = new Schema.Builder();
        innerBuilder.column("name", new VarCharType(255));
        innerBuilder.column("age", new IntType());

        builder.column("ex", new ArrayType(new MapType(new IntType(),
                new ArrayType(new RowType(innerBuilder.build().fields())))));
        Schema paimonSchema = builder.build();

        RowType rowType = paimonSchema.rowType();
        TIcebergSchema schema = PaimonUtils.getTPaimonSchema(rowType);
        
        Assert.assertNotNull(schema);
        Assert.assertEquals(2, schema.getFields().size());

        // Verify field details
        TIcebergSchemaField idField = schema.getFields().get(0);
        Assert.assertEquals(0, idField.getField_id());
        Assert.assertEquals("id", idField.getName());
        
        // Verify struct field and its children
        TIcebergSchemaField outerArrayField = schema.getFields().get(1);
        Assert.assertEquals(1, outerArrayField.getField_id());
        Assert.assertEquals("ex", outerArrayField.getName());
        Assert.assertNotNull(outerArrayField.getChildren());
        Assert.assertEquals(1, outerArrayField.getChildren().size());
        
        // Verify nested fields
        TIcebergSchemaField mapField = outerArrayField.getChildren().get(0);
        Assert.assertEquals(536871936, mapField.getField_id());
        Assert.assertEquals("element", mapField.getName());
        Assert.assertNotNull(mapField.getChildren());
        Assert.assertEquals(2, mapField.getChildren().size());

        // Verify map key and value fields
        TIcebergSchemaField mapKeyField = mapField.getChildren().get(0);
        Assert.assertEquals(536869885, mapKeyField.getField_id());
        Assert.assertEquals("key", mapKeyField.getName());

        TIcebergSchemaField mapValueField = mapField.getChildren().get(1);
        Assert.assertEquals(536871937, mapValueField.getField_id());
        Assert.assertEquals("value", mapValueField.getName());
        Assert.assertNotNull(mapValueField.getChildren());
        Assert.assertEquals(1, mapValueField.getChildren().size());

        TIcebergSchemaField rowTypeField = mapValueField.getChildren().get(0);
        Assert.assertEquals(536871938, rowTypeField.getField_id());
        Assert.assertEquals("element", rowTypeField.getName());
        Assert.assertNotNull(rowTypeField.getChildren());
        Assert.assertEquals(2, rowTypeField.getChildren().size());

        TIcebergSchemaField nameField = rowTypeField.getChildren().get(0);
        Assert.assertEquals(2, nameField.getField_id());
        Assert.assertEquals("name", nameField.getName());
        TIcebergSchemaField ageField = rowTypeField.getChildren().get(1);
        Assert.assertEquals(3, ageField.getField_id());
        Assert.assertEquals("age", ageField.getName());
    }

    @Test
    public void testGetTPaimonSchemaWithNestedStruct2() {
        // Create a schema with a nested struct
        Schema.Builder builder = new Schema.Builder();
        builder.column("ex", new ArrayType(new MapType(new IntType(), new ArrayType(new IntType()))));
        Schema paimonSchema = builder.build();

        RowType rowType = paimonSchema.rowType();
        TIcebergSchema schema = PaimonUtils.getTPaimonSchema(rowType);

        Assert.assertNotNull(schema);
        Assert.assertEquals(1, schema.getFields().size());

        // Verify struct field and its children
        TIcebergSchemaField outerArrayField = schema.getFields().get(0);
        Assert.assertEquals(0, outerArrayField.getField_id());
        Assert.assertEquals("ex", outerArrayField.getName());
        Assert.assertNotNull(outerArrayField.getChildren());
        Assert.assertEquals(1, outerArrayField.getChildren().size());

        // Verify nested fields
        TIcebergSchemaField mapField = outerArrayField.getChildren().get(0);
        Assert.assertEquals(536870912, mapField.getField_id());
        Assert.assertEquals("element", mapField.getName());
        Assert.assertNotNull(mapField.getChildren());
        Assert.assertEquals(2, mapField.getChildren().size());

        // Verify map key and value fields
        TIcebergSchemaField mapKeyField = mapField.getChildren().get(0);
        Assert.assertEquals(536870909, mapKeyField.getField_id());
        Assert.assertEquals("key", mapKeyField.getName());

        TIcebergSchemaField mapValueField = mapField.getChildren().get(1);
        Assert.assertEquals(536870913, mapValueField.getField_id());
        Assert.assertEquals("value", mapValueField.getName());
        Assert.assertNotNull(mapValueField.getChildren());
        Assert.assertEquals(1, mapValueField.getChildren().size());

        TIcebergSchemaField innerArrayField = mapValueField.getChildren().get(0);
        Assert.assertEquals(536870914, innerArrayField.getField_id());
        Assert.assertEquals("element", innerArrayField.getName());
    }
}