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

package com.starrocks.hive.reader;

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnValue;
import com.starrocks.jni.connector.ConnectorScanner;
import com.starrocks.jni.connector.ScannerHelper;
import com.starrocks.jni.connector.SelectedFields;
import com.starrocks.utils.loader.ThreadContextClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class HiveScanner extends ConnectorScanner {

    private static final Logger LOG = LogManager.getLogger(HiveScanner.class);

    private static final String SERDE_PROPERTY_PREFIX = "SerDe.";

    private final String hiveColumnNames;
    private final String[] hiveColumnTypes;
    private Map<String, String> serdeProperties = new HashMap<>();
    private final String[] requiredFields;
    private int[] requiredColumnIds;
    private ColumnType[] requiredTypes;

    private final String[] nestedFields;

    private final String dataFilePath;

    private final long blockOffset;

    private final long blockLength;

    private final String serde;
    private final String inputFormat;

    private RecordReader<Writable, Writable> reader;
    private StructObjectInspector rowInspector;
    private ObjectInspector[] fieldInspectors;
    private StructField[] structFields;
    private Deserializer deserializer;
    private final int fetchSize;
    private final ClassLoader classLoader;
    private final String fsOptionsProps;

    // The key buffer used to store the key part(meta data) of the file.
    private Writable key;
    // The value buffer used to store the value data.
    private Writable value;

    public HiveScanner(int fetchSize, Map<String, String> params) {
        this.fetchSize = fetchSize;
        this.hiveColumnNames = params.get("hive_column_names");
        this.hiveColumnTypes = params.get("hive_column_types").split("#");
        this.requiredFields = params.get("required_fields").split(",");
        this.nestedFields = params.getOrDefault("nested_fields", "").split(",");
        this.dataFilePath = params.get("data_file_path");
        this.blockOffset = Long.parseLong(params.get("block_offset"));
        this.blockLength = Long.parseLong(params.get("block_length"));
        this.serde = params.get("serde");
        this.inputFormat = params.get("input_format");
        this.fieldInspectors = new ObjectInspector[requiredFields.length];
        this.structFields = new StructField[requiredFields.length];
        this.classLoader = this.getClass().getClassLoader();
        this.fsOptionsProps = params.get("fs_options_props");
        for (Map.Entry<String, String> kv : params.entrySet()) {
            if (kv.getKey().startsWith(SERDE_PROPERTY_PREFIX)) {
                this.serdeProperties.put(kv.getKey().substring(SERDE_PROPERTY_PREFIX.length()), kv.getValue());
            }
            LOG.debug("key = " + kv.getKey() + ", value = " + kv.getValue());
        }
    }

    private JobConf makeJobConf(Properties properties) {
        Configuration conf = new Configuration();
        JobConf jobConf = new JobConf(conf);
        jobConf.setBoolean("hive.io.file.read.all.columns", false);
        properties.stringPropertyNames().forEach(name -> jobConf.set(name, properties.getProperty(name)));
        return jobConf;
    }

    private void parseRequiredTypes() {
        String[] hiveColumnNames = this.hiveColumnNames.split(",");
        HashMap<String, Integer> hiveColumnNameToIndex = new HashMap<>();
        HashMap<String, String> hiveColumnNameToType = new HashMap<>();
        for (int i = 0; i < hiveColumnNames.length; i++) {
            hiveColumnNameToIndex.put(hiveColumnNames[i], i);
            hiveColumnNameToType.put(hiveColumnNames[i], hiveColumnTypes[i]);
        }

        requiredTypes = new ColumnType[requiredFields.length];
        requiredColumnIds = new int[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++) {
            requiredColumnIds[i] = hiveColumnNameToIndex.get(requiredFields[i]);
            String type = hiveColumnNameToType.get(requiredFields[i]);
            requiredTypes[i] = new ColumnType(requiredFields[i], type);
        }

        // prune fields
        SelectedFields ssf = new SelectedFields();
        for (String nestField : nestedFields) {
            ssf.addNestedPath(nestField);
        }
        for (int i = 0; i < requiredFields.length; i++) {
            ColumnType type = requiredTypes[i];
            String name = requiredFields[i];
            type.pruneOnField(ssf, name);
        }
    }

    private Properties makeProperties() {
        Properties properties = new Properties();
        properties.setProperty("hive.io.file.readcolumn.ids",
                Arrays.stream(this.requiredColumnIds).mapToObj(String::valueOf)
                        .collect(Collectors.joining(",")));
        properties.setProperty("hive.io.file.readcolumn.names", String.join(",", this.requiredFields));
        // build `readNestedColumn.paths` spec.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < requiredFields.length; i++) {
            String name = requiredFields[i];
            ColumnType type = requiredTypes[i];
            type.buildNestedFieldsSpec(name, sb);
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
            properties.setProperty("hive.io.file.readNestedColumn.paths", sb.toString());
        }
        properties.setProperty("columns", this.hiveColumnNames);
        List<String> types = new ArrayList<>();
        for (int i = 0; i < this.hiveColumnTypes.length; i++) {
            String type = this.hiveColumnTypes[i];
            types.add(type);
        }
        properties.setProperty("columns.types", types.stream().collect(Collectors.joining(",")));
        properties.setProperty("serialization.lib", this.serde);
        properties.putAll(serdeProperties);

        ScannerHelper.parseFSOptionsProps(fsOptionsProps, kv -> {
            properties.put(kv[0], kv[1]);
            return null;
        }, t -> {
            LOG.warn("Invalid hive scanner fs options props argument: " + t);
            return null;
        });
        return properties;
    }

    private void initReader(JobConf jobConf, Properties properties) throws Exception {
        Path path = new Path(dataFilePath);
        FileSplit fileSplit = new FileSplit(path, blockOffset, blockLength, (String[]) null);

        InputFormat<?, ?> inputFormatClass = createInputFormat(jobConf, inputFormat);
        reader = (RecordReader<Writable, Writable>) inputFormatClass.getRecordReader(fileSplit, jobConf, Reporter.NULL);

        deserializer = getDeserializer(jobConf, properties, serde);
        rowInspector = getTableObjectInspector(deserializer);
        for (int i = 0; i < requiredFields.length; i++) {
            StructField field = rowInspector.getStructFieldRef(requiredFields[i]);
            structFields[i] = field;
            fieldInspectors[i] = field.getFieldObjectInspector();
        }
        key = (Writable) reader.createKey();
        value = (Writable) reader.createValue();
    }

    @Override
    public void open() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            parseRequiredTypes();
            initOffHeapTableWriter(requiredTypes, requiredFields, fetchSize);
            Properties properties = makeProperties();
            JobConf jobConf = makeJobConf(properties);
            initReader(jobConf, properties);
        } catch (Exception e) {
            close();
            LOG.error("Failed to open the hive reader.", e);
            throw new IOException("Failed to open the hive reader.", e);
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            LOG.error("Failed to close the hive reader.", e);
            throw new IOException("Failed to close the hive reader.", e);
        }
    }

    @Override
    public int getNext() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            int numRows = 0;
            for (; numRows < getTableSize(); numRows++) {
                if (!reader.next(key, value)) {
                    break;
                }
                Object rowData = deserializer.deserialize(value);
                for (int i = 0; i < requiredFields.length; i++) {
                    Object fieldData = rowInspector.getStructFieldData(rowData, structFields[i]);
                    if (fieldData == null) {
                        appendData(i, null);
                    } else {
                        ColumnValue fieldValue = new HiveColumnValue(fieldInspectors[i], fieldData);
                        appendData(i, fieldValue);
                    }
                }
            }
            return numRows;
        } catch (Exception e) {
            close();
            LOG.error("Failed to get the next off-heap table chunk of hive.", e);
            throw new IOException("Failed to get the next off-heap table chunk of hive.", e);
        }
    }

    private InputFormat<?, ?> createInputFormat(Configuration conf, String inputFormat) throws Exception {
        Class<?> clazz = conf.getClassByName(inputFormat);
        Class<? extends InputFormat<?, ?>> cls =
                (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
        return ReflectionUtils.newInstance(cls, conf);
    }

    private Deserializer getDeserializer(Configuration configuration, Properties properties, String name)
            throws Exception {
        Class<? extends Deserializer> deserializerClass = Class.forName(name, true, JavaUtils.getClassLoader())
                .asSubclass(Deserializer.class);
        Deserializer deserializer = deserializerClass.getConstructor().newInstance();
        deserializer.initialize(configuration, properties);
        return deserializer;
    }

    private StructObjectInspector getTableObjectInspector(Deserializer deserializer) throws Exception {
        ObjectInspector inspector = deserializer.getObjectInspector();
        checkArgument(inspector.getCategory() == ObjectInspector.Category.STRUCT,
                "expected STRUCT: %s", inspector.getCategory());
        return (StructObjectInspector) inspector;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("hiveColumnNames: ");
        sb.append(hiveColumnNames);
        sb.append("\n");
        sb.append("hiveColumnTypes: ");
        sb.append(Arrays.toString(hiveColumnTypes));
        sb.append("\n");
        sb.append("requiredFields: ");
        sb.append(Arrays.toString(requiredFields));
        sb.append("\n");
        sb.append("nestedFields: ");
        sb.append(Arrays.toString(nestedFields));
        sb.append("\n");
        sb.append("dataFilePath: ");
        sb.append(dataFilePath);
        sb.append("\n");
        sb.append("block_offset: ");
        sb.append(blockOffset);
        sb.append("block_length: ");
        sb.append(blockLength);
        sb.append("\n");
        sb.append("serde: ");
        sb.append(serde);
        sb.append("\n");
        sb.append("serdeProperties: ");
        sb.append(serdeProperties.toString());
        sb.append("\n");
        sb.append("inputFormat: ");
        sb.append(inputFormat);
        sb.append("\n");
        return sb.toString();
    }
}
