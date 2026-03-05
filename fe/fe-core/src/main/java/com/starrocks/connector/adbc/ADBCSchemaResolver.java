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

package com.starrocks.connector.adbc;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.type.Type;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Abstract base class for converting Arrow schemas to StarRocks column definitions.
 * Concrete implementations (e.g., FlightSqlSchemaResolver) provide the Arrow-to-SR
 * type mapping via {@link #convertArrowFieldToSRType(Field)}.
 *
 * Plan 03 will implement the concrete FlightSqlSchemaResolver subclass.
 */
public abstract class ADBCSchemaResolver {

    private static final Logger LOG = LogManager.getLogger(ADBCSchemaResolver.class);

    /**
     * Convert an Arrow Field to a StarRocks Type.
     *
     * @param field the Arrow field to convert
     * @return the corresponding StarRocks Type, or null if the type is not supported
     */
    public abstract Type convertArrowFieldToSRType(Field field);

    /**
     * Convert an Arrow Schema to a list of StarRocks Columns.
     * Fields whose Arrow types cannot be mapped are skipped with a warning.
     *
     * @param arrowSchema the Arrow schema to convert
     * @return list of StarRocks Column definitions
     */
    public List<Column> convertToSRTable(Schema arrowSchema) {
        List<Column> fullSchema = Lists.newArrayList();
        for (Field field : arrowSchema.getFields()) {
            Type type = convertArrowFieldToSRType(field);
            if (type == null) {
                LOG.warn("Skipping unsupported Arrow type for field: {} (type: {})",
                        field.getName(), field.getFieldType().getType());
                continue;
            }
            fullSchema.add(new Column(field.getName(), type, field.isNullable(), ""));
        }
        return fullSchema;
    }
}
