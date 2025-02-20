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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.PaimonView;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ColumnTypeConverter;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.view.View;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class PaimonApiConverter {

    @NotNull
    static List<Column> toFullSchemas(List<DataField> fields) {
        List<Column> columns = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            String fieldName = field.name();
            DataType type = field.type();
            Type fieldType = ColumnTypeConverter.fromPaimonType(type);
            Column column = new Column(fieldName, fieldType, true, field.description());
            columns.add(column);
        }
        return columns;
    }

    @NotNull
    static PaimonView getPaimonView(String catalogName, String dbName, String viewName, View paimonNativeView) {
        List<DataField> fields = paimonNativeView.rowType().getFields();
        List<Column> fullSchema = toFullSchemas(fields);
        String comment = "";
        if (paimonNativeView.comment().isPresent()) {
            comment = paimonNativeView.comment().get();
        }
        PaimonView view = new PaimonView(CONNECTOR_ID_GENERATOR.getNextId().asInt(), catalogName, dbName, viewName,
                fullSchema, paimonNativeView.query(), catalogName, dbName, "");
        view.setComment(comment);
        return view;
    }
}
