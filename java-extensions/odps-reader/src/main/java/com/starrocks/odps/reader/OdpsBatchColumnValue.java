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

package com.starrocks.odps.reader;

import com.aliyun.odps.Column;
import com.aliyun.odps.table.arrow.accessor.ArrowVectorAccessor;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class OdpsBatchColumnValue {

    private Column[] columns;
    private ArrowVectorAccessor[] columnAccessors;

    public OdpsBatchColumnValue(VectorSchemaRoot root, Column[] columns) {
        this.columns = columns;
        columnAccessors = new ArrowVectorAccessor[columns.length];
        List<FieldVector> fieldVectors = root.getFieldVectors();
        for (int i = 0; i < fieldVectors.size(); i++) {
            columnAccessors[i] =
                    OdpsTypeUtils.createColumnVectorAccessor(fieldVectors.get(i), columns[i].getTypeInfo());
        }
    }

    public List<OdpsColumnValue> getColumnValue(int i, int limit) {
        try {
            List<OdpsColumnValue> values = new ArrayList<>(limit);
            for (int j = 0; j < limit; j++) {
                Object data = OdpsTypeUtils.getData(columnAccessors[i], columns[i].getTypeInfo(), j);
                OdpsColumnValue odpsColumnValue = new OdpsColumnValue(data, columns[i].getTypeInfo());
                values.add(odpsColumnValue);
            }
            return values;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
