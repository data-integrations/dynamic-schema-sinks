/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.dynamicschema;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.dynamicschema.api.Getable;
import co.cask.dynamicschema.api.GetableException;
import co.cask.dynamicschema.api.StructuredRecordVisitor;

import java.util.Map;

/**
 *
 */
public class TablePutGenerator implements StructuredRecordVisitor, Getable<Put> {
  private final Put put;

  public TablePutGenerator(byte[] rowkey) {
    put = new Put(rowkey);
  }

  public boolean visit(int depth, String name, Schema.Field field, StructuredRecord value) {
    if (depth > 0) {
      // It's a sub-record.
      int size = value.getSchema().getFields().size();
      if (size  == 2) {
        String fld = value.get("field");
        String val = value.get("value");
        put.add(fld, val);
      } else {
        String fld = value.get("field");
        String val = value.get("value");
        String type = value.get("type");
        put.add(fld, val);
      }
    }
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, String value) {
    put.add(name, Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Integer value) {
    put.add(name, Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Float value) {
    put.add(name, Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Double value) {
    put.add(name, Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Boolean value) {
    put.add(name, Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Long value) {
    put.add(name, Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Map<String, String> value) {
    for (Map.Entry<String, String> entry : value.entrySet()) {
      put.add(entry.getKey(), Bytes.toBytes(entry.getValue()));
    }
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, byte[] value) {
    put.add(name, value);
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field) {
    put.add(name, (byte[]) null);
    return true;
  }

  public Put get() throws GetableException {
    return put;
  }
}
