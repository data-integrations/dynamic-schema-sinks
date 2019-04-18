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

package io.cdap.dynamicschema;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.dynamicschema.api.Getable;
import io.cdap.dynamicschema.api.GetableException;
import io.cdap.dynamicschema.api.StructuredRecordVisitor;
import io.cdap.dynamicschema.api.VisitorException;

import java.util.Map;

/**
 * Generate CDAP Table puts.
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

  public boolean visit(int depth, String name, Schema.Field field, String value) throws VisitorException {
    put.add(name, Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Integer value) throws VisitorException {
    put.add(name, Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Float value) throws VisitorException {
    put.add(name, Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Double value) throws VisitorException {
    put.add(name, Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Boolean value) throws VisitorException {
    put.add(name, Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Long value) throws VisitorException {
    put.add(name, Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Map<String, String> value) throws VisitorException {
    for (Map.Entry<String, String> entry : value.entrySet()) {
      put.add(entry.getKey(), Bytes.toBytes(entry.getValue()));
    }
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, byte[] value) throws VisitorException {
    put.add(name, value);
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field) throws VisitorException {
    put.add(name, (byte[]) null);
    return true;
  }

  public Put get() throws GetableException {
    return put;
  }
}
