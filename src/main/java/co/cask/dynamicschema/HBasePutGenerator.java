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

import co.cask.dynamicschema.api.StructuredRecordVisitor;
import co.cask.dynamicschema.api.Getable;
import co.cask.dynamicschema.api.GetableException;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.dynamicschema.api.VisitorException;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 * HBase PUT Generator visiting all parts of {@link StructuredRecord} record.
 */
public class HBasePutGenerator implements StructuredRecordVisitor, Getable<Put> {
  /**
   * Defines the row key for HBase row.
   */
  private byte[] rowkey;

  /**
   * Column family where the data needs to be written.
   */
  private byte[] family;

  /**
   *
   */
  private final Put put;

  public HBasePutGenerator(byte[] rowkey, byte[] family, Durability durability) {
    this.rowkey = rowkey;
    this.family = family;
    this.put = new Put(this.rowkey);
    this.put.setDurability(durability);
  }

  public boolean visit(int depth, String name, Schema.Field field, StructuredRecord value) throws VisitorException {
    if (depth > 0) {
      // It's a sub-record.
      int size = value.getSchema().getFields().size();
      if (size  == 2) {
        String fld = value.get("field");
        String val = value.get("value");
        put.addColumn(family, Bytes.toBytes(fld), Bytes.toBytes(val));
      } else {
        String fld = value.get("field");
        String val = value.get("value");
        String type = value.get("type");
        put.addColumn(family, Bytes.toBytes(fld), Bytes.toBytes(val));
      }
    }
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, String value) throws VisitorException {
    put.addColumn(family, Bytes.toBytes(name), Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Integer value) throws VisitorException {
    put.add(family, Bytes.toBytes(name), Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Float value) throws VisitorException {
    put.addColumn(family, Bytes.toBytes(name), Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Double value) throws VisitorException {
    put.addColumn(family, Bytes.toBytes(name), Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Boolean value) throws VisitorException {
    put.addColumn(family, Bytes.toBytes(name), Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Long value) throws VisitorException {
    put.addColumn(family, Bytes.toBytes(name), Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Map<String, String> value) throws VisitorException {
    for (Map.Entry<String, String> entry : value.entrySet()) {
      put.addColumn(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
    }
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, byte[] value) throws VisitorException {
    put.addColumn(family, Bytes.toBytes(name), value);
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field) throws VisitorException {
    put.addColumn(family, Bytes.toBytes(name), (byte[]) null);
    return true;
  }

  public Put get() throws GetableException {
    return put;
  }
}
