package co.cask.dynamicschema;

import co.cask.dynamicschema.api.FieldVisitor;
import co.cask.dynamicschema.api.Getable;
import co.cask.dynamicschema.api.GetableException;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

public class DynamicSchemaTablePut implements FieldVisitor, Getable<Put> {
  private byte[] row;
  private byte[] family;
  private final Put put;

  public DynamicSchemaTablePut(byte[] row, byte[] family) {
    this.row = row;
    this.family = family;
    this.put = new Put(this.row);
  }

  public boolean visit(int depth, String name, Schema.Field field, StructuredRecord value) {
    if (depth > 0) {
      // It's a sub-record.
      for (Schema.Field f : value.getSchema().getFields()) {
        System.out.println(name + " : " + f.getName());
      }
    }
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, String value) {
    put.add(family, Bytes.toBytes(name), Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Integer value) {
    put.add(family, Bytes.toBytes(name), Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Float value) {
    put.add(family, Bytes.toBytes(name), Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Double value) {
    put.add(family, Bytes.toBytes(name), Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Boolean value) {
    put.add(family, Bytes.toBytes(name), Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Long value) {
    put.add(family, Bytes.toBytes(name), Bytes.toBytes(value));
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, Map<String, String> value) {
    for (Map.Entry<String, String> entry : value.entrySet()) {
      put.add(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
    }
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field, byte[] value) {
    put.add(family, Bytes.toBytes(name), value);
    return true;
  }

  public boolean visit(int depth, String name, Schema.Field field) {
    put.add(family, Bytes.toBytes(name), (byte[]) null);
    return true;
  }

  public Put get() throws GetableException {
    return put;
  }
}
