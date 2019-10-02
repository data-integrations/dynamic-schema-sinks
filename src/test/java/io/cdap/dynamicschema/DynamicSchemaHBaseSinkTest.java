/*
 * Copyright © 2019 Cask Data, Inc.
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
import io.cdap.cdap.internal.io.ReflectionSchemaGenerator;
import io.cdap.dynamicschema.api.ValidationException;
import io.cdap.dynamicschema.observer.SchemaObserver;
import io.cdap.dynamicschema.observer.StructuredRecordObserver;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests {@link DynamicSchemaHBaseSink}
 */
public class DynamicSchemaHBaseSinkTest {

  public final class GoodDynamicSchema {
    private String field;
    private String value;
  }

  public final class GoodDynamicSchema1 {
    private String field;
    private String value;
    private String type;
  }

  public final class BadDynamicSchema1 {
    private String field;
  }

  public final class BadDynamicSchema2 {
    private String field;
    private String value;
    private String something;
  }

  public final class BadDynamicSchema3 {
    private String field;
    private String value;
    private String type;
    private String bad;
  }

  public final class GoodRecord {
    private int i;
    private double d;
    private float f;
    private String s;
    private Map<String, String> m;
    private List<GoodDynamicSchema> l;
  }

  public final class GoodRecord1 {
    private String s;
    private Map<String, String> m;
    private List<GoodDynamicSchema1> l;
  }

  public final class BadRecord1 {
    private int i;
    private List<BadDynamicSchema1> l;
  }

  public final class BadRecord2 {
    private double d;
    private List<BadDynamicSchema2> l;
  }

  public final class BadRecord3 {
    private double d;
    private List<BadDynamicSchema3> l;
  }

  private StructuredRecord createDynamicField(Schema schema, String field, String value) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    builder.set("value", value);
    builder.set("field", field);
    return builder.build();
  }

  @Test
  public void testBasicDynamicSchema() throws Exception {
    Schema schema = new ReflectionSchemaGenerator().generate(GoodRecord.class, true);
    Schema node = new ReflectionSchemaGenerator().generate(GoodDynamicSchema.class);

    Map<String, String> m = new HashMap<String, String>();
    m.put("mapa", "1"); m.put("mapb", "2"); m.put("mapc", "3");

    List<StructuredRecord> r = new ArrayList<StructuredRecord>();
    r.add(createDynamicField(node, "couponnum", "9999932101881702010326"));
    r.add(createDynamicField(node, "SurveyStartDate", "2017-02-02T21:06:44Z"));
    r.add(createDynamicField(node, "ani", "8166686311"));

    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    builder.set("i", 1);
    builder.set("d", 1.0);
    builder.set("f", 2.0f);
    builder.set("s", "test");
    builder.set("m", m);
    builder.set("l", r);
    StructuredRecord record = builder.build();

    // Schema Validator iterate through individual fields to validate
    // and ensure the types are right.
    DynamicSchemaValidator dsc = new DynamicSchemaValidator();
    SchemaObserver so = new SchemaObserver(dsc);
    so.traverse(schema);
    dsc.validate();

    byte[] key = Bytes.toBytes("A");
    byte[] family = Bytes.toBytes("B");
    HBasePutGenerator sp = new HBasePutGenerator(key, family, Durability.SYNC_WAL);
    StructuredRecordObserver walker = new StructuredRecordObserver(sp);
    walker.traverse(record);
    Put p = sp.get();
    Assert.assertNotNull(p);

    TablePutGenerator tablePutGenerator = new TablePutGenerator(key);
    StructuredRecordObserver observer = new StructuredRecordObserver(tablePutGenerator);
    observer.traverse(record);
    io.cdap.cdap.api.dataset.table.Put put = tablePutGenerator.get();
    Assert.assertNotNull(put);
  }

  @Test
  public void testGoodDynamicSchemas() throws Exception {
    // Test All good Schema types for dynamic schema.
    DynamicSchemaValidator dcv = new DynamicSchemaValidator();
    SchemaObserver so = new SchemaObserver(dcv);
    so.traverse(new ReflectionSchemaGenerator().generate(GoodRecord.class, true));
    dcv.validate();

    so.traverse(new ReflectionSchemaGenerator().generate(GoodRecord1.class, true));
    dcv.validate();
  }

  @Test(expected = ValidationException.class)
  public void testBadDynamic1Schemas() throws Exception {
    // Test All bad Schema types for dynamic schema.
    DynamicSchemaValidator dcv = new DynamicSchemaValidator();
    SchemaObserver so = new SchemaObserver(dcv);

    so.traverse(new ReflectionSchemaGenerator().generate(BadRecord1.class, true));
    dcv.validate();
  }

  @Test(expected = ValidationException.class)
  public void testBadDynamic2Schemas() throws Exception {
    // Test All bad Schema types for dynamic schema.
    DynamicSchemaValidator dcv = new DynamicSchemaValidator();
    SchemaObserver so = new SchemaObserver(dcv);

    so.traverse(new ReflectionSchemaGenerator().generate(BadRecord2.class, true));
    dcv.validate();
  }

  @Test(expected = ValidationException.class)
  public void testBadDynamic3Schemas() throws Exception {
    // Test All bad Schema types for dynamic schema.
    DynamicSchemaValidator dcv = new DynamicSchemaValidator();
    SchemaObserver so = new SchemaObserver(dcv);

    so.traverse(new ReflectionSchemaGenerator().generate(BadRecord3.class, true));
    dcv.validate();
  }
}
