package co.cask.dynamicschema;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
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
    boolean status = dsc.validate();
    Assert.assertTrue(status);

    byte[] row = Bytes.toBytes("A");
    byte[] family = Bytes.toBytes("B");
    DynamicSchemaHBasePut sp = new DynamicSchemaHBasePut(row, family, Durability.SYNC_WAL);
    StructuredRecordObserver walker = new StructuredRecordObserver(sp);
    walker.traverse(record);
    Put p = sp.get();

    Assert.assertTrue(1==1);
  }

  @Test
  public void testGoodDynamicSchemas() throws Exception {
    // Test All good Schema types for dynamic schema.
    DynamicSchemaValidator dcv = new DynamicSchemaValidator();
    SchemaObserver so = new SchemaObserver(dcv);
    so.traverse(new ReflectionSchemaGenerator().generate(GoodRecord.class, true));
    Assert.assertTrue(dcv.validate());

    so.traverse(new ReflectionSchemaGenerator().generate(GoodRecord1.class, true));
    Assert.assertTrue(dcv.validate());
  }

  @Test
  public void testBadDynamicSchemas() throws Exception {
    // Test All bad Schema types for dynamic schema.
    DynamicSchemaValidator dcv = new DynamicSchemaValidator();
    SchemaObserver so = new SchemaObserver(dcv);

    so.traverse(new ReflectionSchemaGenerator().generate(BadRecord1.class, true));
    Assert.assertFalse(dcv.validate());

    so.traverse(new ReflectionSchemaGenerator().generate(BadRecord2.class, true));
    Assert.assertFalse(dcv.validate());

    so.traverse(new ReflectionSchemaGenerator().generate(BadRecord3.class, true));
    Assert.assertFalse(dcv.validate());
  }




}