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

package co.cask.dynamicschema.api;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Tests {@link Expression}
 */
public class ExpressionTest {

  private class RecordSchema {
    int a;
    String s;
    float f;
    double d;
    long l;
    Map<String, String> m;
  }

  @Test
  public void testBasicFunctionality() throws Exception {
    Schema schema = new ReflectionSchemaGenerator().generate(RecordSchema.class);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    builder.set("a", 9);
    builder.set("s", "test");
    builder.set("f", 2.8f);
    builder.set("d", 1.9);
    builder.set("l", 7887989);
    StructuredRecord record = builder.build();

    // Good case.
    Expression resolver = new Expression("a + s + f");
    String key = resolver.apply(record);
    Assert.assertEquals("9test2.8", key);

    // Bad case.
    try {
      // Uses map field
      Expression resolver1 = new Expression("a + s + m");
      List<String> variables = resolver1.getVariables();
      Assert.assertEquals(3, variables.size());
      resolver1.apply(record);
      Assert.assertTrue(false);
    } catch (ExpressionException e) {
      Assert.assertTrue(true);
    }

    // Using a constant.
    Expression resolver2 = new Expression("\"c1\"");
    key = resolver2.apply(record);
    Assert.assertEquals("c1", key);
  }
}