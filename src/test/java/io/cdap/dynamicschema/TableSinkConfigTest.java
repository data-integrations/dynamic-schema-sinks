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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.Constants;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TableSinkConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final Schema VALID_INPUT_SCHEMA = Schema.recordOf(
    "schema",
    Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.LONG)))
  );
  private static final TableSinkConfig VALID_CONFIG = new TableSinkConfig(
    "ref",
    "",
    "id"
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector, VALID_INPUT_SCHEMA);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateReference() {
    TableSinkConfig config = TableSinkConfig.builder(VALID_CONFIG)
      .setReferenceName("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_INPUT_SCHEMA);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, Constants.Reference.REFERENCE_NAME);
  }

  @Test
  public void testValidateMissingRowKeyInSchema() {
    TableSinkConfig config = TableSinkConfig.builder(VALID_CONFIG)
      .setRowkey("key")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_INPUT_SCHEMA);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, TableSinkConfig.ROW_KEY);
  }

  @Test
  public void testValidateWrongRowKeyExpression() {
    TableSinkConfig config = TableSinkConfig.builder(VALID_CONFIG)
      .setRowkey("#")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_INPUT_SCHEMA);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, TableSinkConfig.ROW_KEY);
    ValidationAssertions.assertValidationFailedWithStacktrace(failureCollector);
  }

  @Test
  public void testValidateWrongRowKeyType() {
    Schema inputSchema = Schema.recordOf(
      "schema",
      Schema.Field.of("id", Schema.nullableOf(Schema.enumWith("v1")))
    );

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector, inputSchema);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, TableSinkConfig.ROW_KEY);
    ValidationAssertions.assertInputSchemaValidationFailed(failureCollector, "id");
  }

  @Test
  public void testValidInputSchemaWithArray() {
    Schema recordSchema = Schema.recordOf(
      "record",
      Schema.Field.of("field", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("value", Schema.of(Schema.Type.STRING))
    );
    Schema inputSchema = Schema.recordOf(
      "schema",
      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("dynamic", Schema.arrayOf(recordSchema))
    );

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector, inputSchema);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidInputSchemaWithMap() {
    Schema inputSchema = Schema.recordOf(
      "schema",
      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("dynamic", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)))
    );

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector, inputSchema);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateWrongInputSchema() {
    Schema recordSchema = Schema.recordOf(
      "record",
      Schema.Field.of("field", Schema.of(Schema.Type.STRING))
    );
    Schema inputSchema = Schema.recordOf(
      "schema",
      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("dynamic", Schema.arrayOf(recordSchema))
    );

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector, inputSchema);
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());
  }
}
