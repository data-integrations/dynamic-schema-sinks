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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.dynamicschema.api.Expression;
import io.cdap.dynamicschema.api.ExpressionException;
import io.cdap.dynamicschema.api.ObserverException;
import io.cdap.dynamicschema.api.ValidationException;
import io.cdap.dynamicschema.observer.SchemaObserver;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.ReferencePluginConfig;

import java.util.List;

/**
 * Table Sink plugin configuration.
 */
public class TableSinkConfig extends ReferencePluginConfig {
  public static final String TABLE = "table";
  public static final String ROW_KEY = "rowkey";

  @Name(TABLE)
  @Description("Name of table")
  @Macro
  private final String table;

  @Name(ROW_KEY)
  @Description("Expression to specify row key")
  @Macro
  private final String rowkey;

  public TableSinkConfig(String referenceName, String table, String rowkey) {
    super(referenceName);
    this.table = table;
    this.rowkey = rowkey;
  }

  private TableSinkConfig(Builder builder) {
    super(builder.referenceName);
    table = builder.table;
    rowkey = builder.rowkey;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(TableSinkConfig copy) {
    return new Builder()
      .setReferenceName(copy.referenceName)
      .setTable(copy.table)
      .setRowkey(copy.rowkey);
  }

  public void validate(FailureCollector failureCollector, Schema inputSchema) {
    try {
      IdUtils.validateId(referenceName);
    } catch (IllegalArgumentException e) {
      failureCollector.addFailure(e.getMessage(), null)
        .withConfigProperty(Constants.Reference.REFERENCE_NAME);
    }
    try {
      DynamicSchemaValidator dcv = new DynamicSchemaValidator();
      SchemaObserver so = new SchemaObserver(dcv);
      so.traverse(inputSchema);
      dcv.validate();

      validateRowKey(failureCollector, inputSchema);
    } catch (ValidationException | ObserverException e) {
      failureCollector.addFailure("Invalid input schema: " + e.getMessage(),
                                  "Ensure input schema is valid dynamic schema");
    }
  }

  private void validateRowKey(FailureCollector failureCollector, Schema inputSchema) {
    if (containsMacro(ROW_KEY) && rowkey == null) {
      return;
    }
    // Compile Row Key Expression and make sure it's ok.
    Expression rowKeyExpression;
    try {
      rowKeyExpression = new Expression(rowkey);
    } catch (ExpressionException e) {
      failureCollector.addFailure("Error in specifying row key: " + e.getMessage(),
                                  "Row key must be a valid expression")
        .withConfigProperty(ROW_KEY)
        .withStacktrace(e.getStackTrace());
      return;
    }

    List<String> variables = rowKeyExpression.getVariables();
    // If there are no variables, then row key is not correctly formed by the user.
    if (variables.size() == 0) {
      failureCollector.addFailure("Row key is not correctly formed",
                                  "Please specify an input field name or an expression. " +
                                    "You cannot use a constant for the row key.")
        .withConfigProperty(ROW_KEY);
    }
    // We kow check if all the variables in the expression are present in the input schema and they are of simple types.
    for (String variable : variables) {
      Schema.Field field = inputSchema.getField(variable);
      if (field == null) {
        failureCollector.addFailure(
          String.format("Row key expression '%s' has variable '%s' that is not present in input field",
                        rowkey, variable), null)
          .withConfigProperty(ROW_KEY);
      } else if (!field.getSchema().isSimpleOrNullableSimple()) {
        failureCollector.addFailure(
          String.format("Row key expression '%s' has variable '%s' that is not of type " +
                          "'string', 'int', 'long', 'float', 'double'", rowkey, variable), null)
          .withInputSchemaField(field.getName())
          .withConfigProperty(ROW_KEY);
      }
    }
  }

  public Expression getRowKeyExpression() {
    try {
      return new Expression(rowkey);
    } catch (ExpressionException e) {
      throw new IllegalStateException("Failed to resolve expression for 'rowkey'", e);
    }
  }

  public String getTable() {
    return table;
  }

  public static final class Builder {
    private String referenceName;
    private String table;
    private String rowkey;

    private Builder() {
    }

    public Builder setReferenceName(String val) {
      referenceName = val;
      return this;
    }

    public Builder setTable(String val) {
      table = val;
      return this;
    }

    public Builder setRowkey(String val) {
      rowkey = val;
      return this;
    }

    public TableSinkConfig build() {
      return new TableSinkConfig(this);
    }
  }
}
