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

import com.google.common.base.Strings;
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
import org.apache.hadoop.hbase.client.Durability;

import java.util.List;
import javax.annotation.Nullable;

/**
 * HBase Sink plugin configuration.
 */
public class HBaseSinkConfig extends ReferencePluginConfig {
  public static final String TABLE = "table";
  public static final String ROW_KEY = "rowkey";
  public static final String FAMILY = "family";
  public static final String QUORUM = "qorum";
  public static final String PORT = "port";
  public static final String DURABILITY = "durability";
  public static final String PATH = "path";

  @Name(PORT)
  @Description("Client port")
  @Nullable
  @Macro
  private final String port;

  @Name(DURABILITY)
  @Description("Durability of writes")
  @Nullable
  @Macro
  private final String durability;

  @Name(TABLE)
  @Description("Name of table")
  @Macro
  private final String table;

  @Name(ROW_KEY)
  @Description("Expression to specify row key")
  @Macro
  private final String rowkey;

  @Name(FAMILY)
  @Description("Column Family")
  @Macro
  private final String family;

  @Name(QUORUM)
  @Description("Zookeeper Server Qorum. e.g. <hostname>[[:port]:path]")
  @Nullable
  @Macro
  private final String qorum;

  @Name(PATH)
  @Description("Parent node of HBase in Zookeeper")
  @Nullable
  @Macro
  private final String path;

  public HBaseSinkConfig(String referenceName, String table, String rowkey, String family, @Nullable String qorum,
                         @Nullable String port, @Nullable String durability, @Nullable String path) {
    super(referenceName);
    this.table = table;
    this.rowkey = rowkey;
    this.family = family;
    this.qorum = qorum;
    this.port = port;
    this.durability = durability;
    this.path = path;
  }

  private HBaseSinkConfig(Builder builder) {
    super(builder.referenceName);
    port = builder.port;
    durability = builder.durability;
    table = builder.table;
    rowkey = builder.rowkey;
    family = builder.family;
    qorum = builder.qorum;
    path = builder.path;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(HBaseSinkConfig copy) {
    return new Builder()
      .setReferenceName(copy.referenceName)
      .setPort(copy.port)
      .setDurability(copy.durability)
      .setTable(copy.table)
      .setRowkey(copy.rowkey)
      .setFamily(copy.family)
      .setQorum(copy.qorum)
      .setPath(copy.path);
  }

  public void validate(FailureCollector failureCollector, Schema inputSchema) {
    try {
      IdUtils.validateId(referenceName);
    } catch (IllegalArgumentException e) {
      failureCollector.addFailure(e.getMessage(), null)
        .withConfigProperty(Constants.Reference.REFERENCE_NAME);
    }
    try {
      int port = getClientPort();
      if (port < 0 || port > 65535) {
        failureCollector.addFailure("Port must be a number in range 0-65535", null)
          .withConfigProperty(PORT);
      }
    } catch (NumberFormatException e) {
      failureCollector.addFailure("Failed to parse port: " + e.getMessage(), "Port must be a number in range 0-65535")
        .withConfigProperty(PORT)
        .withStacktrace(e.getStackTrace());
    }

    try {
      DynamicSchemaValidator dcv = new DynamicSchemaValidator();
      SchemaObserver so = new SchemaObserver(dcv);
      so.traverse(inputSchema);
      dcv.validate();

      validateRowKey(failureCollector, inputSchema);
      validateFamilyKey(failureCollector, inputSchema);

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

  private void validateFamilyKey(FailureCollector failureCollector, Schema inputSchema) {
    if (containsMacro(FAMILY) && family == null) {
      return;
    }
    // Compile Column Family expression and make sure it's ok.
    // Compile Row Key Expression and make sure it's ok.
    Expression familyExpression;
    try {
      familyExpression = new Expression(family);
    } catch (ExpressionException e) {
      failureCollector.addFailure("Error in specifying column family: " + e.getMessage(),
                                  "Column Family must be a valid expression")
        .withConfigProperty(FAMILY)
        .withStacktrace(e.getStackTrace());
      return;
    }

    List<String> variables = familyExpression.getVariables();
    for (String variable : variables) {
      Schema.Field field = inputSchema.getField(variable);
      if (field == null) {
        failureCollector.addFailure(
          String.format("Column family expression '%s' has variable '%s' that is not present in input field",
                        family, variable), null)
          .withConfigProperty(FAMILY);
      } else if (!field.getSchema().isSimpleOrNullableSimple()) {
        failureCollector.addFailure(
          String.format("Column family expression '%s' has variable '%s' that is not of type " +
                          "'string', 'int', 'long', 'float', 'double'", family, variable), null)
          .withInputSchemaField(field.getName())
          .withConfigProperty(FAMILY);
      }
    }
  }

  /**
   * @return configured port, if empty returns default 2181
   */
  public int getClientPort() {
    return Strings.isNullOrEmpty(port) ? 2181 : Integer.parseInt(port);
  }

  /**
   * @return {@link Durability} setting based on user selection.
   */
  public Durability getDurability() {
    if (durability == null) {
      return Durability.SYNC_WAL;
    }
    if (durability.equalsIgnoreCase("wal ssynchronous")) {
      return Durability.ASYNC_WAL;
    } else if (durability.equalsIgnoreCase("wal asynchronous & force disk write")) {
      return Durability.FSYNC_WAL;
    } else if (durability.equalsIgnoreCase("skip wal")) {
      return Durability.SKIP_WAL;
    } else if (durability.equalsIgnoreCase("wal synchronous")) {
      return Durability.SYNC_WAL;
    }
    return Durability.SYNC_WAL;
  }

  /**
   * @return the formatted quorum string to connect to zookeeper.
   */
  public String getQuorum() {
    return String.format("%s:%s:%s",
                         qorum == null || qorum.isEmpty() ? "localhost" : qorum,
                         getClientPort(),
                         path == null || path.isEmpty() ? "/hbase" : path
    );
  }

  public String getTable() {
    return table;
  }

  public Expression getRowKeyExpression() {
    try {
      return new Expression(rowkey);
    } catch (ExpressionException e) {
      throw new IllegalStateException("Failed to resolve expression for 'rowkey'", e);
    }
  }

  public Expression getFamilyExpression() {
    try {
      return new Expression(family);
    } catch (ExpressionException e) {
      throw new IllegalStateException("Failed to resolve expression for 'family'", e);
    }
  }

  @Nullable
  public String getQorum() {
    return qorum;
  }

  @Nullable
  public String getPath() {
    return path;
  }

  public static final class Builder {
    private String referenceName;
    private String port;
    private String durability;
    private String table;
    private String rowkey;
    private String family;
    private String qorum;
    private String path;

    private Builder() {
    }

    public Builder setReferenceName(String val) {
      referenceName = val;
      return this;
    }

    public Builder setPort(String val) {
      port = val;
      return this;
    }

    public Builder setDurability(String val) {
      durability = val;
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

    public Builder setFamily(String val) {
      family = val;
      return this;
    }

    public Builder setQorum(String val) {
      qorum = val;
      return this;
    }

    public Builder setPath(String val) {
      path = val;
      return this;
    }

    public HBaseSinkConfig build() {
      return new HBaseSinkConfig(this);
    }
  }
}
