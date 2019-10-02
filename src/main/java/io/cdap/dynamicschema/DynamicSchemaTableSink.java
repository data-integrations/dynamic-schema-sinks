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
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.dynamicschema.api.Expression;
import io.cdap.dynamicschema.observer.StructuredRecordObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dynamic Schema support for writing to Table.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("DynTable")
@Description("Dynamic Schema support for Table writes.")
public class DynamicSchemaTableSink extends BatchSink<StructuredRecord, byte[], Put> {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicSchemaTableSink.class);

  /**
   * HBase Plugin configuration to read configuration from JSON.
   */
  private TableSinkConfig config;

  /**
   * Expression evaluator for generating row key.
   */
  private Expression rowKeyExpression;

  public DynamicSchemaTableSink(TableSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    Schema inputSchema = configurer.getStageConfigurer().getInputSchema();
    FailureCollector failureCollector = configurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector, inputSchema);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws DatasetManagementException {
    Schema inputSchema = context.getInputSchema();
    FailureCollector failureCollector = context.getFailureCollector();
    config.validate(failureCollector, inputSchema);

    if (!context.datasetExists(config.getTable())) {
      context.createDataset(config.getTable(), Table.class.getName(), DatasetProperties.builder().build());
    }
    context.addOutput(Output.ofDataset(config.getTable()));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    // Row key resolver setup, we know by now that the expression is valid.
    rowKeyExpression = config.getRowKeyExpression();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<byte[], Put>> emitter) throws Exception {
    String row = rowKeyExpression.apply(input);

    TablePutGenerator generator = new TablePutGenerator(Bytes.toBytes(row));
    StructuredRecordObserver sro = new StructuredRecordObserver(generator);
    sro.traverse(input);

    // Visit all the fields and perform necessary operations.
    emitter.emit(new KeyValue<>(Bytes.toBytes(row), generator.get()));
  }
}
