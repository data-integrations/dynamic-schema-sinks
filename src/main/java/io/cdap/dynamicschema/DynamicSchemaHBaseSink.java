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
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.dynamicschema.api.Expression;
import io.cdap.dynamicschema.observer.StructuredRecordObserver;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.batch.JobUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Dynamic Schema support for writing to HBase.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("DynHBase")
@Description("Dynamic Schema support for HBase writes.")
public class DynamicSchemaHBaseSink extends ReferenceBatchSink<StructuredRecord, NullWritable, Mutation> {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicSchemaHBaseSink.class);

  /**
   * HBase Plugin configuration to read configuration from JSON.
   */
  private HBaseSinkConfig config;

  /**
   * Expression evaluator for generating row key.
   */
  private Expression rowKeyExpression;

  /**
   * Expression evaluator for generating column family.
   */
  private Expression familyExpression;

  public DynamicSchemaHBaseSink(HBaseSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    Schema inputSchema = configurer.getStageConfigurer().getInputSchema();
    FailureCollector failureCollector = configurer.getStageConfigurer().getFailureCollector();
    validateConfiguration(inputSchema, failureCollector);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    Schema inputSchema = context.getInputSchema();
    FailureCollector failureCollector = context.getFailureCollector();
    validateConfiguration(inputSchema, failureCollector);

    Job job;
    ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
    // Switch the context classloader to plugin class' classloader (PluginClassLoader) so that
    // when Job/Configuration is created, it uses PluginClassLoader to load resources (hbase-default.xml)
    // which is present in the plugin jar and is not visible in the CombineClassLoader (which is what oldClassLoader
    // points to).
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    try {
      job = JobUtils.createInstance();
    } finally {
      // Switch back to the original
      Thread.currentThread().setContextClassLoader(oldClassLoader);
    }

    Configuration conf = job.getConfiguration();
    HBaseConfiguration.addHbaseResources(conf);

    context.addOutput(Output.of(config.referenceName, new HBaseOutputFormatProvider(config, conf)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);

    // Row key resolver setup, we know by now that the expression is valid.
    rowKeyExpression = config.getRowKeyExpression();

    // Column family resolver setup, we know by now that is also valid.
    familyExpression = config.getFamilyExpression();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Mutation>> emitter) throws Exception {
    String row = rowKeyExpression.apply(input);
    String family = familyExpression.apply(input);

    // Visit all the fields and perform necessary operations.
    HBasePutGenerator dcs = new HBasePutGenerator(Bytes.toBytes(row),
                                                  Bytes.toBytes(family), config.getDurability());
    StructuredRecordObserver sro = new StructuredRecordObserver(dcs);
    sro.traverse(input);

    // Emit the PUT to go to HBase.
    emitter.emit(new KeyValue<NullWritable, Mutation>(NullWritable.get(), dcs.get()));
  }

  private void validateConfiguration(Schema inputSchema, FailureCollector failureCollector) {
    // Get the input schema and validate if there are fields that support dynamic schema.
    config.validate(failureCollector, inputSchema);
    failureCollector.getOrThrowException();
    validateTable(failureCollector);
    failureCollector.getOrThrowException();
  }

  private void validateTable(FailureCollector failureCollector) {
    if (config.containsMacro(HBaseSinkConfig.TABLE) || config.containsMacro(HBaseSinkConfig.QUORUM)
      || config.containsMacro(HBaseSinkConfig.PORT)) {
      return;
    }
    // Check if the table exists on HBase.
    try {
      Configuration conf = HBaseConfiguration.create();
      conf.set(HConstants.ZOOKEEPER_QUORUM, config.getQorum());
      conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, config.getClientPort());
      HBaseAdmin admin = new HBaseAdmin(conf);
      if (!admin.tableExists(config.getTable())) {
        failureCollector.addFailure(String.format("HBase table '%s' does not exist", config.getTable()), null)
          .withConfigProperty(HBaseSinkConfig.TABLE);
      }
    } catch (MasterNotRunningException e) {
      failureCollector.addFailure("HBase master is not running", "Check the status of HBase")
        .withStacktrace(e.getStackTrace());
    } catch (ZooKeeperConnectionException e) {
      failureCollector.addFailure("Unable to connect to zookeeper: " + e.getMessage(),
                                  "Check zookeeper quorum configuration")
        .withStacktrace(e.getStackTrace());
    } catch (IOException e) {
      failureCollector.addFailure(String.format("Unable to connect to HBase table '%s'", config.getTable()), null)
        .withStacktrace(e.getStackTrace());
    }
  }

  /**
   * Provider for HBase Table Output Format.
   */
  private class HBaseOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    public HBaseOutputFormatProvider(HBaseSinkConfig config, Configuration configuration) {
      this.conf = new HashMap<>();

      conf.put(TableOutputFormat.OUTPUT_TABLE, config.getTable());
      conf.put(TableOutputFormat.QUORUM_ADDRESS, config.getQuorum());
      conf.put(TableOutputFormat.QUORUM_PORT, String.valueOf(config.getClientPort()));
      String[] serializationClasses = {
        configuration.get("io.serializations"),
        MutationSerialization.class.getName(),
        ResultSerialization.class.getName(),
        KeyValueSerialization.class.getName()
      };
      conf.put("io.serializations", StringUtils.arrayToString(serializationClasses));
    }

    /**
     * @return the class table of the output format to use.
     */
    public String getOutputFormatClassName() {
      return TableOutputFormat.class.getName();
    }

    /**
     * @return the configuration properties that the output format expects to
     * find in the Hadoop configuration.
     */
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

}
