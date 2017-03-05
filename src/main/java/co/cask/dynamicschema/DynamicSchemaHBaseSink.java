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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.format.RecordPutTransformer;
import co.cask.dynamicschema.api.ValidationException;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.batch.JobUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.util.HashMap;
import java.util.Map;

/**
 * Dynamic Schema support for writing to HBase.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("DynamicSchemaHBase")
@Description("Dynamic Schema support for HBase writes.")
public class DynamicSchemaHBaseSink extends ReferenceBatchSink<StructuredRecord, NullWritable, Mutation> {

  private HBaseSinkConfig config;
  private RecordPutTransformer recordPutTransformer;

  public DynamicSchemaHBaseSink(HBaseSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    // Get the input schema and validate if there are fields that
    // support dynamic schema.
    Schema schema = configurer.getStageConfigurer().getInputSchema();
    DynamicSchemaValidator dcv = new DynamicSchemaValidator();
    SchemaObserver so = new SchemaObserver(dcv);
    so.traverse(schema);
    try {
      dcv.validate();
    } catch (ValidationException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
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
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Mutation>> emitter) throws Exception {
    for (Schema.Field field : input.getSchema().getFields()) {
    }
  }


  /**
   * Provider for HBase Table Output Format.
   */
  private class HBaseOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    public HBaseOutputFormatProvider(HBaseSinkConfig config, Configuration configuration) {
      this.conf = new HashMap<String, String>();
      conf.put(TableOutputFormat.OUTPUT_TABLE, config.name);
      conf.put(TableOutputFormat.QUORUM_ADDRESS, config.qorum);
      conf.put(TableOutputFormat.QUORUM_PORT, config.port);
    }

    /**
     * @return the class name of the output format to use.
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
