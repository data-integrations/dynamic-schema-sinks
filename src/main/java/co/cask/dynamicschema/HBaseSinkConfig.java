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
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.hydrator.common.ReferencePluginConfig;
import org.apache.hadoop.hbase.client.Durability;

import javax.annotation.Nullable;

/**
 * HBase Sink plugin configuration.
 */
public class HBaseSinkConfig extends ReferencePluginConfig {
  @Name("table")
  @Description("Name of table")
  @Macro
  public String table;

  @Name("rowkey")
  @Description("Expression to specify row key")
  @Macro
  public String rowkey;

  @Name("family")
  @Description("Column Family")
  @Macro
  public String family;


  @Name("namespace")
  @Description("Namespace to be used")
  @Nullable
  @Macro
  public String namespace;

  @Name("qorum")
  @Description("Zookeeper Server Qorum. e.g. <hostname>[[:port]:path]")
  @Nullable
  @Macro
  public String qorum;

  @Name("port")
  @Description("Client port")
  @Nullable
  @Macro
  public String port;

  @Name("durability")
  @Description("Durability of writes")
  @Nullable
  @Macro
  public String durability;

  @Name("path")
  @Description("Parent node of HBase in Zookeeper")
  @Nullable
  @Macro
  public String path;

  public HBaseSinkConfig(String referenceName, String table, String rowkey, String family, String qorum,
                         String port, String durability, String path) {
    super(referenceName);
    this.table = table;
    this.rowkey = rowkey;
    this.family = family;
    this.qorum = qorum;
    this.port = port;
    this.durability = durability;
    this.path = path;
  }

  public int getClientPort() {
    try {
      return Integer.parseInt(port);
    } catch (NumberFormatException e) {
      return 2181;
    }
  }


  public Durability getDurability() {
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
}
