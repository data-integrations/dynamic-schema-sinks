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
import co.cask.hydrator.common.ReferencePluginConfig;

import javax.annotation.Nullable;

/**
 * HBaseSink plugin.
 */
public class HBaseSinkConfig extends ReferencePluginConfig {
  @Name("table")
  @Description("Name of table")
  public String name;

  @Name("rowkey")
  @Description("Expression to specify row key")
  public String rowkey;

  @Name("qorum")
  @Description("Zookeeper Server Qorum. e.g. <hostname>[[:port]:path]")
  @Nullable
  public String qorum;

  @Name("port")
  @Description("Client port")
  @Nullable
  public String port;

  @Name("durability")
  @Description("Durability of writes")
  @Nullable
  public String durability;

  public HBaseSinkConfig(String referenceName, String name, String rowkey, String qorum,
                         String port, String durability) {
    super(referenceName);
    this.name = name;
    this.rowkey = rowkey;
    this.qorum = qorum;
    this.port = port;
    this.durability = durability;
  }

  public int getClientPort() {
    try {
      return Integer.parseInt(port);
    } catch (NumberFormatException e) {
      return 2181;
    }
  }
}
