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

import co.cask.cdap.api.data.schema.Schema;

/**
 * Field Visitor Interface
 */
public class AbstractSchemaVisitor implements SchemaVisitor {
  public boolean visitInt(int depth, String name, Schema.Field field) {
    return true;
  }

  public boolean visitFloat(int depth, String name, Schema.Field field) {
    return true;
  }

  public boolean visitDouble(int depth, String name, Schema.Field field) {
    return true;
  }

  public boolean visitLong(int depth, String name, Schema.Field field) {
    return true;
  }

  public boolean visitBoolean(int depth, String name, Schema.Field field) {
    return true;
  }

  public boolean visitString(int depth, String name, Schema.Field field) {
    return true;
  }

  public boolean visitBytes(int depth, String name, Schema.Field field) {
    return true;
  }

  public boolean visitNull(int depth, String name, Schema.Field field) {
    return true;
  }

  public boolean visitMap(int depth, String name, Schema.Field field) {
    return true;
  }

  public boolean visitArray(int i, String name, Schema.Field field) {
    return true;
  }
}
