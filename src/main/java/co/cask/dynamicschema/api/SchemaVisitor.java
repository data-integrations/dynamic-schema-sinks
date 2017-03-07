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
 * Schema Visitor pattern.
 */
public interface SchemaVisitor {
  /**
   * Called when schema has type INT.
   *
   * @param depth depth into schema.
   * @param name of the field.
   * @param field type.
   * @return true to continue, false to stop processing.
   */
  boolean visitInt(int depth, String name, Schema.Field field) throws VisitorException;

  /**
   * Called when schema has type FLOAT.
   *
   * @param depth depth into schema.
   * @param name of the field.
   * @param field type.
   * @return true to continue, false to stop processing.
   */
  boolean visitFloat(int depth, String name, Schema.Field field) throws VisitorException;

  /**
   * Called when schema has type DOUBLE.
   *
   * @param depth depth into schema.
   * @param name of the field.
   * @param field type.
   * @return true to continue, false to stop processing.
   */
  boolean visitDouble(int depth, String name, Schema.Field field) throws VisitorException;

  /**
   * Called when schema has type LONG.
   *
   * @param depth depth into schema.
   * @param name of the field.
   * @param field type.
   * @return true to continue, false to stop processing.
   */
  boolean visitLong(int depth, String name, Schema.Field field) throws VisitorException;

  /**
   * Called when schema has type BOOLEAN.
   *
   * @param depth depth into schema.
   * @param name of the field.
   * @param field type.
   * @return true to continue, false to stop processing.
   */
  boolean visitBoolean(int depth, String name, Schema.Field field) throws VisitorException;

  /**
   * Called when schema has type STRING.
   *
   * @param depth depth into schema.
   * @param name of the field.
   * @param field type.
   * @return true to continue, false to stop processing.
   */
  boolean visitString(int depth, String name, Schema.Field field) throws VisitorException;

  /**
   * Called when schema has type BYTES.
   *
   * @param depth depth into schema.
   * @param name of the field.
   * @param field type.
   * @return true to continue, false to stop processing.
   */
  boolean visitBytes(int depth, String name, Schema.Field field) throws VisitorException;

  /**
   * Called when schema has type NULL.
   *
   * @param depth depth into schema.
   * @param name of the field.
   * @param field type.
   * @return true to continue, false to stop processing.
   */
  boolean visitNull(int depth, String name, Schema.Field field) throws VisitorException;

  /**
   * Called when schema has type MAP.
   *
   * @param depth depth into schema.
   * @param name of the field.
   * @param field type.
   * @return true to continue, false to stop processing.
   */
  boolean visitMap(int depth, String name, Schema.Field field) throws VisitorException;

  /**
   * Called when schema has type ARRAY.
   *
   * @param depth depth into schema.
   * @param name of the field.
   * @param field type.
   * @return true to continue, false to stop processing.
   */
  boolean visitArray(int depth, String name, Schema.Field field) throws VisitorException;
}
