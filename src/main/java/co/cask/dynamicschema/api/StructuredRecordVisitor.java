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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

import java.util.Map;

/**
 * Field Visitor Interface for {@link StructuredRecord}.
 */
public interface StructuredRecordVisitor {
  /**
   * Visits all fields that are of {@link StructuredRecord} type.
   *
   * @param depth Specifies the depth in the record evaluation.
   * @param name Name of the field.
   * @param field {@link Schema.Field} of the field.
   * @param value value extracted from {@link StructuredRecord} for that named field.
   * @return true to continue, false to terminate visiting further.
   */
  boolean visit(int depth, String name, Schema.Field field, StructuredRecord value) throws VisitorException;

  /**
   * Visits all fields that are of {@link String} type.
   *
   * @param depth Specifies the depth in the record evaluation.
   * @param name Name of the field.
   * @param field {@link Schema.Field} of the field.
   * @param value value extracted from {@link StructuredRecord} for that named field.
   * @return true to continue, false to terminate visiting further.
   */
  boolean visit(int depth, String name, Schema.Field field, String value) throws VisitorException;

  /**
   * Visits all fields that are of {@link Integer} type.
   *
   * @param depth Specifies the depth in the record evaluation.
   * @param name Name of the field.
   * @param field {@link Schema.Field} of the field.
   * @param value value extracted from {@link StructuredRecord} for that named field.
   * @return true to continue, false to terminate visiting further.
   */
  boolean visit(int depth, String name, Schema.Field field, Integer value) throws VisitorException;

  /**
   * Visits all fields that are of {@link Float} type.
   *
   * @param depth Specifies the depth in the record evaluation.
   * @param name Name of the field.
   * @param field {@link Schema.Field} of the field.
   * @param value value extracted from {@link StructuredRecord} for that named field.
   * @return true to continue, false to terminate visiting further.
   */
  boolean visit(int depth, String name, Schema.Field field, Float value) throws VisitorException;

  /**
   * Visits all fields that are of {@link Double} type.
   *
   * @param depth Specifies the depth in the record evaluation.
   * @param name Name of the field.
   * @param field {@link Schema.Field} of the field.
   * @param value value extracted from {@link StructuredRecord} for that named field.
   * @return true to continue, false to terminate visiting further.
   */
  boolean visit(int depth, String name, Schema.Field field, Double value) throws VisitorException;

  /**
   * Visits all fields that are of {@link Boolean} type.
   *
   * @param depth Specifies the depth in the record evaluation.
   * @param name Name of the field.
   * @param field {@link Schema.Field} of the field.
   * @param value value extracted from {@link StructuredRecord} for that named field.
   * @return true to continue, false to terminate visiting further.
   */
  boolean visit(int depth, String name, Schema.Field field, Boolean value) throws VisitorException;

  /**
   * Visits all fields that are of {@link Long} type.
   *
   * @param depth Specifies the depth in the record evaluation.
   * @param name Name of the field.
   * @param field {@link Schema.Field} of the field.
   * @param value value extracted from {@link StructuredRecord} for that named field.
   * @return true to continue, false to terminate visiting further.
   */
  boolean visit(int depth, String name, Schema.Field field, Long value) throws VisitorException;

  /**
   * Visits all fields that are of {@link Map} type.
   *
   * @param depth Specifies the depth in the record evaluation.
   * @param name Name of the field.
   * @param field {@link Schema.Field} of the field.
   * @param value value extracted from {@link StructuredRecord} for that named field.
   * @return true to continue, false to terminate visiting further.
   */
  boolean visit(int depth, String name, Schema.Field field, Map<String, String> value) throws VisitorException;

  /**
   * Visits all fields that are of {@link byte[]} type.
   *
   * @param depth Specifies the depth in the record evaluation.
   * @param name Name of the field.
   * @param field {@link Schema.Field} of the field.
   * @param value value extracted from {@link StructuredRecord} for that named field.
   * @return true to continue, false to terminate visiting further.
   */
  boolean visit(int depth, String name, Schema.Field field, byte[] value) throws VisitorException;

  /**
   * Visits all fields that are of 'null' type.
   *
   * @param depth Specifies the depth in the record evaluation.
   * @param name Name of the field.
   * @param field {@link Schema.Field} of the field.
   * @return true to continue, false to terminate visiting further.
   */
  boolean visit(int depth, String name, Schema.Field field) throws VisitorException;
}
