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

package io.cdap.dynamicschema.observer;

import io.cdap.dynamicschema.api.Observer;
import io.cdap.dynamicschema.api.ObserverException;
import io.cdap.dynamicschema.api.SchemaVisitor;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.dynamicschema.api.VisitorException;

/**
 * Structured Record Observer.
 */
public final class SchemaObserver implements Observer<Schema> {
  private final SchemaVisitor visitor;

  public SchemaObserver(SchemaVisitor visitor) {
    this.visitor = visitor;
  }

  public void traverse(Schema schema) throws ObserverException {
    traverse(schema, 0);
  }

  private void traverse(Schema schema, int depth) throws ObserverException {
    for (Schema.Field field : schema.getFields()) {
      Schema.Type type;
      if (!field.getSchema().isNullable()) {
        type = field.getSchema().getType();
      } else {
        type = field.getSchema().getNonNullable().getType();
      }

      String name = field.getName();

      try {
        boolean exit = false;
        switch(type) {
          case INT:
            if(!visitor.visitInt(depth, name, field)) {
              exit = true;
            }
            break;

          case FLOAT:
            if(!visitor.visitFloat(depth, name, field)) {
              exit = true;
            }
            break;

          case DOUBLE:
            if(!visitor.visitDouble(depth, name, field)) {
              exit = true;
            }
            break;

          case LONG:
            if(!visitor.visitLong(depth, name, field)) {
              exit = true;
            }
            break;

          case BOOLEAN:
            if(!visitor.visitBoolean(depth, name, field)) {
              exit = true;
            }
            break;

          case STRING:
            if(!visitor.visitString(depth, name, field)) {
              exit = true;
            }
            break;

          case BYTES:
            if(!visitor.visitBytes(depth, name, field)) {
              exit = true;
            }
            break;

          case NULL:
            if(!visitor.visitNull(depth, name, field)) {
              exit = true;
            }
            break;

          case MAP:
            if(!visitor.visitMap(depth, name, field)){
              exit = true;
            }
            break;

          case ARRAY:
            if (!visitor.visitArray(depth + 1, name, field)) {
              exit = true;
              break;
            }
        }
      } catch (VisitorException e) {
        throw new ObserverException(
          e.getMessage()
        );
      }
    }
  }
}
