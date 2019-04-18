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

package io.cdap.dynamicschema.api;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.MapContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Expression for
 */
public class Expression {
  private final String condition;
  private final JexlScript script;

  public static class Convertors {
    public static String cons(String value) {
      return value;
    }
  }

  public Expression(String condition) throws ExpressionException {
    this.condition = condition;

    Map<String, Object> functions = new HashMap<String, Object>();
    functions.put(null, Convertors.class);
    JexlEngine engine = new JexlBuilder().namespaces(functions)
      .silent(false).cache(1000).strict(true).create();

    try {
      script = engine.createScript(condition);
    } catch (JexlException e) {
      if (e.getCause() != null) {
        throw new ExpressionException(e.getCause().getMessage());
      } else {
        throw new ExpressionException(e.getMessage());
      }
    }
  }

  public List<String> getVariables() {
    List<String> variables = new ArrayList<String>();
    Set<List<String>> groups = script.getVariables();
    for (List<String> group : groups ) {
      variables.addAll(group);
    }
    return variables;
  }

  public String apply(StructuredRecord record) throws ExpressionException {
    JexlContext ctx = new MapContext();
    for (Schema.Field field : record.getSchema().getFields()) {
      if(field.getSchema().isSimpleOrNullableSimple()) {
        ctx.set(field.getName(), record.get(field.getName()));
      }
    }

    try {
      String result = (String) script.execute(ctx);
      return result;
    } catch (JexlException e) {
      // Generally JexlException wraps the original exception, so it's good idea
      // to check if there is a inner exception, if there is wrap it in 'StepException'
      // else just print the error message.
      if (e.getCause() != null) {
        throw new ExpressionException(e.getCause().getMessage());
      } else {
        throw new ExpressionException(e.getMessage());
      }
    }
  }
}
