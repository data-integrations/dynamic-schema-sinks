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
import org.apache.commons.jexl3.scripting.JexlScriptEngine;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

/**
 * Created by nitin on 3/3/17.
 */
@SuppressWarnings("ALL")
public class RowKeyResolver {
  private final String condition;
  private final CompiledScript script;

  public RowKeyResolver(String condition) throws KeyResolverException {
    this.condition = condition;
    JexlScriptEngine engine = new JexlScriptEngine();
    try {
      script = engine.compile(condition);
    } catch (ScriptException e) {
      if (e.getCause() != null) {
        throw new KeyResolverException(e.getCause().getMessage());
      } else {
        throw new KeyResolverException(e.getMessage());
      }
    }
  }

  public Object apply(StructuredRecord record) throws KeyResolverException {
    Bindings ctx = new SimpleBindings();
    for (int i = 0; i < record.getSchema().getFields().size(); ++i) {
      ctx.put(null, null);
    }

    try {
      Object result = script.eval(ctx);
      return result;
    } catch (ScriptException e) {
      // Generally JexlException wraps the original exception, so it's good idea
      // to check if there is a inner exception, if there is wrap it in 'StepException'
      // else just print the error message.
      if (e.getCause() != null) {
        throw new KeyResolverException(e.getCause().getMessage());
      } else {
        throw new KeyResolverException(e.getMessage());
      }
    }
  }
}
