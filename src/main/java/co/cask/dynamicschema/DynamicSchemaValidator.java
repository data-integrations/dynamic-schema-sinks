package co.cask.dynamicschema;

import co.cask.dynamicschema.api.AbstractSchemaVisitor;
import co.cask.dynamicschema.api.ValidationException;
import co.cask.dynamicschema.api.Validator;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.dynamicschema.api.VisitorException;

import java.util.ArrayList;
import java.util.List;

public class DynamicSchemaValidator extends AbstractSchemaVisitor implements Validator {
  private boolean valid;
  private String message;

  public DynamicSchemaValidator() {
    valid = true;
    message = "OK";
  }

  @Override
  public boolean visitArray(int i, String name, Schema.Field field) throws VisitorException {
    valid = true;
    message = "OK";

    List<Schema.Field> fields = new ArrayList<>();
    try {
      Schema schema = field.getSchema().getNonNullable().getComponentSchema().getNonNullable();
       fields = schema.getFields();
    } catch (Exception e) {
      throw new VisitorException (
        "Dynamic schema can only support array of records. Please make sure you have only array of records."
      );
    }

    int size = fields.size();
    if (size != 2 && size != 3) {
      valid = false;
      message = "Dynamic schema record type is not either having a record of 'field' and 'value' or " +
        "record of 'field', 'value', 'type'";
      return false;
    }

    int count = 0;
    for (Schema.Field f : fields) {
      String fieldname = f.getName();
      if (fieldname.equalsIgnoreCase("field")) {
        count++;
      }
      if (fieldname.equalsIgnoreCase("value")) {
        count++;
      }
      if (fieldname.equalsIgnoreCase("type")) {
        count++;
      }
    }

    if (size != count) {
      valid = false;
      if (size == 2) {
        message = "Dynamic schema does not specify required fields 'field' and 'value' as the fields for the record.";
      } else {
        message = "Dynamic schema does not specify required fields 'field', 'value' and 'type' as fields for the record.";
      }
    }

    return true;
  }

  public void validate() throws ValidationException {
    if (!valid) {
      throw new ValidationException(message);
    }
  }

}
