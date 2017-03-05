## Dynamic or Variable Schema Table

This plugin support writing dynamic schemas to CDAP Dataset Table in addition to writing regular structured records
to Table.

### Use-case


### Usage Notes

#### Defining Dynamic Schema

#### Row Key Expression

Row key generation supports expression for creating row key. The expression can simple or complex.

  * Simple expression specifies just the field name of the input record to this plugin.
  * Complex expression can specify an JEXL expression using the input record field names as variables.

> The row key expression cannot be a constant. If the user specifies a constant, a deployment time error is generated.

The expression specified for row key can be constructed based on the input record field that are of the following
types.

  * integer,
  * double,
  * float,
  * string and
  * long

In general, the row key expression supports only simple data types.

Let's assume a input record with following fields in the record:

```
  { "fname", "lname", "ssn", "age", address", "city", "state", "zipcode" }
```

The row key expression

```
  fname + ":" + lname
```

would generate a key for every record that would concatenate 'fname' and 'lname' using ':'.

Another expression

```
  ssn
```

would use the value of 'ssn' field in the record as the key.

A slightly complex expression can include operations as follows:

```
  ssn + (age % 10)
```

#### Column Family

Similar to Row Key expression, the column family expression supports the same capabilities with addition of being able
to specify the column family name as constant.

Instead of an expression, let's assume you are interested in specifying a constant 'C1' for column family, then the
expression specifies the constant by enclosing them in double-quotes("). An example is as below.

```
  "C1"
```

would write the values to the C1 column family.

##### Expression

Input record
```
  {
    "col1" : "name",
    "col2" : 2.8f,
    "col3" : 1,
    "other" : "other"
  }
```

```
  col1 + ":" + col2 + ":" + col3
```

Would generate row key as

```
  name:2.8:1
```


## Configuration

## Additional Notes


