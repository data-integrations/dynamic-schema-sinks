## Dynamic or Variable Schema Table

This plugin supports writing dynamic schemas record to CDAP Dataset Table. In addition to writing dynamic schema tables, it also support regular structured records to be written to Tables.

Schema design determines the way an application handles its data. With traditional relational databases,
you must define your schema before you can add any data. This inflexibility means you can’t change your schema as
your data, application requirements or business evolves. In today’s world hyper-competitive, global business
environment, this can hamper your efforts to innovate and stay on top of the competition.

NoSQL databases arose to address this limitation by allowing you to insert data without a predefined schema.
Because of this, you can easily make changes to an application without interruption. The result is more reliable
code integration, faster development, and database administration time.

### Usage Notes

In cases, where one does not know in advance the columns the table would have, this plugin is very useful. It uses pre-defined structure to provide dynamic schema capabilities.
 
In order to define dynamic schemas, one has to use **'Map'** or **'Array'** field types in the record to define the variability.
 

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

Let's assume a input record as below:

```
  { "col1" : "name", "col2" : 2.8f, "col3" : 1, "other" : "other" }
```

Specifying an expression below 

```
  col1 + ":" + col2 + ":" + col3
```

Would generate row key as

```
  name:2.8:1
```

## Configuration

## Additional Notes


