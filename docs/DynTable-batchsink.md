## Dynamic or Variable Schema Table

This plugin supports writing dynamic schemas record to CDAP Dataset Table. In addition to writing dynamic schema tables, it also support regular structured records to be written to Tables.

Schema design determines the way an application handles its data. With traditional relational databases,
you must define your schema before you can add any data. This inflexibility means you can’t change your schema as
your data, application requirements or business evolves. In today’s world hyper-competitive, global business
environment, this can hamper your efforts to innovate and stay on top of the competition.

NoSQL databases arose to address this limitation by allowing you to insert data without a predefined schema.
Because of this, you can easily make changes to an application without interruption. The result is more reliable
code integration, faster development, and database administration time.

## Plugin Configuration

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Table Name** | **Y** | N/A | Specifies the name of the Dataset to write. If the Dataset doesn't exist, then it will be created. If a macro is not specified, then it's created during deployment, else it's created at runtime. |
| **Row Key** | **Y** | N/A | Specifies how unique key needs to be generated. This can be an expression. |

## Usage Notes

In cases, where one does not know in advance the columns the table would have, this plugin is very useful. It uses pre-defined structure to provide dynamic schema capabilities.
 
### Defining Dynamic Schema

In order to define dynamic schemas, one has to use **'Map'** or **'Array'** field types in the record to define the variability of fields to be written to Table dataset. 

#### Map

When using map, the key field is considered to be the column name and the value field is considered as the value for the column. Both are of type string. So, when value is written, it's written as string. So let's say you have a map as follows

```
  { "ssn" : "000-00-0000", "dynamic" : { "name" : "joltie", "address" : "mars ave", "zipcode" : "3423424", "salary":"10.9" } }
```

When it's written to then table, 'ssn', 'name', 'address', 'zipcode' and 'salary' as used as qualifier (column names) and their values will be respectively '000-00-0000', 'joltie', 'mars ave', '3423424' and '10.9'.

#### Array

This plugin supports writing dynamic schema using array of records. There are two types of records supported

  1. Record that has two fields, namely 'field' and 'value' and 
  2. Record that has three field, namely 'field', 'value' and 'type'. 
  
In order for the dynamic schema to work, the field names within the record of an array have to be fixed. They can only be 
'field', 'value' or 'type'. 

When the record is specified as type (1), then both are of type string and they would be written as string. Let's assume a record:

```
 { 
   "ssn" : "000-00-0000", 
   "dynamic" : [ 
     { "field" : "name", "value" : "joltie" },
     { "field" : "address", "value" : "mars ave" },
     { "field" : "zipcode", "value" : "3423424},
     { "field" : "salary", "value" : "10.9"  
   ]
 }   
```
would generate the same result as the 'Map' based dynamic schema would generate.

> Note that value is written as string. 

But, when the record is specified as type (2), the major difference is that the values are written based on the 'type' specified. So, let's look at an example: 

```
 { 
   "ssn" : "000-00-0000", 
   "dynamic" : [ 
     { "field" : "name", "value" : "joltie", "type" : "string" },
     { "field" : "address", "value" : "mars ave", "type" : "string"},
     { "field" : "zipcode", "value" : "3423424", "type" : "long"},
     { "field" : "salary", "value" : "10.9", "type" : "double" }
   ]
 }   
```

In this case, the cell values are serialized as the type they are specified in the 'type' field.

Following are the types supported:

* string,
* int,
* long,
* short,
* double,
* float, 
* boolean

### Row Key Expression

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

### Column Family Expression

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

## Additional Notes

Upon creating the CDAP table using this plugin, you can either use Apache Phoenix or Hive with HBase Storage handler to create a view over different schemas that are available in the same table. 

