# Dynamic Schema Sinks

<a href="https://cdap-users.herokuapp.com/"><img alt="Join CDAP community" src="https://cdap-users.herokuapp.com/badge.svg?t=1"/></a> [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

This repository contains HBase and CDAP Table Dataset sink with support for writing dynamic schema records. 

* [HBase Sink](docs/DynTable-batchsink.md)
* [CDAP Table Dataset Sink](docs/DynHBase-batchsink.md)

## Defining Dynamic Schema

In order to define dynamic schemas, one has to use **'Map'** or **'Array'** field types in the record to define the variability of fields to be written to Table dataset or HBase Table. 

### Map

When using map, the key field is considered to be the column name and the value field is considered as the value for the column. Both are of type string. So, when value is written, it's written as string. So let's say you have a map as follows

```
  { "ssn" : "000-00-0000", "dynamic" : { "name" : "joltie", "address" : "mars ave", "zipcode" : "3423424", "salary":"10.9" } }
```

When it's written to then table, 'ssn', 'name', 'address', 'zipcode' and 'salary' as used as qualifier (column names) and their values will be respectively '000-00-0000', 'joltie', 'mars ave', '3423424' and '10.9'.

### Array

This plugin supports writing dynamic schema using array of records. There are two types of records supported

  1. Record that has two fields, namely **'field'** and **'value'** and 
  2. Record that has three field, namely **'field'**, **'value'** and **'type'**. 
  
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

## Additional Notes

Upon creating the CDAP table or HBase using plugins here, one can either create Apache Phoenix or Hive views over those tables.

## Build

To build your plugins:

    mvn clean package -DskipTests

The build will create a .jar and .json file under the ``target`` directory for wrangler-transform and .jar for wrangler-service application. These files can be used to deploy your plugins and wrangler backend.


## Deployment
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/hbase-sink-dynamic-schema-<version>.jar> config-file <target/hbase-sink-dynamic-schema-<version>.json>

For example, if your artifact is named 'hbase-sink-dynamic-schema-<version>':

    > load artifact target/hbase-sink-dynamic-schema-<version>.jar config-file target/hbase-sink-dynamic-schema-<version>.json

## Mailing Lists

CDAP User Group and Development Discussions:

- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.

## IRC Channel

CDAP IRC Channel: #cdap on irc.freenode.net


## License and Trademarks

Copyright © 2016-2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
