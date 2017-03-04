# HBase Sink with support for dynamic schema

HBase sink with support for dynamic schema

## Usage Notes

## Configuration

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
