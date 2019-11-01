/* Copyright (c) 2018, 2019, Oracle and/or its affiliates. All rights reserved. */

/******************************************************************************
 *
 * You may not use the identified files except in compliance with the Apache
 * License, Version 2.0 (the "License.")
 *
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * NAME
 *   example.js
 *
 * DESCRIPTION
 *   A basic node-oracledb example using Node.js 8's async/await syntax.
 *
 *   For a connection pool example see connectionpool.js
 *   For a ResultSet example see resultset2.js
 *   For a query stream example see selectstream.js
 *   For a Promise example see promises.js
 *   For a callback example see select1.js
 *
 *   This example requires node-oracledb 2.2 or later.
 *
 *****************************************************************************/

// Using a fixed Oracle time zone helps avoid machine and deployment differences
process.env.ORA_SDTZ = "UTC";

var oracledb = require("oracledb");
var dbConfig = require("./dbconfig.js");
var build = require("./query.js");
var { dynamicValues } = require("./dynamicValues.js");

function run() {
  oracledb
    .getConnection({
      user: dbConfig.user,
      password: dbConfig.password,
      connectionString: dbConfig.connectString
    })
    .then(c => {
      console.time("Tiempo: ");

      var queries = dynamicValues.map(valuesQuery =>
        c.execute(build.query(valuesQuery))
      );

      Promise.all(queries).then(function(values) {
        console.log(values.length);
        console.timeEnd("Tiempo: ");
      });
    });
}

run();
