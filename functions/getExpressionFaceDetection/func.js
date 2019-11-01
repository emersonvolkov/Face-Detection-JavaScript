const fdk = require("@fnproject/fdk");
const oracledb = require("oracledb");
oracledb.outFormat = oracledb.OBJECT;
// oracledb.fetchAsString = [oracledb.CLOB];

let pool;

fdk.handle(async function(input) {
  if (!pool) {
    pool = await oracledb.createPool({
      user: process.env.DB_USER || "admin",
      password: process.env.DB_PASSWORD || "Welcome12345#",
      connectString: process.env.CONNECT_STRING || "adwfaced_medium"
    });
  }
  const connection = await pool.getConnection();
  const records = await connection.execute("select * from EXPRESSIONS");
  const result = records.rows.map(row => {
    return {
      id: row.ID,
      capturedAt: row.CAPTURED_AT,
      expression: row.EXPRESSION,
      probability: row.PROBABILITY
    };
  });
  await connection.close();
  return result;
}, {});
