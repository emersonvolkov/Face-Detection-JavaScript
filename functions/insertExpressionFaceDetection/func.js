const fdk = require("@fnproject/fdk");
const oracledb = require("oracledb");
const dateFormat = require("dateformat");

oracledb.outFormat = oracledb.OBJECT;
oracledb.fetchAsString = [oracledb.CLOB];

let pool;

function getStringDate() {
  const capturedAtDate = new Date();
  const year = capturedAtDate.getFullYear();
  const monthRaw = capturedAtDate.getMonth();
  const month =
    monthRaw < 10
      ? String.prototype.concat("0", monthRaw.toString())
      : monthRaw.toString();
  const dayRaw = capturedAtDate.getDate();
  const day =
    dayRaw < 10
      ? String.prototype.concat("0", dayRaw.toString())
      : dayRaw.toString();
  const hoursRaw = capturedAtDate.getHours();
  const hours =
    hoursRaw < 10
      ? String.prototype.concat("0", hoursRaw.toString())
      : hoursRaw.toString();
  const minutesRaw = capturedAtDate.getMinutes();
  const minutes =
    minutesRaw < 10
      ? String.prototype.concat("0", minutesRaw.toString())
      : minutesRaw.toString();
  const secondsRaw = capturedAtDate.getSeconds();
  const seconds =
    secondsRaw < 10
      ? String.prototype.concat("0", secondsRaw.toString())
      : secondsRaw.toString();
  const millisecondsRaw = capturedAtDate.getMilliseconds();
  const milliseconds =
    millisecondsRaw < 10
      ? String.prototype.concat("00", millisecondsRaw.toString())
      : millisecondsRaw < 100
      ? String.prototype.concat("0", millisecondsRaw.toString())
      : millisecondsRaw.toString();

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}:${milliseconds}`;
}

fdk.handle(async function(input) {
  if (!pool) {
    pool = await oracledb.createPool({
      user: process.env.DB_USER || "admin",
      password: process.env.DB_PASSWORD || "Welcome12345#",
      connectString: process.env.CONNECT_STRING || "adwfaced_medium"
    });
  }

  const connection = await pool.getConnection();
  const insert = await connection.execute(
    "insert into EXPRESSIONS (CAPTURED_AT,EXPRESSION,PROBABILITY) values (:capturedAt, :expression, :probability )",
    {
      capturedAt: getStringDate(),
      expression: "Feliz",
      probability: 0.5234523
    },
    { autoCommit: true }
  );
  await connection.close();
  return { insert: insert, complete: true };
}, {});
