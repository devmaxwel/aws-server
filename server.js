const express = require("express");
const bodyparser = require("body-parser");
const cassandra = require("cassandra-driver");
const cors = require("cors");
const fs = require("fs");
const dotenv = require("dotenv");
const bcrypt = require("bcrypt");
dotenv.config();

let auth = new cassandra.auth.PlainTextAuthProvider(
  process.env.username,
  process.env.password
);

const sslOptions1 = {
  ca: [fs.readFileSync("./sf-class2-root.crt", "utf-8")],
  host: "cassandra.us-east-2.amazonaws.com",
  rejectUnauthorized: true,
};
const client = new cassandra.Client({
  contactPoints: ["cassandra.us-east-2.amazonaws.com"],
  localDataCenter: "us-east-2",
  authProvider: auth,
  keyspace: "business_keyspace",
  sslOptions: sslOptions1,
  protocolOptions: { port: 9142 },
});
client.connect(function (err, result) {
  if (err) {
    return console.log("Cassandra connection error: " + err.message);
  }
  console.log("Cassandra connected");
});

const app = express();
app.use(cors());
app.use(bodyparser.json());
app.use(bodyparser.urlencoded({ extended: true }));

app.get("/", (req, res) => {
  res.send("Health Check Okay");
});

// Fetch Employees

app.get("/api/v1/employees", (req, res) => {
  const query = "SELECT * FROM business_keyspace.employees";
  client.execute(query, (err, result) => {
    if (err) {
      console.log(err);
      return res.status(404).json({ msg: err });
    }
    res.json(result.rows);
  });
});

// Fetch Suppliers

app.get("/api/v1/suppliers", (req, res) => {
  const query = "SELECT * FROM business_keyspace.suppliers";
  client.execute(query, (err, result) => {
    if (err) {
      console.log(err);
      return res.status(404).json({ msg: err });
    }
    res.json(result.rows);
  });
});

// Fetch Products

app.get("/api/v1/products", (req, res) => {
  const query = "SELECT * FROM business_keyspace.retail_items";
  client.execute(query, (err, result) => {
    if (err) {
      console.log(err);
      return res.status(404).json({ msg: err });
    }
    res.json(result.rows);
  });
});

app.get("/api/v1/customers", (req, res) => {
  const query = "SELECT * FROM business_keyspace.customers";
  client.execute(query, (err, result) => {
    if (err) {
      console.log(err);
      return res.status(404).json({ msg: err });
    }
    res.json(result.rows);
  });
});

app.post("/api/v1/create-login", async (req, res) => {
  const { email, password } = req.body;

  // Check if email already exists in database
  const checkQuery = `SELECT email FROM business_keyspace.admin_login WHERE email = '${email}'`;
  client.execute(
    checkQuery,
    [],
    { consistency: cassandra.types.consistencies.localQuorum },
    async (err, result) => {
      if (err) {
        return res.status(404).json({ msg: err });
      }
      if (result.rows.length > 0) {
        return res.status(409).json({ msg: "Email already exists" });
      }

      // Email does not exist, insert new record
      let salt = await bcrypt.genSalt(12);
      let hashedPassword = await bcrypt.hash(password, salt);

      const insertQuery = `INSERT INTO business_keyspace.admin_login(email, password) VALUES('${email}', '${hashedPassword}')`;

      client.execute(
        insertQuery,
        [],
        { consistency: cassandra.types.consistencies.localQuorum },
        (err, result) => {
          if (err) {
            return res.status(404).json({ msg: err });
          }
          res.json(result.rows);
        }
      );
    }
  );
});

app.post("/api/v1/login", async (req, res) => {
  const { email, password } = req.body;
  const query = `SELECT * FROM business_keyspace.admin_login WHERE email = '${email}'`;
  client.execute(
    query,
    [],
    { consistency: cassandra.types.consistencies.localQuorum },
    async (err, result) => {
      if (err) {
        return res.status(404).json({ msg: err });
      }
      const hashedPassword = result.rows[0].password;
      const isMatch = await bcrypt.compare(password, hashedPassword);
      if (!isMatch) {
        return res.status(404).json({ msg: "Invalid Credentials" });
      }
      const user = {
        id: result.rows[0].id,
        email: result.rows[0].email,
        // add any other fields you want to return
      };
      res.json({
        user: user,
        msg: "Login Successful",
      });
    }
  );
});

const PORT = 4500;
app.listen(PORT, async () => {
  console.log(`Server is running on port ${PORT}`);
});
