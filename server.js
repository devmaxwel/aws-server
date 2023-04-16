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



const PORT = 4500;
app.listen(PORT, async () => {
  console.log(`Server is running on port ${PORT}`);
});
