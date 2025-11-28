require("dotenv").config();
const { Kafka } = require("kafkajs");
const axios = require("axios");
const express = require("express");
const bodyParser = require("body-parser");

// ───────────────────────── LOG WRAPPER ────────────────────────────
const logToKafka = async (level, message, extra = {}) => {
  try {
    const event = {
      timestamp: new Date().toISOString(),
      level,
      message,
      ...extra
    };

    await producer.send({
      topic: process.env.LOG_TOPIC,
      messages: [{ value: JSON.stringify(event) }],
    });

    console.log(`[${level}] ${message}`, extra);
  } catch (err) {
    console.error("Logging Failure:", err.message);
  }
};

// ───────────────────────── CONFIGS ────────────────────────────────
const KAFKA_BROKER = process.env.KAFKA_BROKER;
const SOURCE_TOPIC = process.env.SOURCE_TOPIC;
const LOG_TOPIC = process.env.LOG_TOPIC;

const EMAIL_API_ENDPOINT = process.env.SFMC_EMAIL_URL;
const SMS_API_ENDPOINT = process.env.SFMC_SMS_URL;
const TOKEN_ENDPOINT = process.env.SFMC_TOKEN_URL;

const CLIENT_ID = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET;
const DEFAULT_EMAIL = process.env.DEFAULT_EMAIL || "kaliappan_ext@royalenfield.com";

let cachedToken = null;
let tokenExpiry = null;

// ───────────────────────── TOKEN LOGIC ────────────────────────────
const getAccessToken = async () => {
  if (cachedToken && tokenExpiry > Date.now()) return cachedToken;

  const tokenResponse = await axios.post(
    TOKEN_ENDPOINT,
    { grant_type: "client_credentials", client_id: CLIENT_ID, client_secret: CLIENT_SECRET },
    { headers: { "Content-Type": "application/json" } }
  );

  cachedToken = tokenResponse.data.access_token;
  tokenExpiry = Date.now() + (tokenResponse.data.expires_in - 120) * 1000;

  await logToKafka("INFO", "New SFMC token generated");
  return cachedToken;
};

// ───────────────────────── API SEND FUNCTION ──────────────────────
const sendNotification = async (payload, isEmail = true) => {
  const token = await getAccessToken();
  const endpoint = isEmail ? EMAIL_API_ENDPOINT : SMS_API_ENDPOINT;

  await logToKafka("INFO", `Sending ${isEmail ? "Email" : "SMS"}`, payload);

  const response = await axios.post(endpoint, payload, {
    headers: { Authorization: `Bearer ${token}` },
  });

  await logToKafka("SUCCESS", `${isEmail ? "Email" : "SMS"} delivered`, response.data);
  return response.data;
};

// ───────────────────────── KAFKA SETUP ───────────────────────────
const kafka = new Kafka({ clientId: "notification-service", brokers: [KAFKA_BROKER] });
const consumer = kafka.consumer({ groupId: "notification-group" });
const producer = kafka.producer();

// ───────────────────────── MESSAGE HANDLER ────────────────────────
const processMessage = async (data) => {
  try {
    await logToKafka("INFO", "Message received", data);

    let payload;
    let isEmail = true;

    if (data.alertId === "001") {
      payload = {
        definitionKey: "Worry_free_service",
        recipients: [{
          contactKey: data.contact_number,
          to: data.email,
          attributes: {
            SubscriberKey: data.contact_number,
            EmailAddress: data.email,
            CUSTOMERNAME: data.name,
            DEALERNAME: "RE INDIA",
            DATE: data.date,
            TIME: data.time,
          },
        }],
      };
    } else if (data.alertId === "003") {
      payload = {
        Subscribers: [{
          MobileNumber: data.contact_number,
          SubscriberKey: data.contact_number,
          Attributes: { OTPNUMBER: Math.floor(100000 + Math.random() * 900000).toString() },
        }],
        Subscribe: true,
        Resubscribe: true,
        keyword: "RE",
      };
      isEmail = false;
    } else {
      throw new Error("Invalid alertId");
    }

    await sendNotification(payload, isEmail);
  } catch (err) {
    await logToKafka("ERROR", "Notification failed", { error: err.message, data });
  }
};

// ───────────────────────── START CONSUMER ─────────────────────────
const startKafkaProcessing = async () => {
  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({ topic: SOURCE_TOPIC });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value.toString());
      await processMessage(data);
    },
  });

  await logToKafka("INFO", "Kafka listener running");
};

// ───────────────────────── EXPRESS API ────────────────────────────
const app = express();
app.use(bodyParser.json());

app.post("/notify-service", async (req, res) => {
  await logToKafka("INFO", "HTTP API Request Received", req.body);

  try {
    const response = await processMessage(req.body);
    res.json({ status: "SUCCESS", message: "Notification processed" });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get("/health", (req, res) => res.json({ status: "UP" }));
app.get("/version", (req, res) => res.json({ version: "v1.0.0" }));

// ───────────────────────── START SERVER ───────────────────────────
const PORT = process.env.PORT || 3008;
app.listen(PORT, () => console.log(`API running on ${PORT}`));

startKafkaProcessing().catch(console.error);
