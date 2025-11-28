require("dotenv").config();
const { Kafka } = require("kafkajs");
const axios = require("axios");
const express = require("express");
const bodyParser = require("body-parser");

// ---------- Logging Setup ----------
const log = (...msg) => console.log(new Date().toISOString(), ...msg);

// Kafka logging topic
const LOG_TOPIC = "notification-logs";

// Kafka setup
const kafka = new Kafka({
  clientId: "notification-service",
  brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "notification-group" });
const logProducer = kafka.producer();
const otpGenerator = () => Math.floor(100000 + Math.random() * 900000).toString();

// Initialize log producer
(async () => {
  try {
    await logProducer.connect();
    log("Kafka Log Producer connected");
  } catch (err) {
    log("❌ Kafka Log Producer connection failed:", err.message);
  }
})();

// Logging function → Kafka + console fallback
async function logToKafka(level, message, meta = {}) {
  const event = {
    timestamp: new Date().toISOString(),
    level,
    message,
    ...meta,
  };

  try {
    await logProducer.send({
      topic: LOG_TOPIC,
      messages: [{ value: JSON.stringify(event) }],
    });
  } catch (err) {
    log("⚠ Failed to push log to Kafka:", err.message);
    log("Fallback Log:", event);
  }
}

// ---------- Salesforce Marketing Cloud Credentials ----------
const TOKEN_ENDPOINT = "https://mc3snfg-sfh7x8jmy5gw1rdk4zbq.auth.marketingcloudapis.com/v2/token";
const EMAIL_API_ENDPOINT = "https://mc3snfg-sfh7x8jmy5gw1rdk4zbq.rest.marketingcloudapis.com/messaging/v1/email/messages";
const SMS_API_ENDPOINT = "https://mc3snfg-sfh7x8jmy5gw1rdk4zbq.rest.marketingcloudapis.com/sms/v1/messageContact/NzcwMzo3ODow/send";

let cachedToken = null;
let tokenExpiry = null;

// Token Handler
async function getAccessToken() {
  if (cachedToken && tokenExpiry && Date.now() < tokenExpiry) return cachedToken;

  const body = {
    grant_type: "client_credentials",
    client_id: process.env.CLIENT_ID,
    client_secret: process.env.CLIENT_SECRET,
  };

  try {
    const response = await axios.post(TOKEN_ENDPOINT, body);
    cachedToken = response.data.access_token;
    tokenExpiry = Date.now() + (response.data.expires_in - 1200) * 1000;

    await logToKafka("INFO", "SFMC access token refreshed");
    return cachedToken;
  } catch (err) {
    await logToKafka("ERROR", "Failed to fetch SFMC token", { error: err.message });
    throw err;
  }
}

// Push Notification Sender
async function sendNotification(payload, isEmail = true) {
  const token = await getAccessToken();
  const endpoint = isEmail ? EMAIL_API_ENDPOINT : SMS_API_ENDPOINT;

  await logToKafka("INFO", "Sending notification request to SFMC", { endpoint, payload });

  const response = await axios.post(endpoint, payload, {
    headers: { Authorization: `Bearer ${token}` },
  });

  await logToKafka("INFO", "SFMC response received", response.data);
  return response.data;
}

// Kafka → Handles async messages
async function processKafkaMessage(message) {
  try {
    const data = JSON.parse(message.value.toString());
    const { alertId, contact_number, email, name, date, time, reg_no } = data;

    await logToKafka("INFO", "Kafka Notification trigger received", data);

    let payload, isEmail = true;

    if (alertId === "001") {
      payload = {
        definitionKey: "Worry_free_service",
        recipients: [
          {
            contactKey: contact_number,
            to: email,
            attributes: {
              SubscriberKey: contact_number,
              EmailAddress: email,
              CUSTOMERNAME: name,
              DEALERNAME: "RE INDIA",
              DATE: date,
              TIME: time,
            },
          },
        ],
      };
    } else if (alertId === "003") {
      const otp = otpGenerator();
      await logToKafka("INFO", "Generated OTP", { contact_number, otp });

      payload = {
        Subscribers: [{ MobileNumber: contact_number, SubscriberKey: contact_number, Attributes: { OTPNUMBER: otp }}],
        Subscribe: "true",
        Resubscribe: "true",
        keyword: "RE",
        Override: "false",
      };
      isEmail = false;
    } else {
      throw new Error(`Unsupported alertId: ${alertId}`);
    }

    await sendNotification(payload, isEmail);
    await logToKafka("SUCCESS", "Notification delivered successfully", data);
  } catch (err) {
    await logToKafka("ERROR", "Processing Kafka message failed", { error: err.message });
  }
}

// Kafka Listener Service
(async () => {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: process.env.SOURCE_TOPIC || "alert_input_test", fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ message }) => await processKafkaMessage(message),
    });

    await logToKafka("INFO", "Kafka listener running");
  } catch (err) {
    await logToKafka("ERROR", "Kafka listener startup failed", { error: err.message });
  }
})();

// ---------- REST API Layer (direct-trigger testing) ----------
const app = express();
app.use(bodyParser.json());

app.post("/notify-service", async (req, res) => {
  const body = req.body;
  await logToKafka("INFO", "API Notification Trigger Received", body);

  try {
    await processKafkaMessage({ value: Buffer.from(JSON.stringify(body)) });
    res.json({ status: "SUCCESS", message: "Notification pushed" });
  } catch (err) {
    await logToKafka("ERROR", "API trigger failed", { error: err.message });
    res.status(500).json({ status: "FAILED", error: err.message });
  }
});

// Health Checks
app.get("/health", (_, res) => res.json({ status: "OK", time: new Date().toISOString() }));
app.get("/version", (_, res) => res.json({ version: "1.0.0" }));

const PORT = process.env.PORT || 3008;
app.listen(PORT, () => log("Server listening on port", PORT));
