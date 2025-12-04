require("dotenv").config();
const { Kafka } = require("kafkajs");
const axios = require("axios");
const express = require("express");

// Optional (for OTP verification later)
// const Redis = require("ioredis");
// const redis = new Redis(process.env.REDIS_URL);

// ─────────────────────────── CONFIG ──────────────────────────────
const {
  KAFKA_BROKER,
  SOURCE_TOPIC,
  LOG_TOPIC,
  SFMC_EMAIL_URL,
  SFMC_SMS_URL,
  SFMC_TOKEN_URL,
  CLIENT_ID,
  CLIENT_SECRET,
  PORT = 3008
} = process.env;

let cachedToken = null;
let tokenExpiry = null;

// ───────────────────────── LOG FUNCTION ──────────────────────────
const logToKafka = async (level, message, meta = {}) => {
  const event = { timestamp: new Date(), level, message, ...meta };
  console.log(`[${level}]`, message, meta);

  try {
    await producer.send({
      topic: LOG_TOPIC,
      messages: [{ value: JSON.stringify(event) }]
    });
  } catch (err) {
    console.error("⚠ Failed to push logs to Kafka:", err.message);
  }
};

// ───────────────────────── TOKEN MANAGEMENT ──────────────────────
async function getAccessToken() {
  if (cachedToken && tokenExpiry > Date.now()) return cachedToken;

  try {
    const res = await axios.post(
      SFMC_TOKEN_URL,
      { grant_type: "client_credentials", client_id: CLIENT_ID, client_secret: CLIENT_SECRET },
      { headers: { "Content-Type": "application/json" } }
    );

    cachedToken = res.data.access_token;
    tokenExpiry = Date.now() + (res.data.expires_in - 180) * 1000; // Refresh 3 mins before expiry

    await logToKafka("INFO", "🔐 SFMC token refreshed");
    return cachedToken;

  } catch (err) {
    await logToKafka("ERROR", "Token generation failed", { err: err.message });
    throw new Error("SFMC Token fetch failed");
  }
}

// ───────────────────────── NOTIFICATION SENDER ───────────────────
async function sendNotification(payload, isEmail = true, retry = 2) {
  const token = await getAccessToken();
  const url = isEmail ? SFMC_EMAIL_URL : SFMC_SMS_URL;

  try {
    const res = await axios.post(url, payload, {
      headers: { Authorization: `Bearer ${token}` }
    });

    await logToKafka("SUCCESS", `${isEmail ? "Email" : "SMS"} sent`, res.data);
    return res.data;

  } catch (err) {
    await logToKafka("ERROR", "SFMC Send Failed", { err: err.message, retry });

    // Retry with new token
    if (retry > 0) {
      cachedToken = null;
      return sendNotification(payload, isEmail, retry - 1);
    }

    throw err;
  }
}

// ───────────────────────── MESSAGE HANDLER ───────────────────────
function buildPayload(data) {
  const otp = Math.floor(100000 + Math.random() * 900000).toString();

  switch (data.alertId) {
    case "001": // EMAIL TEMPLATE
      return {
        payload: {
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
              TIME: data.time
            }
          }]
        },
        isEmail: true
      };

    case "003": // OTP SMS TEMPLATE
      return {
        payload: {
          Subscribers: [{
            MobileNumber: data.contact_number,
            SubscriberKey: data.contact_number,
            Attributes: { OTPNUMBER: otp }
          }],
          Subscribe: true,
          Resubscribe: true,
          keyword: "RE"
        },
        isEmail: false,
        otp
      };

    default:
      throw new Error("Unsupported alertId");
  }
}

async function processMessage(data) {
  try {
    const { payload, isEmail, otp } = buildPayload(data);

    await logToKafka("INFO", "Processing notification", data);
    const result = await sendNotification(payload, isEmail);

    // OTP save example
    // if (!isEmail) await redis.setex(`OTP:${data.contact_number}`, 300, otp);

    return result;

  } catch (err) {
    await logToKafka("ERROR", "Processing failed", { data, error: err.message });
    throw err;
  }
}

// ───────────────────────── KAFKA SETUP ───────────────────────────
const kafka = new Kafka({ clientId: "notification-service", brokers: [KAFKA_BROKER] });
const consumer = kafka.consumer({ groupId: "notif-consumers" });
const producer = kafka.producer();

async function startKafka() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: SOURCE_TOPIC });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const data = JSON.parse(message.value.toString());
        await processMessage(data);
      } catch (e) {
        await logToKafka("ERROR", "Invalid Kafka message", { raw: message.value.toString() });
      }
    }
  });

  await logToKafka("INFO", "Kafka listener running...");
}

// ───────────────────────── EXPRESS API ───────────────────────────
const app = express();
app.use(express.json());

app.post("/notify", async (req, res) => {
  try {
    await processMessage(req.body);
    res.json({ status: "SUCCESS" });
  } catch (err) {
    res.status(500).json({ status: "FAILED", error: err.message });
  }
});

app.get("/health", (_, res) => res.json({ status: "UP" }));

app.listen(PORT, () => console.log(`🚀 API running on port ${PORT}`));
startKafka();

// Graceful Shutdown
process.on("SIGTERM", async () => {
  console.log("Shutting down...");
  await consumer.disconnect();
  await producer.disconnect();
  process.exit(0);
});
