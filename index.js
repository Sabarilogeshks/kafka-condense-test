require("dotenv").config();
const { Kafka } = require("kafkajs");
const axios = require("axios");
const express = require("express");
const bodyParser = require("body-parser");

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

const PORT = process.env.PORT || 3008;

let cachedToken = null;
let tokenExpiry = null;

// ───────────────────────── KAFKA SETUP ───────────────────────────
const kafka = new Kafka({ clientId: "notification-service", brokers: [KAFKA_BROKER] });
const consumer = kafka.consumer({ groupId: "notification-group" });
const producer = kafka.producer();

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
      topic: LOG_TOPIC,
      messages: [{ value: JSON.stringify(event) }],
    });

    console.log(`[${level}] ${message}`, extra);
  } catch (err) {
    console.error("Logging Failure:", err.message);
  }
};

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

// ───────────────────────── ALERT TEMPLATE CONFIG ──────────────────
// alertId = decides CONTENT
// Channel = decides where to send (SMS / Email / Both)
const ALERT_TEMPLATES = {
  /**
   * alertId: "001" – Live location share
   * Example Params:
   *  {
   *    "Channel":"SMS" | "Email" | "Both",
   *    "alertId":"001",
   *    "contact_number":"919655771145",
   *    "email":"xyz@abc.com",
   *    "Params": { "Name":"Sabari", "url":"https://..." }
   *  }
   */
  "001": {
    name: "Live location share",
    buildContent: (data) => {
      if (!data.Params || !data.Params.Name || !data.Params.url) {
        throw new Error("Params.Name and Params.url are required for alertId 001");
      }

      const smsText = `${data.Params.Name} has shared live location with you. Click the link to track the location ${data.Params.url}`;

      return {
        sms: {
          keyword: "RELOC",
          attributes: {
            MESSAGE_TEXT: smsText // Use %%MESSAGE_TEXT%% in SFMC SMS template
          }
        },
        email: {
          definitionKey: "LIVE_LOCATION_EMAIL_TEMPLATE", // configure in SFMC
          attributes: {
            NAME: data.Params.Name,
            URL: data.Params.url
          }
        }
      };
    }
  },

  /**
   * alertId: "002" – OTP content
   * Example:
   *  {
   *    "Channel":"SMS" | "Email" | "Both",
   *    "alertId":"002",
   *    "contact_number":"919655771145",
   *    "email":"xyz@abc.com"
   *  }
   */
  "002": {
    name: "OTP verification",
    buildContent: (data) => {
      const otp = Math.floor(100000 + Math.random() * 900000).toString();
      const smsText = `Your OTP is ${otp}. Please do not share it with anyone.`;

      return {
        sms: {
          keyword: "REOTP",
          attributes: {
            OTPNUMBER: otp // Use %%OTPNUMBER%% in SMS template
          }
        },
        email: {
          definitionKey: "OTP_EMAIL_TEMPLATE", // configure in SFMC
          attributes: {
            OTP: otp // Use %%OTP%% in email template
          }
        },
        meta: { otp }
      };
    }
  },

  // Example: you can easily add more alerts like old Worry_free_service here:
  // "003": {
  //   name: "Worry free service",
  //   buildContent: (data) => ({
  //     email: {
  //       definitionKey: "Worry_free_service",
  //       attributes: {
  //         SubscriberKey: data.contact_number,
  //         EmailAddress: data.email,
  //         CUSTOMERNAME: data.name,
  //         DEALERNAME: "RE INDIA",
  //         DATE: data.date,
  //         TIME: data.time
  //       }
  //     }
  //   })
  // }
};

// ───────────────────────── HELPERS FOR PAYLOADS ───────────────────
const buildSmsPayload = (contactNumber, smsConfig) => {
  if (!contactNumber) {
    throw new Error("contact_number is required for SMS channel");
  }

  return {
    Subscribers: [{
      MobileNumber: contactNumber,
      SubscriberKey: contactNumber,
      Attributes: smsConfig.attributes
    }],
    Subscribe: true,
    Resubscribe: true,
    keyword: smsConfig.keyword || "RE"
  };
};

const buildEmailPayload = (contactNumber, email, emailConfig) => {
  const toEmail = email || DEFAULT_EMAIL;

  if (!toEmail) {
    throw new Error("Email is required for Email channel");
  }

  return {
    definitionKey: emailConfig.definitionKey,
    recipients: [{
      contactKey: contactNumber || toEmail,
      to: toEmail,
      attributes: emailConfig.attributes
    }]
  };
};

// ───────────────────────── MESSAGE HANDLER ────────────────────────
const processMessage = async (data) => {
  try {
    await logToKafka("INFO", "Message received", data);

    const alertDef = ALERT_TEMPLATES[data.alertId];
    if (!alertDef) {
      throw new Error(`Invalid or unsupported alertId: ${data.alertId}`);
    }

    const channel = (data.Channel || "Email").toUpperCase(); // SMS | EMAIL | BOTH
    const { sms, email, meta } = alertDef.buildContent(data);

    let smsResult = null;
    let emailResult = null;

    // SMS
    if (channel === "SMS" || channel === "BOTH") {
      if (!sms) {
        throw new Error(`SMS content not configured for alertId: ${data.alertId}`);
      }
      const smsPayload = buildSmsPayload(data.contact_number, sms);
      smsResult = await sendNotification(smsPayload, false);
    }

    // EMAIL
    if (channel === "EMAIL" || channel === "BOTH") {
      if (!email) {
        throw new Error(`Email content not configured for alertId: ${data.alertId}`);
      }
      const emailPayload = buildEmailPayload(data.contact_number, data.email, email);
      emailResult = await sendNotification(emailPayload, true);
    }

    await logToKafka("SUCCESS", "Notification processed", {
      alertId: data.alertId,
      channel,
      smsSent: !!smsResult,
      emailSent: !!emailResult
    });

    return { smsResult, emailResult, meta };

  } catch (err) {
    await logToKafka("ERROR", "Notification failed", { error: err.message, data });
    throw err;
  }
};

// ───────────────────────── START CONSUMER ─────────────────────────
const startKafkaProcessing = async () => {
  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({ topic: SOURCE_TOPIC });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const data = JSON.parse(message.value.toString());
        await processMessage(data);
      } catch (err) {
        await logToKafka("ERROR", "Kafka message processing error", {
          error: err.message,
          raw: message.value.toString()
        });
      }
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
    const result = await processMessage(req.body);
    res.json({ status: "SUCCESS", message: "Notification processed", result });
  } catch (error) {
    res.status(500).json({ status: "FAILED", error: error.message });
  }
});

app.get("/health", (req, res) => res.json({ status: "UP" }));
app.get("/version", (req, res) => res.json({ version: "v1.0.0" }));

// ───────────────────────── START SERVER ───────────────────────────
app.listen(PORT, () => console.log(`API running on ${PORT}`));

startKafkaProcessing().catch((err) => {
  console.error("Kafka startup error:", err.message);
});
