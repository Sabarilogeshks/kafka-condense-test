require("dotenv").config();
const { Kafka } = require("kafkajs");
const axios = require("axios");
const express = require("express");
const bodyParser = require("body-parser");

// Logging helper
const log = (type, msg, obj = null) => {
  const ts = new Date().toISOString();
  if (obj) {
    console.log(`[${ts}] [${type}] ${msg}`, obj);
  } else {
    console.log(`[${ts}] [${type}] ${msg}`);
  }
};

const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:9092";
const SOURCE_TOPIC = process.env.SOURCE_TOPIC || "notification-topic";
const EMAIL_API_ENDPOINT =
  "https://mc3snfg-sfh7x8jmy5gw1rdk4zbq.rest.marketingcloudapis.com/messaging/v1/email/messages";
const SMS_API_ENDPOINT =
  "https://mc3snfg-sfh7x8jmy5gw1rdk4zbq.rest.marketingcloudapis.com/sms/v1/messageContact/NzcwMzo3ODow/send";
const TOKEN_ENDPOINT =
  "https://mc3snfg-sfh7x8jmy5gw1rdk4zbq.auth.marketingcloudapis.com/v2/token";

const MAX_RETRIES = 3;
const RETRY_DELAY = 1000;
const DEFAULT_EMAIL = "schetan@royalenfield.com";

let cachedToken = null;
let tokenExpiry = null;

const generateOTP = () => Math.floor(100000 + Math.random() * 900000).toString();

const getAccessToken = async (retryCount = 0) => {
  try {
    if (cachedToken && tokenExpiry && Date.now() < tokenExpiry) {
      return cachedToken;
    }

    if (!process.env.CLIENT_ID || !process.env.CLIENT_SECRET) {
      throw new Error("Missing CLIENT_ID or CLIENT_SECRET");
    }

    const response = await axios.post(
      TOKEN_ENDPOINT,
      {
        grant_type: "client_credentials",
        client_id: process.env.CLIENT_ID,
        client_secret: process.env.CLIENT_SECRET,
      },
      { headers: { "Content-Type": "application/json" } }
    );

    cachedToken = response.data.access_token;
    tokenExpiry = Date.now() + (response.data.expires_in - 1200) * 1000;

    log("INFO", "New SFMC access token generated");
    return cachedToken;
  } catch (error) {
    log("ERROR", "Token fetch failed", error.toString());
    if (retryCount < MAX_RETRIES) {
      await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY));
      return getAccessToken(retryCount + 1);
    }
    throw error;
  }
};

// Kafka setup
const kafka = new Kafka({ clientId: "notification-processor", brokers: [KAFKA_BROKER] });
const consumer = kafka.consumer({ groupId: "notification-group" });

const sendNotification = async (payload, isEmail = true) => {
  try {
    const token = await getAccessToken();
    const endpoint = isEmail ? EMAIL_API_ENDPOINT : SMS_API_ENDPOINT;

    log("INFO", `${isEmail ? "Email" : "SMS"} sending...`, payload);

    const response = await axios.post(endpoint, payload, {
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
    });

    log("SUCCESS", `${isEmail ? "Email" : "SMS"} sent successfully`, response.data);
    return response.data;
  } catch (err) {
    log("ERROR", "Notification sending failed", err.response?.data || err.message);
    throw err;
  }
};

const processMessage = async (message) => {
  try {
    const jsonString = message.value.toString();
    log("INFO", `Kafka Message Received => ${jsonString}`);

    const data = JSON.parse(jsonString);
    const { alertId, contact_number, email, name, date, time, reg_no } = data;

    let payload;
    let isEmail = true;

    switch (alertId) {
      case "001":
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
        break;

      case "003":
        if (!contact_number) throw new Error("Missing contact number");

        const otp = generateOTP();
        log("INFO", `Generated OTP: ${otp} → ${contact_number}`);

        payload = {
          Subscribers: [
            {
              MobileNumber: contact_number,
              SubscriberKey: contact_number,
              Attributes: { OTPNUMBER: otp },
            },
          ],
          Subscribe: "true",
          Resubscribe: "true",
          keyword: "RE",
          Override: "false",
        };
        isEmail = false;
        break;

      default:
        throw new Error(`Unsupported alertId: ${alertId}`);
    }

    await sendNotification(payload, isEmail);
  } catch (error) {
    log("ERROR", "Processing Kafka message failed", error.toString());
  }
};

// Kafka Listener
const startKafkaProcessing = async () => {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: SOURCE_TOPIC, fromBeginning: false });
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        log("INFO", `Processing message from ${topic}:${partition}`);
        await processMessage(message);
      },
    });
    log("INFO", "Kafka Listening Started 🎯");
  } catch (error) {
    log("ERROR", "Kafka processing failed", error.toString());
  }
};

// Express API
const app = express();
app.use(bodyParser.json());

app.post("/notify-service", async (req, res) => {
  log("INFO", "API Request Received", req.body);

  const { contact } = req.body;

  if (!contact) {
    log("WARN", "Missing contact in API request");
    return res.status(400).json({ error: "contact is required" });
  }

  await consumer.emitMessage({
    value: JSON.stringify({ ...req.body, alertId: "001" }),
  });

  res.json({ message: "Notification request received successfully" });
});

// Server Start
const PORT = process.env.PORT || 3008;
app.listen(PORT, () => log("SUCCESS", `API Running on port ${PORT}`));
startKafkaProcessing();
