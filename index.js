require("dotenv").config();
const { Kafka } = require("kafkajs");
const axios = require("axios");
const express = require("express");
const bodyParser = require("body-parser");

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
console.log("1");
let cachedToken = null;
let tokenExpiry = null;

const generateOTP = () => {
  return Math.floor(100000 + Math.random() * 900000).toString();
};

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
      {
        headers: { "Content-Type": "application/json" },
      }
    );

    if (!response.data?.access_token) {
      throw new Error("Invalid token response");
    }

    cachedToken = response.data.access_token;
    tokenExpiry = Date.now() + (response.data.expires_in - 1200) * 1000;
    return cachedToken;
  } catch (error) {
    if (retryCount < MAX_RETRIES) {
      await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY));
      return getAccessToken(retryCount + 1);
    }
    throw error;
  }
};

// Kafka setup
const kafka = new Kafka({
  clientId: "notification-processor",
  brokers: [KAFKA_BROKER],
});

const consumer = kafka.consumer({ groupId: "notification-group" });

const sendNotification = async (payload, isEmail = true) => {
  const token = await getAccessToken();
  const endpoint = isEmail ? EMAIL_API_ENDPOINT : SMS_API_ENDPOINT;

  const response = await axios.post(endpoint, payload, {
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
  });
  return response.data;
};

const processMessage = async (message) => {
  try {
    const data = JSON.parse(message.value.toString());
    const { alertId, contact_number, email, name, date, time, reg_no } = data;

    let payload;
    let isEmail = true;

    switch (alertId) {
      case "001":
        if (!date || !time || !email || !name) {
          throw new Error("Missing required fields for service notification");
        }
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

      case "002":
        if (!reg_no || !email || !name) {
          throw new Error("Missing required fields for OTA notification");
        }
        payload = {
          definitionKey: "OTA _FOTA",
          recipients: [
            {
              contactKey: contact_number,
              to: email,
              attributes: {
                SubscriberKey: contact_number,
                EmailAddress: email,
                REGISTRATIONNUMBER: reg_no,
                Contact_Key: contact_number,
                CUSTOMERNAME: name,
              },
            },
          ],
        };
        break;

      case "003":
        if (!contact_number) {
          throw new Error("Missing contact number for SMS notification");
        }
        const otp = generateOTP();
        console.log(`Generated OTP for ${contact_number}: ${otp}`);

        payload = {
          Subscribers: [
            {
              MobileNumber: contact_number,
              SubscriberKey: contact_number,
              Attributes: {
                OTPNUMBER: otp,
              },
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

    if (payload) {
      const response = await sendNotification(payload, isEmail);
      console.log(
        `${isEmail ? "Email" : "SMS"} sent successfully for alertId: ${alertId}`
      );
      return response;
    }
  } catch (error) {
    console.error("Error processing message:", error);
    throw error;
  }
};

const startKafkaProcessing = async () => {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: SOURCE_TOPIC, fromBeginning: false });
    await consumer.run({
      eachMessage: async ({ message }) => {
        await processMessage(message);
      },
    });
  } catch (error) {
    console.error("Error in Kafka processing:", error);
  }
};

// Express setup
const app = express();
app.use(bodyParser.json());

app.post("/notify-service", async (req, res) => {
  try {
    const { url, contact, sessionId } = req.body;

    if (!url || !contact || !sessionId) {
      return res.status(400).json({
        error: "Missing required fields: url, contact, or sessionId",
      });
    }

    const payload = {
      definitionKey: "Worry_free_service",
      recipients: [
        {
          contactKey: contact,
          to: DEFAULT_EMAIL,
          attributes: {
            SubscriberKey: contact,
            EmailAddress: DEFAULT_EMAIL,
            CUSTOMERNAME: sessionId,
            DEALERNAME: url,
            DATE: "date",
            TIME: "time",
          },
        },
      ],
    };

    const response = await sendNotification(payload, true);
    res.json({
      message: "Email notification sent successfully",
      data: response,
    });
  } catch (error) {
    console.error("Error sending notification:", error);
    res.status(500).json({
      error: "Failed to send notification",
      details: error.message,
    });
  }
});

// Start Express server
const PORT = process.env.PORT || 3008;
app.listen(PORT, () => {
  console.log(`API server running on port ${PORT}`);
});

startKafkaProcessing().catch(console.error);
