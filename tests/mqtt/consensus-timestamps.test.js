const mqtt = require("mqtt");
const events = require("node:events");

describe("Consensus Timestamps", () => {
    test("receive timestamp_received user property when requested", async () => {
        // Client 1: Subscriber
        console.log("Connecting Subscriber...");
        const subClient = await mqtt.connectAsync("mqtt://127.0.0.1:1883", { protocolVersion: 5 });

        // Client 2: Publisher
        console.log("Connecting Publisher...");
        const pubClient = await mqtt.connectAsync("mqtt://127.0.0.1:1883", { protocolVersion: 5 });

        // Setup message collector on Subscriber BEFORE verifying subscription or publishing
        const messagesReceived = [];
        const completionPromise = new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                reject(new Error("Timeout waiting for messages"));
            }, 10000);

            subClient.on('message', (topic, message, packet) => {
                console.log(`Received: ${topic} ${message.toString()}`);
                messagesReceived.push({
                    msg: message.toString(),
                    props: packet.properties
                });

                if (messagesReceived.length >= 3) {
                    clearTimeout(timeout);
                    resolve();
                }
            });
        });

        // Request timestamps via subscription properties
        console.log("Subscribing...");
        await subClient.subscribeAsync("timestamp/test", {
            qos: 1,
            properties: {
                userProperties: {
                    include_broker_timestamps: "true"
                }
            }
        });
        console.log("Subscribed!");

        // Publish messages from Client 2
        console.log("Publishing...");
        await pubClient.publishAsync("timestamp/test", "message 1", { qos: 1 });
        await pubClient.publishAsync("timestamp/test", "message 2", { qos: 1 });
        await pubClient.publishAsync("timestamp/test", "message 3", { qos: 1 });
        console.log("Published!");

        // Wait for subscriber to get them
        await completionPromise;

        // Verify content and timestamps
        expect(messagesReceived.length).toBe(3);

        let lastTimestamp = "";
        for (const m of messagesReceived) {
            expect(m.props).toBeDefined();
            expect(m.props.userProperties).toBeDefined();
            expect(m.props.userProperties.timestamp_received).toBeDefined();

            const currentTs = m.props.userProperties.timestamp_received;
            if (lastTimestamp !== "") {
                expect(currentTs >= lastTimestamp).toBe(true);
            }
            lastTimestamp = currentTs;
        }

        console.log("Timestamps verified:", messagesReceived.map(m => m.props.userProperties.timestamp_received));

        await subClient.endAsync();
        await pubClient.endAsync();
    }, 30000);
});
