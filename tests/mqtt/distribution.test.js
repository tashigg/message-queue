const mqtt = require("mqtt");
const fs = require("node:fs");

const events = require("node:events");

const timers = require("node:timers/promises");

describe("publish to node 1, receive from node2", () => {

    test("synchronously", async () => {
        // Test v4 (3.1.1) and v5 (5.0) simultaneously
        const client1 = await mqtt.connectAsync("mqtt://localhost:1883", { protocolVersion: 4 });
        // `protocolVersion` defaults to 4 (v3.1.1) otherwise
        const client2 = await mqtt.connectAsync("mqtt://localhost:1884", { protocolVersion: 5 });

        await client2.subscribeAsync("weather");

        await client1.publishAsync("weather", "cloudy");

        // `node:events` has functions to promisify events now
        const [topic, message] = await events.once(client2, 'message');

        console.log(topic.toString() + " message received: " + message.toString());
        expect(topic.toString()).toBe("weather");
        expect(message.toString()).toBe("cloudy");
        await client1.endAsync();
        await client2.endAsync();
    });

    test("asynchronously, delivered on reconnect", async () => {
        const client1 = await mqtt.connectAsync("mqtt://localhost:1883", { protocolVersion: 4 });
        const client2 = await mqtt.connectAsync("mqtt://localhost:1884", { protocolVersion: 5 });

        await client2.subscribeAsync("weather/sacramento", { qos: 2 });

        await client2.endAsync();

        await client1.publishAsync("weather/sacramento", "sunny");
        await client1.publishAsync("weather/sacramento", "cloudy", { qos: 1 });
        await client1.publishAsync("weather/sacramento", "rainy", { qos: 2 });

        client2.reconnect();

        const unorderedMessages = [];
        const orderedMessages = [];

        for await (const [topic, message, packet] of events.on(client2, 'message')) {
            const messageStr = message.toString();

            console.log(topic.toString() + " message received: " + messageStr);

            if (packet.qos === 0) {
                unorderedMessages.push({ topic, message: messageStr, qos: packet.qos });
            } else {
                orderedMessages.push({ topic, message: messageStr, qos: packet.qos });
            }

            if (messageStr === "rainy") {
                break;
            }
        }

        await client1.endAsync();
        await client2.endAsync();

        // QoS 0 messages are not guaranteed to be in order with QoS 1 and 2 messages,
        // and that's how FoxMQ treats them.
        //
        // Depending on the order that things actually happen, the QoS 0 message may not get delivered.
        if (unorderedMessages.length === 1) {
            expect(unorderedMessages).toEqual([
                { topic: "weather/sacramento", message: "sunny", qos: 0 }
            ])
        } else {
            expect(unorderedMessages).toEqual([]);
        }

        expect(orderedMessages).toEqual([
            { topic: "weather/sacramento", message: "cloudy", qos: 1 },
            { topic: "weather/sacramento", message: "rainy", qos: 2 },
        ]);
    });

    test("synchronously, over TLS", async () => {
        // Note: if you use `localhost` here the TLS stack will try to verify it against the subjectAltName
        // on the server's TLS certificate. Using an IP address appears to bypass that.
        const client1 = await mqtt.connectAsync("mqtts://127.0.0.1:8883", {
            // Test v4 (3.1.1) and v5 (5.0) simultaneously
            protocolVersion: 4,
            servername: "broker1.example.com",
            ca: fs.readFileSync("foxmq.d/key_0.crt"),
            minVersion: "TLSv1.3"
        });
        const client2 = await mqtt.connectAsync("mqtts://127.0.0.1:8884", {
            protocolVersion: 5,
            servername: "broker2.example.com",
            ca: fs.readFileSync("foxmq.d/key_1.crt"),
            minVersion: "TLSv1.3"
        });

        await client2.subscribeAsync("weather/singapore");

        await client1.publishAsync("weather/singapore", "cloudy");

        const [topic, message] = await events.once(client2, 'message');

        console.log(topic.toString() + " message received: " + message.toString());
        expect(topic.toString()).toBe("weather/singapore");
        expect(message.toString()).toBe("cloudy");
        await client1.endAsync();
        await client2.endAsync();
    });

    test("synchronously, over Websockets", async () => {
        // Test v4 (3.1.1) and v5 (5.0) simultaneously
        const client1 = await mqtt.connectAsync("ws://127.0.0.1:8080", { protocolVersion: 4 });
        const client2 = await mqtt.connectAsync("ws://127.0.0.1:8081", { protocolVersion: 5 });

        await client2.subscribeAsync("weather/los_angeles");

        await client1.publishAsync("weather/los_angeles", "cloudy");

        const [topic, message] = await events.once(client2, 'message');

        console.log(topic.toString() + " message received: " + message.toString());
        expect(topic.toString()).toBe("weather/los_angeles");
        expect(message.toString()).toBe("cloudy");
        await client1.endAsync();
        await client2.endAsync();
    });

    test("retained messages", async () => {
        /**
         * Collect messages from `client`, waiting at most `timeoutMs` to make sure we got all that will arrive.
         */
        async function collectMessages(client, timeoutMs = 500) {
            const messages = [];

            const messagesIter = events.on(
                client,
                'message',
                // This will cause the loop to throw an `AbortError`.
                { signal: AbortSignal.timeout(timeoutMs) }
            );

            try {
                for await (const [topic, message] of messagesIter) {
                    messages.push({
                        topic,
                        message: message.toString()
                    });
                }
            } catch (e) {
                if (e.name !== 'AbortError') {
                    throw e;
                }
            }

            return messages;
        }

        const client1 = await mqtt.connectAsync("mqtt://localhost:1883", { protocolVersion: 4 });
        const client2 = await mqtt.connectAsync("mqtt://localhost:1884", { protocolVersion: 5 });

        console.log('sending retained messages');

        // We need to create this before we subscribe to avoid racing the subscribe call itself.
        //
        // This will buffer messages until awaited.
        const messages = collectMessages(client2);

        await client2.subscribeAsync("tickers/#");

        await client1.publishAsync("tickers/eth/usd", "3107.60", { qos: 1, retain: true });
        await client1.publishAsync("tickers/eth", '{ "usd": "3107.60" }', { qos: 1, retain: true });
        await client1.publishAsync("tickers/btc/usd", "62838.80", { qos: 1, retain: true });
        await client1.publishAsync("tickers/btc", '{ "usd": "62838.80" }', { qos: 1, retain: true });

        console.log('waiting for retained messages');

        // Retained message handling gives messages a total order; in this case, it's based on the order they were sent.
        await expect(messages).resolves.toEqual([
            {
                topic: "tickers/eth/usd",
                message: "3107.60",
            },
            {
                topic: "tickers/eth",
                message: '{ "usd": "3107.60" }'
            },

            {
                topic: "tickers/btc/usd",
                message: "62838.80"
            },
            {
                topic: "tickers/btc",
                message: '{ "usd": "62838.80" }'
            },
        ]);

        await client2.unsubscribeAsync("tickers/#");

        // Test retained message delivery
        {
            console.log('test retained messages: exact topic');

            const messages = collectMessages(client2);

            await client2.subscribeAsync("tickers/eth");

            await expect(messages).resolves.toEqual([
                {
                    topic: "tickers/eth",
                    message: '{ "usd": "3107.60" }'
                },
            ]);

            await client2.unsubscribeAsync("tickers/eth");
        }

        {
            console.log('test retained messages: multi-level wildcard');

            const messages = collectMessages(client2);

            // Multi-level wildcards match their parent and any children.
            await client2.subscribeAsync("tickers/btc/#");

            await expect(messages).resolves.toEqual([
                {
                    topic: "tickers/btc/usd",
                    message: "62838.80"
                },
                {
                    topic: "tickers/btc",
                    message: '{ "usd": "62838.80" }'
                },
            ]);

            await client2.unsubscribeAsync("tickers/btc/#");
        }

        {
            console.log('test retained messages: single-level wildcard');

            const messages = collectMessages(client2);

            // Since this is a single-level wildcard, we should only expect 2 messages.
            await client2.subscribeAsync("tickers/+");

            // This is the order the messages were sent.
            await expect(messages).resolves.toEqual([
                {
                    topic: "tickers/eth",
                    message: '{ "usd": "3107.60" }'
                },
                {
                    topic: "tickers/btc",
                    message: '{ "usd": "62838.80" }'
                },
            ]);

            await client2.unsubscribeAsync("tickers/+");
        }

        console.log("closing clients");

        await client1.endAsync();
        await client2.endAsync();
    }),
        test("Permissions", async () => {
            async function waitForEventWithTimeout(emitter, eventName, timeoutMs) {
                const timeout = new Promise((_, reject) =>
                    setTimeout(() => reject(new Error(`Timeout waiting for ${eventName}`)), timeoutMs)
                );

                const eventPromise = events.once(emitter, eventName);

                return Promise.race([eventPromise, timeout]);
            }


            // Can publish to test_topic but not subscribe
            const client1 = await mqtt.connectAsync("mqtt://localhost:1883", { protocolVersion: 5, username: "test_user1", password: "1234" });
            // Can subscribe to test_topic but not publish
            const client2 = await mqtt.connectAsync("mqtt://localhost:1883", { protocolVersion: 5, username: "test_user2", password: "1234" });
            // Can do anything anywhere
            const client3 = await mqtt.connectAsync("mqtt://localhost:1883", { protocolVersion: 5 });


            // Client 2 can sub and client 1 can publish
            await client2.subscribeAsync("test_topic");

            await client1.publishAsync("test_topic", "a test message");

            const [topic, message] = await events.once(client2, 'message');

            expect(topic.toString()).toBe("test_topic");
            expect(message.toString()).toBe("a test message");

            // Client 2 cannot publish
            await client2.publishAsync("test_topic", "a test message");

            try {
                await waitForEventWithTimeout(client2, 'message', 100);
                throw new Error('Test failed: event was received unexpectedly');
            } catch (err) {
                expect(err.message).toBe('Timeout waiting for message');
            }


            // Client 1 cannot sub

            await client1.subscribeAsync("test_topic");

            await client1.publishAsync("test_topic", "a test message");

            try {
                await waitForEventWithTimeout(client1, 'message', 100);
                throw new Error('Test failed: event was received unexpectedly');
            } catch (err) {
                expect(err.message).toBe('Timeout waiting for message');
            }

            // User 3 can do anything in any topic

            await client3.subscribeAsync("test_topic");

            await client3.publishAsync("test_topic", "a test message");

            const [topic3, message3] = await events.once(client3, 'message');

            expect(topic3.toString()).toBe("test_topic");
            expect(message3.toString()).toBe("a test message");


            // Client 1 can do whatever in other topics.

            await client1.subscribeAsync("test_topic1");

            await client1.publishAsync("test_topic1", "a test message");

            const [topic_any_1, message_any_1] = await events.once(client1, 'message');

            expect(topic_any_1.toString()).toBe("test_topic1");
            expect(message_any_1.toString()).toBe("a test message");


            // Client 2 can do whatever in other topics.

            await client2.subscribeAsync("test_topic1");

            await client2.publishAsync("test_topic1", "a test message");

            const [topic_any_2, message_any_2] = await events.once(client2, 'message');

            expect(topic_any_2.toString()).toBe("test_topic1");
            expect(message_any_2.toString()).toBe("a test message");


            await client1.endAsync();
            await client2.endAsync();
            await client3.endAsync();
        })
        ;
});
