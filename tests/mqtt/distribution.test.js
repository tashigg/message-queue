const mqtt = require("mqtt");
const fs = require("node:fs");

const events = require("node:events");

const timers = require("node:timers/promises");

describe("publish to node 1, receive from node2", () => {
    // Putting tests in a `describe()` block appears to cause them to execute sequentially.
    // This way we can avoid any unintentional cross-pollination between tests.
    test("synchronously", async () => {
        // `protocolVersion` defaults to 4 (v3.1.1) otherwise
        const client1 = await mqtt.connectAsync("mqtt://localhost:1883", {protocolVersion: 5});
        const client2 = await mqtt.connectAsync("mqtt://localhost:1884", {protocolVersion: 5});

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
        const client1 = await mqtt.connectAsync("mqtt://localhost:1883", {protocolVersion: 5});
        const client2 = await mqtt.connectAsync("mqtt://localhost:1884", {protocolVersion: 5});

        await client2.subscribeAsync("weather/sacramento", {qos: 2});

        await client2.endAsync();

        await client1.publishAsync("weather/sacramento", "sunny");
        await client1.publishAsync("weather/sacramento", "cloudy", {qos: 1});
        await client1.publishAsync("weather/sacramento", "rainy", {qos: 2});

        client2.reconnect();

        const unorderedMessages = [];
        const orderedMessages = [];

        for await (const [topic, message, packet] of events.on(client2, 'message')) {
            const messageStr = message.toString();

            console.log(topic.toString() + " message received: " + messageStr);

            if (packet.qos === 0) {
                unorderedMessages.push({topic, message: messageStr, qos: packet.qos});
            } else {
                orderedMessages.push({topic, message: messageStr, qos: packet.qos});
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
                {topic: "weather/sacramento", message: "sunny", qos: 0}
            ])
        } else {
            expect(unorderedMessages).toEqual([]);
        }

        expect(orderedMessages).toEqual([
            {topic: "weather/sacramento", message: "cloudy", qos: 1},
            {topic: "weather/sacramento", message: "rainy", qos: 2},
        ]);
    });

    test("synchronously, over TLS", async () => {
        // Note: if you use `localhost` here the TLS stack will try to verify it against the subjectAltName
        // on the server's TLS certificate. Using an IP address appears to bypass that.
        const client1 = await mqtt.connectAsync("mqtts://127.0.0.1:8883", {
            // Defaults to 4 otherwise
            protocolVersion: 5,
            servername: "broker1.example.com",
            ca: fs.readFileSync("foxmq.d/key_0.crt"),
            minVersion: "TLSv1.3"
        });
        const client2 = await mqtt.connectAsync(
            "mqtts://127.0.0.1:8884",
            {
                protocolVersion: 5,
                servername: "broker2.example.com",
                ca: fs.readFileSync("foxmq.d/key_1.crt"),
                minVersion: "TLSv1.3"
            });

        await client2.subscribeAsync("weather");

        await client1.publishAsync("weather", "cloudy");

        const [topic, message] = await events.once(client2, 'message');

        console.log(topic.toString() + " message received: " + message.toString());
        expect(topic.toString()).toBe("weather");
        expect(message.toString()).toBe("cloudy");
        await client1.endAsync();
        await client2.endAsync();
    });

    test("retained messages", async () => {
        // The timeout to wait when we want to make sure no more messages are being sent on a topic.
        const no_message_timeout = 500;

        const client1 = await mqtt.connectAsync("mqtt://localhost:1883", {protocolVersion: 5});
        const client2 = await mqtt.connectAsync("mqtt://localhost:1884", {protocolVersion: 5});

        await client1.publishAsync("tickers/eth/usd", "3107.60", {qos: 1, retain: true});
        await client1.publishAsync("tickers/eth", '{ "usd": "3107.60" }', {qos: 1, retain: true});
        await client1.publishAsync("tickers/btc/usd", "62838.80", {qos: 1, retain: true});
        await client1.publishAsync("tickers/btc", '{ "usd": "62838.80" }', {qos: 1, retain: true});

        const received_msgs = [];

        await client2.subscribeAsync("tickers/#");

        // Makes sure the retained messages are sent before we test the actual retain handling
        for await (const [topic, message] of events.on(client2, 'message')) {
            received_msgs.push({
                topic,
                message: message.toString()
            });

            if (received_msgs.length === 4) {
                break;
            }
        }

        await client2.unsubscribeAsync("tickers/#");

        // Retained message handling gives messages a total order; in this case, it's based on the order they were sent.
        expect(received_msgs).toEqual([
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

        // Test retained message delivery
        {
            await client2.subscribeAsync("tickers/eth");

            const [topic, message] = await events.once(client2, 'message');

            expect({topic, message: message.toString()}).toEqual(
                {
                    topic: "tickers/eth",
                    message: '{ "usd": "3107.60" }'
                },
            );

            // Ensure only one message is delivered.
            const result = await Promise.race([
                events.once(client2, 'message'),
                // If the timeout elapses before another message is delivered, this promise will resolve to `[]`.
                timers.setTimeout(no_message_timeout, [])
            ]);

            expect(result).toEqual([]);

            await client2.unsubscribeAsync("tickers/eth");
        }

        {
            // This is a wildcard filter but should only match the one topic.
            await client2.subscribeAsync("tickers/btc/#");

            const [topic, message] = await events.once(client2, 'message');

            expect({topic, message: message.toString()}).toEqual(
                {
                    topic: "tickers/btc/usd",
                    message: "62838.80"
                },
            );

            // Ensure only one message is delivered.
            const result = await Promise.race([
                events.once(client2, 'message'),
                // If the timeout elapses before another message is delivered, this promise will resolve to `[]`.
                timers.setTimeout(no_message_timeout, [])
            ]);

            expect(result).toEqual([]);

            await client2.unsubscribeAsync("tickers/btc/#");
        }

        {
            // Since this is a single-level wildcard, we should only expect 2 messages.
            await client2.subscribeAsync("tickers/+");

            const received_msgs = [];

            for await (const [topic, message] of events.on(client2, 'message')) {
                received_msgs.push({
                    topic,
                    message: message.toString()
                });

                if (received_msgs.length === 2) {
                    break;
                }
            }

            // Ensure only one message is delivered.
            const result = await Promise.race([
                events.once(client2, 'message'),
                // If the timeout elapses before another message is delivered, this promise will resolve to `[]`.
                timers.setTimeout(no_message_timeout, [])
            ]);

            expect(result).toEqual([]);

            // This is the order the messages were sent.
            expect(received_msgs).toEqual([
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

        await client1.endAsync();
        await client2.endAsync();
    });
});
