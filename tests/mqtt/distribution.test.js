const mqtt = require("mqtt");
const fs = require("node:fs");

describe("publish to node 1, receive from node2", () => {
    // Putting tests in a `describe()` block appears to cause them to execute sequentially.
    // This way we can avoid any unintentional cross-pollination between tests.
    test("synchronously", async () => {
        // Defaults to 4 otherwise
        var client1 = await mqtt.connectAsync("mqtt://localhost:1883", {protocolVersion: 5});
        var client2 = await mqtt.connectAsync("mqtt://localhost:1884", {protocolVersion: 5});

        await client2.subscribeAsync("weather");

        await client1.publishAsync("weather", "cloudy");

        return new Promise((resolver) => {
            client2.on("message", async (topic, message, packet) => {
                console.log(topic.toString() + " message received: " + message.toString());
                expect(topic.toString()).toBe("weather");
                expect(message.toString()).toBe("cloudy");
                await client1.endAsync();
                await client2.endAsync();
                resolver();
            });
        });
    });

    test("asynchronously, delivered on reconnect", async () => {
        // Defaults to 4 otherwise
        var client1 = await mqtt.connectAsync("mqtt://localhost:1883", {protocolVersion: 5});
        var client2 = await mqtt.connectAsync("mqtt://localhost:1884", {protocolVersion: 5});

        await client2.subscribeAsync("weather/sacramento", {qos: 2});

        await client2.endAsync();

        await client1.publishAsync("weather/sacramento", "sunny");
        await client1.publishAsync("weather/sacramento", "cloudy", {qos: 1});
        await client1.publishAsync("weather/sacramento", "rainy", {qos: 2});

        client2.reconnect();

        const unorderedMessages = [];
        const orderedMessages = [];

        await new Promise((resolver) => {
            client2.on("message", async (topic, message, packet) => {
                const messageStr = message.toString();

                console.log(topic.toString() + " message received: " + messageStr);

                if (packet.qos === 0) {
                    unorderedMessages.push({topic, message: messageStr, qos: packet.qos});
                } else {
                    orderedMessages.push({topic, message: messageStr, qos: packet.qos});
                }

                if (messageStr === "rainy") {
                    resolver();
                }
            });
        });

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
        var client1 = await mqtt.connectAsync("mqtts://127.0.0.1:8883", {
            // Defaults to 4 otherwise
            protocolVersion: 5,
            servername: "broker1.example.com",
            ca: fs.readFileSync("foxmq.d/key_0.crt"),
            minVersion: "TLSv1.3"
        });
        var client2 = await mqtt.connectAsync(
            "mqtts://127.0.0.1:8884",
            {
                protocolVersion: 5,
                servername: "broker2.example.com",
                ca: fs.readFileSync("foxmq.d/key_1.crt"),
                minVersion: "TLSv1.3"
            });

        await client2.subscribeAsync("weather");

        await client1.publishAsync("weather", "cloudy");

        return new Promise((resolver) => {
            client2.on("message", async (topic, message, packet) => {
                console.log(topic.toString() + " message received: " + message.toString());
                expect(topic.toString()).toBe("weather");
                expect(message.toString()).toBe("cloudy");
                await client1.endAsync();
                await client2.endAsync();
                resolver();
            });
        });
    });
});
