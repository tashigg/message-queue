const mqtt = require("mqtt");

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

        const messages = [];

        await new Promise((resolver) => {
            client2.on("message", async (topic, message, packet) => {
                const messageStr = message.toString();

                console.log(topic.toString() + " message received: " + messageStr);

                messages.push({topic, message: messageStr, qos: packet.qos});

                if (messageStr === "rainy") {
                    resolver();
                }
            });
        });

        await client1.endAsync();
        await client2.endAsync();

        // Depending on when the QoS 0 message gets through TCE, it may or may not get delivered.
        if (messages.length === 2) {
            expect(messages).toEqual([
                {topic: "weather/sacramento", message: "cloudy", qos: 1},
                {topic: "weather/sacramento", message: "rainy", qos: 2},
            ]);
        } else {
            expect(messages).toEqual([
                {topic: "weather/sacramento", message: "sunny", qos: 0},
                {topic: "weather/sacramento", message: "cloudy", qos: 1},
                {topic: "weather/sacramento", message: "rainy", qos: 2},
            ]);
        }
    });

});
