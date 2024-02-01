const mqtt = require("mqtt");

beforeAll(async () => {
    client1 = await mqtt.connectAsync("mqtt://localhost:1883");
    client2 = await mqtt.connectAsync("mqtt://localhost:1884");
});

test("publish to node1, receive from node2", async () => {
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
