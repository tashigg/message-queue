use rumqttc::v5::{
    mqttbytes::{v5::SubscribeProperties, QoS},
    AsyncClient, Event, MqttOptions,
};
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut mqttoptions = MqttOptions::new("client", "0.0.0.0", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(10));
    mqttoptions.set_credentials("sharkst", "1234");

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    client
        .subscribe_with_properties(
            "test/topic",
            QoS::AtLeastOnce,
            SubscribeProperties {
                id: Some(1),
                user_properties: vec![(
                    "include_broker_timestamps".to_string(),
                    "true".to_string(),
                )],
            },
        )
        .await?;

    client
        .publish("test/topic", QoS::AtLeastOnce, false, "Hello")
        .await?;

    while let Ok(event) = eventloop.poll().await {
        if let Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::Publish(publish)) = event {
            println!(
                "Received message: {}",
                String::from_utf8_lossy(&publish.payload)
            );

            if let Some(properties) = publish.properties {
                for prop in properties.user_properties {
                    println!("{:#?}", prop);
                }
            }
        }
    }

    Ok(())
}
