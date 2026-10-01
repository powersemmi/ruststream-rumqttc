//! A declared reply over a real MQTT connection, read off the wire by a client of the test's own:
//! where the answer goes and which MQTT 5 properties it carries, which only the packet a broker
//! delivers can show.
//!
//! The retry fallback over a real connection runs in `both_modes_mqtt.rs`, where each test body
//! runs in process and against the stand alike.
//!
//! Start a broker with `just brokers-up` (mosquitto), then:
//! `MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --all-features -- --test-threads=1`.

use std::time::Duration;

use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::{Packet, Publish as PublishPacket};
use rumqttc::v5::{AsyncClient, Event, EventLoop, MqttOptions};
use ruststream::{ConnectedBroker, ServerSpec};
use ruststream_rumqttc::ConnectedMqttBroker;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

const RECV_TIMEOUT: Duration = Duration::from_secs(15);

/// How long an assertion of absence waits before it counts as absence. It follows a delivery that
/// did arrive, so what is being timed is a broker that already had its answer ready.
const QUIET: Duration = Duration::from_millis(500);

/// The live broker URL, or `None` when there is no stand to run against.
///
/// Without a stand the gated tests skip quietly, which is what keeps the suite usable while
/// developing. `RUSTSTREAM_REQUIRE_LIVE` turns that skip into a failure: a job that means to run
/// against a real broker sets it, so a renamed variable, a dropped `env:` block or a reordered
/// step cannot leave the whole live suite reporting success without running a single assertion.
fn test_url() -> Option<String> {
    match std::env::var("MQTT_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            assert!(
                std::env::var_os("RUSTSTREAM_REQUIRE_LIVE").is_none(),
                "RUSTSTREAM_REQUIRE_LIVE is set, so the live handler suite must run, \
                 but MQTT_TEST_URL is missing or empty"
            );
            eprintln!("MQTT_TEST_URL is not set; skipping the live handler test");
            None
        }
    }
}

/// A connection of the test's own, which is where the first message comes from: the service under
/// test is the subscriber, not the sender.
async fn connect(url: &str, id: &str) -> ConnectedMqttBroker {
    MqttBroker::new(url, format!("live-{id}-{}", std::process::id()))
        .connect()
        .await
        .expect("broker connects")
}

#[derive(Debug, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "live/reply/requests")]
struct Ping {
    id: u64,
}

/// An answer goes wherever the mount site collects them, so the type leaves the topic open.
#[derive(Debug, Deserialize, Serialize, Outgoing)]
struct Pong {
    id: u64,
}

#[subscriber("live/reply/requests", publish("live/reply/answers"))]
async fn answer_ping(ping: &Ping) -> Pong {
    Pong { id: ping.id }
}

/// A client of the broker's own, subscribed and past its `SUBACK`, so what it reads afterwards is
/// the packet the service produced rather than this crate's view of it.
async fn raw_subscriber(url: &str, id: &str, filter: &str) -> (AsyncClient, EventLoop) {
    let authority = ServerSpec::host_from_url(url);
    let (host, port) = authority
        .rsplit_once(':')
        .expect("the stand url names a port");

    let mut options = MqttOptions::new(
        format!("{id}-{}", std::process::id()),
        host,
        port.parse::<u16>().expect("the port parses"),
    );
    options.set_max_packet_size(Some(1024 * 1024));
    let (client, mut eventloop) = AsyncClient::new(options, 16);
    client
        .subscribe(filter.to_owned(), QoS::AtLeastOnce)
        .await
        .expect("the raw client subscribes");
    loop {
        let event = tokio::time::timeout(RECV_TIMEOUT, eventloop.poll())
            .await
            .expect("the raw client reaches a SUBACK")
            .expect("the raw event loop stays alive");
        if matches!(event, Event::Incoming(Packet::SubAck(_))) {
            break;
        }
    }
    (client, eventloop)
}

/// The next PUBLISH packet the raw client reads, whatever protocol traffic precedes it.
async fn next_publish(eventloop: &mut EventLoop) -> PublishPacket {
    loop {
        let event = tokio::time::timeout(RECV_TIMEOUT, eventloop.poll())
            .await
            .expect("the raw client receives the publish")
            .expect("the raw event loop stays alive");
        if let Event::Incoming(Packet::Publish(publish)) = event {
            break publish;
        }
    }
}

/// Where a reply goes is the declaration's business, and the request's own response topic is not
/// part of it: a requester that names one is answered on the topic the mount site declared, and
/// the answer names no response topic of its own - that property belongs to whoever is asking.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reply_lands_where_it_was_declared_and_names_no_response_topic() {
    let Some(url) = test_url() else { return };

    let app = RustStream::new(AppInfo::new("live-reply", "0.1.0")).with_broker(
        MqttBroker::new(&url, format!("live-reply-{}", std::process::id())),
        |b| {
            b.include(answer_ping);
        },
    );
    let running = app.start().await.expect("the service starts");

    // One filter over both topics, so the answer and the topic it was not sent to are read on the
    // same connection and in the order the broker published them.
    let (_raw, mut eventloop) = raw_subscriber(&url, "reply-watcher", "live/reply/#").await;

    let sender = connect(&url, "reply-sender").await;
    let mut headers = HeaderMap::new();
    headers.insert("reply-to", "live/reply/elsewhere");
    sender
        .publisher()
        .message(&Ping { id: 7 })
        .with_headers(headers)
        .publish()
        .await
        .expect("publish succeeds");

    let request = next_publish(&mut eventloop).await;
    assert_eq!(request.topic, "live/reply/requests");
    assert_eq!(
        request
            .properties
            .expect("the request carries properties")
            .response_topic
            .as_deref(),
        Some("live/reply/elsewhere"),
        "the requester's own response topic reaches the packet"
    );

    let reply = next_publish(&mut eventloop).await;
    assert_eq!(
        reply.topic, "live/reply/answers",
        "the answer went where the mount site declared, not where the request pointed"
    );
    let properties = reply.properties.unwrap_or_default();
    assert_eq!(
        properties.response_topic, None,
        "a service's own publish describes no response topic: the property belongs to whoever is \
         asking"
    );
    assert_eq!(
        properties.content_type, None,
        "a media type is a header, and a reply carries the headers something put on it - the \
         codec of the position is not one of them"
    );
    assert_eq!(properties.payload_format_indicator, None);

    assert!(
        tokio::time::timeout(QUIET, next_publish(&mut eventloop))
            .await
            .is_err(),
        "nothing was published to the topic the request pointed at"
    );

    running.shutdown().await.expect("the service stops");
    sender.shutdown().await.expect("shutdown succeeds");
}
