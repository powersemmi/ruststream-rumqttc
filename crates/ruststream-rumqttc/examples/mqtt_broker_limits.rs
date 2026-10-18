//! The limits of one connection: the largest packet the server may send, and how many
//! unacknowledged deliveries it may have in flight. The packet limit is tested in process; the
//! window lives on the server, so its test runs against the stand and skips without it.
//!
//! ```text
//! cargo test --example mqtt_broker_limits --features testing
//! just brokers-up
//! MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --example mqtt_broker_limits --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::num::{NonZeroU16, NonZeroU32};

use serde::{Deserialize, Serialize};

use ruststream_rumqttc::prelude::*;

#[cfg(test)]
#[path = "../tests/live_broker/mod.rs"]
mod live;

#[subscriber("broker-limits/firmware")]
async fn stage(firmware: &Firmware) -> HandlerOutcome {
    tracing::info!(
        firmware.version,
        bytes = firmware.image.len(),
        "firmware staged"
    );
    HandlerOutcome::ack()
}

fn service(broker: MqttBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("firmware", "0.1.0")).with_broker(broker, |b| {
        b.include(stage);
    })
}

// --8<-- [start:broker]
const LARGEST_PACKET: NonZeroU32 = NonZeroU32::new(256 * 1024).unwrap();
const IN_FLIGHT: NonZeroU16 = NonZeroU16::new(100).unwrap();

/// Firmware images reach 256 KiB, and at most 100 deliveries wait for an acknowledgement.
fn broker(url: &str, client_id: &str) -> MqttBroker {
    MqttBroker::new(url, client_id)
        .max_packet_size(LARGEST_PACKET)
        .receive_maximum(IN_FLIGHT)
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(broker("mqtt://localhost:1883", "firmware-svc"))
}
// --8<-- [end:broker]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "broker-limits/firmware")]
struct Firmware {
    version: String,
    image: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::num::NonZeroU16;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;
    use serde::{Deserialize, Serialize};

    use super::{Firmware, app, broker, live};

    fn firmware(version: &str, bytes: usize) -> Firmware {
        Firmware {
            version: version.to_owned(),
            image: "f".repeat(bytes),
        }
    }

    #[tokio::test]
    async fn an_image_over_the_packet_limit_never_arrives() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&firmware("1.0.0", 200 * 1024))
            .publish()
            .await?;
        tb.broker::<MqttBroker>()
            .message(&firmware("2.0.0", 300 * 1024))
            .publish()
            .await?;

        let staged = tb
            .broker::<MqttBroker>()
            .subscriber("broker-limits/firmware")
            .assert_called_once()
            .received::<Firmware>();
        assert_eq!(staged[0].version, "1.0.0");
        Ok(())
    }

    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(name = "broker-limits/held")]
    struct Job {
        id: u64,
    }

    /// Leaves every delivery unacknowledged: MQTT has no negative acknowledgement, so a retry
    /// waits for the next session and holds its place in the window until then.
    #[subscriber("broker-limits/held")]
    async fn hold(job: &Job) -> HandlerOutcome {
        tracing::info!(job.id, "job held");
        HandlerOutcome::retry()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_window_holds_deliveries_until_one_is_acknowledged() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("MQTT_TEST_URL") else {
            return Ok(());
        };
        let broker =
            broker(&url, &live::client_id("broker-limits")).receive_maximum(NonZeroU16::MIN);
        let app = RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(broker, |b| {
            b.include(hold);
        });
        // The second job never arrives, so the bound is how long the test waits for it.
        let tb = TestApp::start_live_within(app, Duration::from_secs(2)).await?;

        tb.broker::<MqttBroker>()
            .message(&Job { id: 1 })
            .publish()
            .await?;
        let second = tb
            .broker::<MqttBroker>()
            .message(&Job { id: 2 })
            .publish()
            .await;

        assert!(second.is_err(), "the server delivered past a full window");
        tb.broker::<MqttBroker>()
            .subscriber("broker-limits/held")
            .assert_called_once()
            .with(&Job { id: 1 })
            .settled(HandlerOutcome::retry());
        tb.shutdown().await?;
        Ok(())
    }
}
