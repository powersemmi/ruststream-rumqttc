//! Keyed lanes: deliveries that share the `partition-key` header run in order on one lane, and
//! different keys run in parallel. The publisher sets the header, which rides a user property.
//!
//! ```text
//! cargo test --example mqtt_subscriptions_lanes --features testing
//! cargo run --example mqtt_subscriptions_lanes --features testing -- run
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:lanes]
use ruststream_rumqttc::PARTITION_KEY_HEADER;
use ruststream_rumqttc::prelude::*;

/// Eight lanes: one tenant's orders never run at the same time, and different tenants run side
/// by side.
#[subscriber("orders/placed", workers(8, by_key))]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, tenant = %order.tenant, "order placed");
    HandlerOutcome::ack()
}

/// The checkout keys every order it places by its tenant.
#[subscriber("checkout/completed")]
async fn checkout(purchase: &Purchase, Out(orders): Out<impl Publisher>) -> HandlerOutcome {
    let order = Order {
        id: purchase.id,
        tenant: purchase.tenant.clone(),
    };
    let mut key = HeaderMap::new();
    key.insert(PARTITION_KEY_HEADER, purchase.tenant.clone());
    if orders
        .message(&order)
        .with_headers(key)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "orders-svc"),
        |b| {
            b.include(place);
            b.include(checkout)
                .out(DefaultSlot, Publish::default())
                .build();
        },
    )
}
// --8<-- [end:lanes]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders/placed")]
struct Order {
    id: u64,
    tenant: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "checkout/completed")]
struct Purchase {
    id: u64,
    tenant: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::PARTITION_KEY_HEADER;
    use ruststream_rumqttc::prelude::*;
    use serde::{Deserialize, Serialize};

    use super::{Order, Purchase, app};

    /// An order on the topic whose lanes do real work.
    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(name = "orders/timed")]
    struct Timed {
        id: u64,
        tenant: String,
    }

    /// What a lane finished, in the order lanes finish.
    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(name = "orders/finished")]
    struct Finished {
        id: u64,
    }

    #[derive(OutSlot)]
    #[publishes(Finished)]
    struct Finishes;

    /// The lanes of the page with real work in them: the first order takes longer than the rest.
    #[subscriber("orders/timed", workers(8, by_key))]
    async fn place_timed(
        order: &Timed,
        Out(finishes): Out<impl Publisher, Finishes>,
    ) -> HandlerOutcome {
        let work = if order.id == 1 { 30 } else { 1 };
        tokio::time::sleep(Duration::from_millis(work)).await;
        if finishes
            .message(&Finished { id: order.id })
            .publish()
            .await
            .is_err()
        {
            return HandlerOutcome::retry();
        }
        HandlerOutcome::ack()
    }

    fn timed() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            MqttBroker::new("mqtt://localhost:1883", "orders-svc"),
            |b| {
                b.include(place_timed)
                    .out(Finishes, Publish::default())
                    .build();
            },
        )
    }

    /// The checkout's publish carries the tenant as the partition key, and the lanes take it.
    #[tokio::test]
    async fn the_checkout_keys_each_order_by_its_tenant() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 7,
            tenant: "acme".to_owned(),
        };
        let purchase = Purchase {
            id: 7,
            tenant: "acme".to_owned(),
        };
        tb.broker::<MqttBroker>()
            .message(&purchase)
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .published::<Order>("orders/placed")
            .assert_called_once()
            .with_header(PARTITION_KEY_HEADER, "acme");
        tb.broker::<MqttBroker>()
            .subscriber("orders/placed")
            .assert_called_once()
            .with(&order)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    // All four orders are on the topic before the first one finishes. The slow first `acme`
    // order holds back the other two `acme` orders on its lane, and `globex` finishes beside it.
    #[tokio::test(start_paused = true)]
    async fn one_tenants_orders_finish_in_publish_order() -> Result<(), Box<dyn Error>> {
        let mut publisher = None;
        let tb = TestApp::with_state(timed(), |brokers| {
            publisher = Some(brokers.broker::<MqttBroker>().publisher());
        })
        .await?;
        let publisher = publisher.ok_or("the state builder ran")?;
        for (id, tenant) in [(1, "acme"), (2, "globex"), (3, "acme"), (4, "acme")] {
            let mut key = HeaderMap::new();
            key.insert(PARTITION_KEY_HEADER, tenant);
            publisher
                .message(&Timed {
                    id,
                    tenant: tenant.to_owned(),
                })
                .with_headers(key)
                .publish()
                .await?;
        }
        tb.advance(Duration::from_millis(100)).await?;

        let finished: Vec<u64> = tb
            .broker::<MqttBroker>()
            .published::<Finished>("orders/finished")
            .assert_called(4)
            .decoded()
            .into_iter()
            .map(|finished| finished.id)
            .collect();
        assert_eq!(finished, [2, 1, 3, 4]);
        tb.shutdown().await?;
        Ok(())
    }
}
