//! The service of `examples/mqtt_subscriptions_lanes.rs` without the macros: the handlers are
//! named types, the lanes a mount step, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_mqtt_subscriptions_lanes --features testing
//! cargo run --example manual_mqtt_subscriptions_lanes --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:lanes]
use ruststream_rumqttc::PARTITION_KEY_HEADER;
use ruststream_rumqttc::prelude::*;

struct Place;

impl Handle<Order> for Place {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, tenant = %order.tenant, "order placed");
        ready(Ok(()))
    }
}

/// The checkout keys every order it places by its tenant.
struct Checkout;

impl<Entry> Handle<Purchase, (), Outs<(Entry,)>> for Checkout
where
    Entry: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        purchase: &Purchase,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let order = Order {
            id: purchase.id,
            tenant: purchase.tenant.clone(),
        };
        let mut key = HeaderMap::new();
        key.insert(PARTITION_KEY_HEADER, purchase.tenant.clone());
        let placed = outs.get(DefaultSlot).message(&order).with_headers(key);
        if placed.publish().await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "orders-svc"),
        |b| {
            // Eight lanes: one tenant's orders never run at the same time, and different
            // tenants run side by side.
            b.include(
                subscriber("orders/placed", Place)
                    .workers_by_key(nonzero!(8))
                    .build(),
            );
            b.include(subscriber("checkout/completed", Checkout).build())
                .out(DefaultSlot, Publish::default())
                .build();
        },
    )
}
// --8<-- [end:lanes]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    tenant: String,
}

// `#[outgoing(name = "orders/placed")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders/placed";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Purchase {
    id: u64,
    tenant: String,
}

// `#[outgoing(name = "checkout/completed")]` by hand.
impl OutgoingDestination for Purchase {
    type Form = FixedName;
    const DESTINATION: &'static str = "checkout/completed";
}

impl MessageHeaders for Purchase {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::schemars::JsonSchema;
    use ruststream::testing::TestApp;
    use ruststream_rumqttc::PARTITION_KEY_HEADER;
    use ruststream_rumqttc::prelude::*;
    use serde::{Deserialize, Serialize};

    use super::{Order, Purchase, app};

    /// An order on the topic whose lanes do real work.
    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
    #[schemars(crate = "ruststream::schemars")]
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

    /// The lanes of the page with real work in them: the first order takes longer than the rest.
    struct PlaceTimed;

    impl<Entry> Handle<Timed, (), Outs<(Entry,)>> for PlaceTimed
    where
        Entry: OutEntry<DefaultSlot, Wire: Publisher>,
    {
        async fn handle(
            &self,
            order: &Timed,
            outs: &Outs<(Entry,)>,
            _ctx: &mut Context<'_>,
        ) -> Result<(), HandlerOutcome> {
            let work = if order.id == 1 { 30 } else { 1 };
            tokio::time::sleep(Duration::from_millis(work)).await;
            let finished = Finished { id: order.id };
            if outs
                .get(DefaultSlot)
                .message(&finished)
                .publish()
                .await
                .is_err()
            {
                return Err(HandlerOutcome::retry());
            }
            Ok(())
        }
    }

    fn timed() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            MqttBroker::new("mqtt://localhost:1883", "orders-svc"),
            |b| {
                b.include(
                    subscriber("orders/timed", PlaceTimed)
                        .workers_by_key(nonzero!(8))
                        .build(),
                )
                .out(DefaultSlot, Publish::default())
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
