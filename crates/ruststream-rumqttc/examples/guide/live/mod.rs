//! What the live legs of the guides' tests share: the gate on the stand's address, a client id no
//! other run holds, and a process that dies without a word.
//!
//! A live test skips when `MQTT_TEST_URL` is unset, which keeps `cargo test` usable on a machine
//! with no broker. A job that stood a broker up sets `RUSTSTREAM_REQUIRE_LIVE`, and under it the
//! skip becomes a failure: a test that returns before its first assertion reports `ok` like one
//! that ran.

#![allow(dead_code)]

use std::error::Error;
use std::process;
use std::thread;

use ruststream::runtime::App;
use ruststream::testing::TestApp;

/// The stand's address, or `None` to skip the test.
///
/// # Panics
///
/// Panics when `RUSTSTREAM_REQUIRE_LIVE` is set and the address is not.
pub(crate) fn url() -> Option<String> {
    match std::env::var("MQTT_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            assert!(
                std::env::var_os("RUSTSTREAM_REQUIRE_LIVE").is_none(),
                "RUSTSTREAM_REQUIRE_LIVE is set, but MQTT_TEST_URL is missing or empty"
            );
            None
        }
    }
}

/// A client id this run alone holds: a second connection under one id takes the session over,
/// and other runs use the same stand at the same time.
pub(crate) fn client_id(name: &str) -> String {
    format!("{name}-{}", process::id())
}

/// Starts the app `build` returns against the stand, then kills it: its runtime is dropped with
/// the connection open, so the server sees the connection close without a `DISCONNECT`, as it
/// does when a process crashes.
///
/// # Errors
///
/// Returns the error the start failed with.
pub(crate) fn crash<A, Build>(build: Build) -> Result<(), Box<dyn Error + Send + Sync>>
where
    A: App<State = ()>,
    Build: FnOnce() -> A + Send + 'static,
{
    // A runtime of its own, on a thread of its own: dropping a runtime inside another one panics.
    thread::spawn(move || -> Result<(), Box<dyn Error + Send + Sync>> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let tb = runtime.block_on(TestApp::start_live(build()))?;
        drop(tb);
        drop(runtime);
        Ok(())
    })
    .join()
    .map_err(|_| "the crashing service panicked")?
}
