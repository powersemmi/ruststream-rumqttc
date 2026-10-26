//! What the live tests of the broker examples share: the gate on the stand's addresses, a client
//! id no other run holds, and a process that dies without a word.
//!
//! A live test skips when its address is unset, which keeps `cargo test` usable on a machine with
//! no broker. The same skip in a job that stood a broker up is a lie, because a test that returns
//! before its first assertion reports `ok` like one that ran. Such a job sets
//! `RUSTSTREAM_REQUIRE_LIVE`, and under it every skip becomes a failure naming what it wanted.

#![allow(dead_code)]

use std::error::Error;
use std::panic::Location;
use std::path::PathBuf;
use std::process;
use std::thread;

use ruststream::runtime::App;
use ruststream::testing::TestApp;

/// The variable a job sets to say it stood a broker up, so skipping past it is a defect.
pub(crate) const REQUIRE_LIVE: &str = "RUSTSTREAM_REQUIRE_LIVE";

/// Whether this run is required to reach a live broker.
fn required() -> bool {
    std::env::var(REQUIRE_LIVE).is_ok_and(|value| !value.is_empty())
}

/// The value of `name`, or `None` to skip the test.
///
/// # Panics
///
/// Panics when [`REQUIRE_LIVE`] is set and `name` is not: a job that stood a broker up and then
/// lost its address is a broken job, and the tests behind it would pass without running.
pub(crate) fn url(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Some(value),
        _ => {
            assert!(
                !required(),
                "{REQUIRE_LIVE} is set, so this suite must run, but {name} is unset or empty",
            );
            None
        }
    }
}

/// The password listener's address and the password of the stand's user `tester`, or `None` to
/// skip. The stand defines the password, and the run hands it over, so none is written here.
pub(crate) fn auth() -> Option<(String, String)> {
    let url = url("MQTT_TEST_AUTH_URL")?;
    let password = self::url("MQTT_TEST_AUTH_PASSWORD")?;
    Some((url, password))
}

/// The TLS listener's address and the directory of the stand's certificates, or `None` to skip.
pub(crate) fn tls() -> Option<(String, PathBuf)> {
    let url = url("MQTT_TEST_TLS_URL")?;
    let dir = self::url("MQTT_TEST_TLS_DIR")?;
    Some((url, PathBuf::from(dir)))
}

/// A client id this run alone holds: a second connection under one id takes the session over, and
/// other runs use the same stand at the same time.
///
/// The macros and the manual variant of an example run in one process under one name, so the id
/// also says which of them asked: otherwise the second variant inherits the session and the
/// retained messages the first one left.
#[track_caller]
pub(crate) fn client_id(name: &str) -> String {
    let variant = if Location::caller().file().contains("/manual/") {
        "manual-"
    } else {
        ""
    };
    format!("{name}-{variant}{}", process::id())
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
