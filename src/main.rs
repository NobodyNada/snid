use std::process::ExitCode;

use futures::TryFutureExt;
use tokio::{select, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

mod server;
mod snes;

#[cfg(feature = "jemallocator")]
#[global_allocator]
static MALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<ExitCode> {
    // Initialize tracing.
    let mut layers = vec![
        tracing_subscriber::fmt::layer()
            .with_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
            .boxed(),
    ];
    if std::env::var("TOKIO_CONSOLE_BIND").is_ok() {
        layers.push(console_subscriber::spawn().boxed())
    }
    let _flamegraph_guard = if let Ok(path) = std::env::var("FLAMEGRAPH") {
        let (layer, guard) = tracing_flame::FlameLayer::with_file(path)?;
        layers.push(layer.boxed());
        Some(guard)
    } else {
        None
    };
    tracing_subscriber::registry().with(layers).init();

    let cancel = CancellationToken::new();

    // Spawn our tasks:

    // - Initialize the SNES core UART interface.
    let (snes, snes_task) = snes::Snes::run();

    // - Initialize the SNI server.
    let sni_addr = "0.0.0.0:8191".parse()?;
    let sni_task = server::sni::run(sni_addr, snes.clone(), cancel.child_token());

    // - Initialize the QUsb2snes server.
    let qusb_addr = "0.0.0.0:23074".parse()?;
    let qusb_task = server::qusb::run(qusb_addr, snes, cancel.child_token());

    // - Listen for Ctrl+C presses.
    let cancel_ = cancel.clone();
    let ctrl_c_task = async move {
        let result = select! {
            r = tokio::signal::ctrl_c() => r,
            () = cancel_.cancelled() => Ok(())
        };
        tracing::info!("Shutting down...");

        // If Ctrl+C is pressed twice, terminate immediately.
        tokio::spawn(async {
            if let Ok(()) = tokio::signal::ctrl_c().await {
                std::process::exit(1);
            }
        });
        result
    };

    let mut tasks = JoinSet::new();
    tasks.build_task().name("SNI").spawn(sni_task)?;
    tasks.build_task().name("SNES").spawn(snes_task)?;
    tasks.build_task().name("QUsb2snes").spawn(qusb_task)?;
    tasks
        .build_task()
        .name("CtrlC")
        .spawn(ctrl_c_task.err_into())?;

    // Wait for our tasks.
    let mut status = ExitCode::SUCCESS;
    while let Some(result) = tasks.join_next().await {
        // If any task exits, shut down the rest.
        cancel.cancel();

        // If any task failed, log the error.
        if let Err(e) = result.unwrap_or_else(|e| Err(e.into())) {
            tracing::error!("{e:#?}");
            status = ExitCode::FAILURE;
        }
    }

    Ok(status)
}
