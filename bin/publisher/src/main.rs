//! Ethera Shared Publisher.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use prometheus_client::registry::Registry;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use publisher_config::{Cli, Config};
use publisher_coordinator::coordinator::Coordinator;
use publisher_coordinator::handlers::{self, parse_chain_id};
use publisher_coordinator::l1_submit::L1Submitter;
use publisher_metrics::PublisherMetrics;
use publisher_server::router::build_router;
use publisher_server::state::AppState;
use publisher_transport::server::QuicServer;

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("failed to install rustls crypto provider"))?;

    let cli = Cli::parse();
    let cfg = Config::load(&cli.config)?;

    publisher_tracing::init(&cfg.log.level, cfg.log.pretty);
    info!("Starting Ethera Shared Publisher");
    if cfg.mock_mode {
        warn!(
            "MOCK_MODE enabled — expecting fabricated proofs from sidecars \
             and a MockVerifier on L1; no real ZK proof is ever checked"
        );
    }

    let mut registry = Registry::default();
    let metrics = if cfg.metrics.enabled {
        Some(Arc::new(PublisherMetrics::new(&mut registry)))
    } else {
        None
    };

    let server = Arc::new(QuicServer::new(
        cfg.server.listen_addr.clone(),
        cfg.server.max_message_size,
    ));

    let s = &cfg.settlement;
    let l1_submitter = if !s.l1_rpc_url.is_empty()
        && !s.l2oo_address.is_empty()
        && !s.proposer_key.is_empty()
    {
        match L1Submitter::new(
            &s.l2oo_address,
            s.l1_rpc_url.clone(),
            s.proposer_key.clone(),
        ) {
            Ok(sub) => {
                info!("L1 submitter configured for {}", s.l2oo_address);
                Some(sub)
            }
            Err(e) => {
                error!(error = %e, "Failed to create L1 submitter — running without L1 settlement");
                None
            }
        }
    } else {
        info!("No settlement config — running without L1 settlement");
        None
    };

    let coordinator_builder = Coordinator::new(
        server.clone(),
        metrics.clone(),
        cfg.consensus.timeout,
        cfg.consensus.proof_window,
    );
    let coordinator = Arc::new(if let Some(sub) = l1_submitter {
        coordinator_builder.with_l1_submitter(sub)
    } else {
        coordinator_builder
    });

    coordinator.init_from_l1().await;

    let coord_for_handler = coordinator.clone();
    let on_message = Arc::new(move |client_id: String, data: Vec<u8>| {
        let coord = coord_for_handler.clone();
        tokio::spawn(async move {
            handlers::dispatch(coord, client_id, data).await;
        });
    });

    let coord_for_connect = coordinator.clone();
    let metrics_connect = metrics.clone();
    let on_connect = Arc::new(move |client_id: String| {
        if let Some(m) = &metrics_connect {
            m.connections_active.inc();
        }

        let coord = coord_for_connect.clone();
        tokio::spawn(async move {
            match parse_chain_id(&client_id) {
                Ok(chain_id) => coord.register_chain(&client_id, chain_id).await,
                Err(e) => {
                    warn!(client_id, error = %e, "Ignoring connection with unparseable chain ID");
                }
            }
        });
    });

    let metrics_disconnect = metrics.clone();
    let on_disconnect = Arc::new(move |_client_id: String| {
        if let Some(m) = &metrics_disconnect {
            m.connections_active.dec();
        }
    });

    let _quic_handle = server.start(on_message, Some(on_connect), Some(on_disconnect))?;

    let coord_for_period = coordinator.clone();
    let period_duration = cfg.consensus.period_duration;
    tokio::spawn(async move { period_loop(coord_for_period, period_duration).await });

    let coord_for_reaper = coordinator.clone();
    tokio::spawn(async move { reaper_loop(coord_for_reaper).await });

    let state = if cfg.metrics.enabled {
        AppState::new(coordinator.clone()).with_registry(registry)
    } else {
        AppState::new(coordinator.clone())
    };
    let router = build_router(state, cfg.api.request_timeout);
    let listener = TcpListener::bind(&cfg.api.listen_addr).await?;
    info!(addr = %cfg.api.listen_addr, "HTTP API listening");

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("Shutting down");
    coordinator.server().close();
    Ok(())
}

async fn period_loop(coordinator: Arc<Coordinator>, period_duration: Duration) {
    let mut interval = tokio::time::interval(period_duration);

    loop {
        interval.tick().await;
        if let Err(e) = coordinator.advance_period().await {
            error!(error = %e, "Failed to broadcast period");
        }
    }
}

/// Runs periodic cleanup: times out stale 2PC instances and triggers rollback
/// for proof sets that exceed the proof window.
async fn reaper_loop(coordinator: Arc<Coordinator>) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        interval.tick().await;
        coordinator.reap_timed_out_xts().await;
        coordinator.reap_expired_proofs().await;
    }
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C handler");
    info!("Received shutdown signal");
}
