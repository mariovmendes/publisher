//! Prometheus metrics for the shared publisher.

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

#[derive(Debug)]
pub struct PublisherMetrics {
    pub connections_active: Gauge,
    pub messages_received_total: Counter<u64>,
    pub broadcasts_sent_total: Counter<u64>,
    pub xt_started_total: Counter<u64>,
    pub xt_decided_commit_total: Counter<u64>,
    pub xt_decided_abort_total: Counter<u64>,
    pub xt_decision_latency_seconds: Histogram,
    pub xt_finalised_per_period: Gauge,
    pub xt_block_inclusion_latency_seconds: Histogram,
    pub period_broadcast_total: Counter<u64>,
}

impl PublisherMetrics {
    pub fn new(registry: &mut Registry) -> Self {
        let connections_active = Gauge::default();
        registry.register(
            "publisher_connections_active",
            "Active sidecar connections",
            connections_active.clone(),
        );

        let messages_received_total = Counter::default();
        registry.register(
            "publisher_messages_received",
            "Inbound messages from sidecars",
            messages_received_total.clone(),
        );

        let broadcasts_sent_total = Counter::default();
        registry.register(
            "publisher_broadcasts_sent",
            "Broadcast messages sent to sidecars",
            broadcasts_sent_total.clone(),
        );

        let xt_started_total = Counter::default();
        registry.register(
            "publisher_xt_started",
            "Cross-chain transactions started",
            xt_started_total.clone(),
        );

        let xt_decided_commit_total = Counter::default();
        registry.register(
            "publisher_xt_decided_commit",
            "Cross-chain transactions committed",
            xt_decided_commit_total.clone(),
        );

        let xt_decided_abort_total = Counter::default();
        registry.register(
            "publisher_xt_decided_abort",
            "Cross-chain transactions aborted",
            xt_decided_abort_total.clone(),
        );

        let xt_finalised_per_period = Gauge::default();
        registry.register(
            "publisher_xt_finalized_per_period",
            "Cross-chain transactions finalized in the last period (L1-confirmed)",
            xt_finalised_per_period.clone(),
        );

        let xt_decision_latency_seconds =
            Histogram::new([0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0].into_iter());
        registry.register(
            "publisher_xt_decision_latency_seconds",
            "Time from XT start to decision",
            xt_decision_latency_seconds.clone(),
        );

        let xt_block_inclusion_latency_seconds =
            Histogram::new([1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0, 3.25, 3.5, 3.75, 4.0, 4.25, 4.5, 4.75, 5.0].into_iter());
        registry.register(
            "publisher_xt_block_inclusion_latency_seconds",
            "Time from XT submission to all chains confirming block inclusion",
            xt_block_inclusion_latency_seconds.clone(),
        );

        let period_broadcast_total = Counter::default();
        registry.register(
            "publisher_period_broadcast",
            "Period broadcasts sent",
            period_broadcast_total.clone(),
        );

        Self {
            connections_active,
            messages_received_total,
            broadcasts_sent_total,
            xt_started_total,
            xt_decided_commit_total,
            xt_decided_abort_total,
            xt_decision_latency_seconds,
            xt_finalised_per_period,
            xt_block_inclusion_latency_seconds,
            period_broadcast_total,
        }
    }
}
