//! Core coordinator state and public API.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::l1_submit::L1Submitter;
use crate::proof_types::ProofData;

use compose_spec::{ChainId, PeriodId, SequenceNumber, SuperblockNumber, XtRequest};
use compose_spec_sbcp::generate_instance_id;
use prost::Message;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use publisher_metrics::PublisherMetrics;
use publisher_transport::server::QuicServer;

const MAX_ACTIVE_XTS: usize = 100;

#[derive(Debug)]
pub(crate) struct ActiveXt {
    received_at: Instant,
    chains: Vec<ChainId>,
    votes: HashMap<ChainId, bool>,
    instance_id_bytes: Vec<u8>,
    start_time: Instant,
}

#[derive(Debug)]
pub(crate) struct PendingConfirmation {
    received_at: Instant,
    chains: Vec<ChainId>,
    confirmed_chains: HashSet<ChainId>,
    #[allow(dead_code)]
    instance_id_bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
struct ChainProof {
    #[allow(dead_code)]
    superblock_number: u64,
    data: ProofData,
}

#[derive(Debug)]
pub(crate) struct CoordinatorState {
    pub chain_to_client: HashMap<ChainId, String>,
    pub client_to_chain: HashMap<String, ChainId>,
    pub active_xts: HashMap<String, ActiveXt>,
    pub current_period_id: PeriodId,
    pub next_sequence_num: SequenceNumber,
    pub next_superblock_number: SuperblockNumber,
    pub last_finalized_superblock_number: u64,
    pub last_finalized_superblock_hash: Vec<u8>,
    pub current_period_committed_xts: u64,
    /// TODO: Replace with per-superblock-number keyed collection once op-succinct
    /// sends the publisher's global superblock number instead of chain-local `end_block`.
    /// Currently collects the latest proof from each chain regardless of `superblock_number`.
    pending_proofs: HashMap<u64, ChainProof>,
    proof_collection_started: Option<Instant>,
    pending_confirmations: HashMap<String, PendingConfirmation>,
}

impl CoordinatorState {
    fn new() -> Self {
        Self {
            chain_to_client: HashMap::new(),
            client_to_chain: HashMap::new(),
            active_xts: HashMap::new(),
            current_period_id: PeriodId(0),
            next_sequence_num: SequenceNumber(1),
            next_superblock_number: SuperblockNumber::new(1),
            last_finalized_superblock_number: 0,
            last_finalized_superblock_hash: Vec::new(),
            current_period_committed_xts: 0,
            pending_proofs: HashMap::new(),
            proof_collection_started: None,
            pending_confirmations: HashMap::new(),
        }
    }

    fn register_chain(&mut self, client_id: &str, chain_id: ChainId) {
        if let Some(old_chain) = self.client_to_chain.get(client_id) {
            self.chain_to_client.remove(old_chain);
        }
        self.chain_to_client.insert(chain_id, client_id.to_string());
        self.client_to_chain.insert(client_id.to_string(), chain_id);
    }

    fn check_decision(&self, xt_id: &str) -> Option<bool> {
        let xt = self.active_xts.get(xt_id)?;
        if xt.votes.len() < xt.chains.len() {
            return None;
        }
        Some(xt.votes.values().all(|&v| v))
    }

    fn prepare_xt(
        &mut self,
        xt_req: &compose_spec_proto::XtRequest,
        chains: &[ChainId],
        received_at: Instant
    ) -> (String, Vec<u8>) {
        let compose_req = proto_to_spec_xt(xt_req);
        let seq_num = self.next_sequence_num;
        let period_id = self.current_period_id;
        let instance_id = generate_instance_id(period_id, seq_num, &compose_req);
        let xt_id = instance_id.to_string();
        self.next_sequence_num = SequenceNumber(seq_num.get() + 1);

        self.active_xts.insert(
            xt_id.clone(),
            ActiveXt {
                received_at,
                chains: chains.to_vec(),
                votes: HashMap::new(),
                instance_id_bytes: instance_id.as_bytes().to_vec(),
                start_time: Instant::now(),
            },
        );

        let msg = compose_spec_proto::Message {
            sender_id: "publisher".into(),
            payload: Some(compose_spec_proto::Payload::StartInstance(
                compose_spec_proto::StartInstance {
                    instance_id: instance_id.as_bytes().to_vec(),
                    period_id: period_id.get(),
                    sequence_number: seq_num.get(),
                    xt_request: Some(xt_req.clone()),
                },
            )),
        };

        info!(
            xt_id,
            period_id = %period_id,
            seq_num = %seq_num,
            chains = chains.len(),
            "XT prepared"
        );

        (xt_id, msg.encode_to_vec())
    }

    fn record_vote(
        &mut self,
        xt_id: &str,
        instance_id_bytes: &[u8],
        chain_id: ChainId,
        vote: bool,
    ) -> Option<(bool, f64, Vec<u8>)> {
        let xt = self.active_xts.get_mut(xt_id)?;

        if !xt.chains.contains(&chain_id) {
            warn!(xt_id, chain_id = %chain_id, "Ignoring vote from non-participant chain");
            return None;
        }

        if xt.votes.contains_key(&chain_id) {
            warn!(xt_id, chain_id = %chain_id, "Ignoring duplicate vote");
            return None;
        }

        xt.votes.insert(chain_id, vote);

        let decision = if !vote {
            Some(false)
        } else {
            self.check_decision(xt_id)
        };

        let decision = decision?;

        if decision {
            self.current_period_committed_xts += 1;
        }

        let latency = self
            .active_xts
            .get(xt_id)
            .map(|x| x.start_time.elapsed().as_secs_f64())
            .unwrap_or(0.0);

        // Move tracking data before removal
        if let Some(xt) = self.active_xts.get(xt_id) {
            self.pending_confirmations.insert(xt_id.to_string(), PendingConfirmation {
                received_at: xt.received_at,  // placeholder until received_at is added
                chains: xt.chains.clone(),
                confirmed_chains: HashSet::new(),
                instance_id_bytes: xt.instance_id_bytes.clone(),
            });
        }

        self.active_xts.remove(xt_id);

        let msg = compose_spec_proto::Message {
            sender_id: "publisher".into(),
            payload: Some(compose_spec_proto::Payload::Decided(
                compose_spec_proto::Decided {
                    instance_id: instance_id_bytes.to_vec(),
                    decision,
                },
            )),
        };

        Some((decision, latency, msg.encode_to_vec()))
    }

    fn handle_confirmed(&mut self, xt_id: &str, chain_id: ChainId) -> Option<f64> {
        let pc = self.pending_confirmations.get_mut(xt_id)?;

        if !pc.chains.contains(&chain_id) {
            warn!(xt_id, chain_id = %chain_id, "Confirmed from non-participant chain");
            return None;
        }

        if !pc.confirmed_chains.insert(chain_id) {
            return None;
        }

        if pc.confirmed_chains.len() == pc.chains.len() {
            let latency = pc.received_at.elapsed().as_secs_f64();
            self.pending_confirmations.remove(xt_id);
            Some(latency)
        } else {
            info!(xt_id, chain_id = %chain_id, "Confirmed from participant chain");
            None
        }
    }

    /// Finds timed-out xTs and produces `Decided(false)` messages for each.
    fn reap_timed_out(&mut self, timeout: Duration) -> Vec<(String, Vec<u8>)> {
        let now = Instant::now();
        let expired: Vec<String> = self
            .active_xts
            .iter()
            .filter(|(_, xt)| now.duration_since(xt.start_time) >= timeout)
            .map(|(id, _)| id.clone())
            .collect();

        let mut results = Vec::with_capacity(expired.len());
        for xt_id in expired {
            let instance_id_bytes = self
                .active_xts
                .get(&xt_id)
                .map(|xt| xt.instance_id_bytes.clone())
                .unwrap_or_default();

            self.active_xts.remove(&xt_id);

            let msg = compose_spec_proto::Message {
                sender_id: "publisher".into(),
                payload: Some(compose_spec_proto::Payload::Decided(
                    compose_spec_proto::Decided {
                        instance_id: instance_id_bytes,
                        decision: false,
                    },
                )),
            };
            results.push((xt_id, msg.encode_to_vec()));
        }
        results
    }

    /// Returns true if proof collection has expired and clears collected proofs.
    fn reap_expired_proofs(&mut self, proof_window: Duration) -> bool {
        if let Some(started) = self.proof_collection_started {
            if Instant::now().duration_since(started) >= proof_window {
                self.pending_proofs.clear();
                self.current_period_committed_xts = 0;
                self.proof_collection_started = None;
                return true;
            }
        }
        false
    }

    fn is_chain_registered(&self, chain_id: ChainId) -> bool {
        self.chain_to_client.contains_key(&chain_id)
    }
}

pub struct Coordinator {
    pub(crate) state: Arc<RwLock<CoordinatorState>>,
    pub(crate) server: Arc<QuicServer>,
    pub(crate) metrics: Option<Arc<PublisherMetrics>>,
    pub(crate) l1_submitter: Option<Arc<L1Submitter>>,
    scp_timeout: Duration,
    proof_window: Duration,
    messages_processed: AtomicU64,
    broadcasts_sent: AtomicU64,
    start_time: Instant,
}

impl std::fmt::Debug for Coordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Coordinator").finish()
    }
}

impl Coordinator {
    pub fn new(
        server: Arc<QuicServer>,
        metrics: Option<Arc<PublisherMetrics>>,
        scp_timeout: Duration,
        proof_window: Duration,
    ) -> Self {
        Self {
            state: Arc::new(RwLock::new(CoordinatorState::new())),
            server,
            metrics,
            l1_submitter: None,
            scp_timeout,
            proof_window,
            messages_processed: AtomicU64::new(0),
            broadcasts_sent: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    pub fn with_l1_submitter(mut self, submitter: L1Submitter) -> Self {
        self.l1_submitter = Some(Arc::new(submitter));
        self
    }

    pub fn server(&self) -> &Arc<QuicServer> {
        &self.server
    }

    pub fn inc_messages(&self) {
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
        if let Some(m) = &self.metrics {
            m.messages_received_total.inc();
        }
    }

    fn inc_broadcasts(&self) {
        self.broadcasts_sent.fetch_add(1, Ordering::Relaxed);
        if let Some(m) = &self.metrics {
            m.broadcasts_sent_total.inc();
        }
    }

    pub async fn register_chain(&self, client_id: &str, chain_id: ChainId) {
        let mut state = self.state.write().await;
        state.register_chain(client_id, chain_id);
        info!(client_id, chain_id = %chain_id, "Chain registered");
    }

    /// Initializes superblock state from L1 on startup — must be called before
    /// the period loop starts so `next_superblock_number` and `parent_hash` are
    /// correct after a restart.
    pub async fn init_from_l1(&self) {
        if let Some(submitter) = &self.l1_submitter {
            match submitter.fetch_latest_superblock_state().await {
                Ok(Some((sb_num, sb_hash))) => {
                    let mut state = self.state.write().await;
                    state.last_finalized_superblock_number = sb_num;
                    state.last_finalized_superblock_hash = sb_hash.to_vec();
                    state.next_superblock_number = SuperblockNumber::new(sb_num + 1);
                    info!(
                        last_finalized = sb_num,
                        next = sb_num + 1,
                        "Initialized superblock state from L1"
                    );
                }
                Ok(None) => {
                    info!("No superblocks on L1 yet — starting from genesis");
                }
                Err(e) => {
                    warn!(error = %e, "Failed to read L1 superblock state — starting from genesis");
                }
            }
        }
    }

    pub async fn advance_period(&self) -> Result<(), publisher_transport::error::TransportError> {
        let (period_id, superblock_num) = {
            let mut state = self.state.write().await;
            let pid = PeriodId(state.current_period_id.get() + 1);
            state.current_period_id = pid;
            state.next_sequence_num = SequenceNumber(1);
            if let Some(m) = &self.metrics {
                m.xt_finalised_per_period.set(state.current_period_committed_xts as i64);
            }
            state.current_period_committed_xts = 0;
            let sb = state.next_superblock_number;
            (pid, sb)
        };

        let msg = compose_spec_proto::Message {
            sender_id: "publisher".into(),
            payload: Some(compose_spec_proto::Payload::StartPeriod(
                compose_spec_proto::StartPeriod {
                    period_id: period_id.get(),
                    superblock_number: superblock_num.get(),
                },
            )),
        };
        let data = msg.encode_to_vec();

        info!(period_id = %period_id, superblock_num = %superblock_num, "Broadcasting period");
        self.inc_broadcasts();
        if let Some(m) = &self.metrics {
            m.period_broadcast_total.inc();
        }
        self.server.broadcast_raw(&data, "").await
    }

    pub(crate) async fn handle_xt_request(
        &self,
        client_id: String,
        xt_req: compose_spec_proto::XtRequest,
    ) {
        let chains = extract_chains(&xt_req);

        if chains.len() < 2 {
            warn!(
                client_id,
                chains = chains.len(),
                "Rejecting XT: must span at least 2 chains"
            );
            return;
        }

        let received_at = Instant::now();

        let broadcast = {
            let mut state = self.state.write().await;

            if state.active_xts.len() >= MAX_ACTIVE_XTS {
                warn!(client_id, "Active XT limit reached, rejecting new transaction");
                return;
            }

            Some(state.prepare_xt(&xt_req, &chains, received_at))
        };

        if let Some((_xt_id, data)) = broadcast {
            self.inc_broadcasts();
            if let Some(m) = &self.metrics {
                m.xt_started_total.inc();
            }
            if let Err(e) = self.server.broadcast_raw(&data, "").await {
                error!(error = %e, "Failed to broadcast XT start");
            }
        }
    }

    pub(crate) async fn handle_vote(
        &self,
        _client_id: &str,
        instance_id_bytes: &[u8],
        chain_id: ChainId,
        vote: bool,
    ) {
        let xt_id = hex::encode(instance_id_bytes);
        info!(xt_id, chain_id = %chain_id, vote, "Vote received");

        let result = {
            let mut state = self.state.write().await;
            state.record_vote(&xt_id, instance_id_bytes, chain_id, vote)
        };

        if let Some((decision, latency, data)) = result {
            info!(
                xt_id,
                decision,
                latency_ms = (latency * 1000.0) as u64,
                "Decision reached"
            );
            if let Some(m) = &self.metrics {
                if decision {
                    m.xt_decided_commit_total.inc();
                } else {
                    m.xt_decided_abort_total.inc();
                }
                m.xt_decision_latency_seconds.observe(latency);
            }

            self.inc_broadcasts();
            if let Err(e) = self.server.broadcast_raw(&data, "").await {
                error!(xt_id, error = %e, "Failed to broadcast decision");
            }
        }
    }

    pub(crate) async fn handle_mailbox_relay(&self, mailbox: &compose_spec_proto::MailboxMessage) {
        let dest_chain = ChainId::new(mailbox.destination_chain);

        let client_id = {
            let state = self.state.read().await;
            state.chain_to_client.get(&dest_chain).cloned()
        };

        let Some(client_id) = client_id else {
            warn!(dest_chain = %dest_chain, "No sidecar for destination chain");
            return;
        };

        let msg = compose_spec_proto::Message {
            sender_id: "publisher".into(),
            payload: Some(compose_spec_proto::Payload::MailboxMessage(mailbox.clone())),
        };
        let data = msg.encode_to_vec();
        self.inc_broadcasts();

        if let Err(e) = self.server.send_raw(&client_id, &data).await {
            warn!(client_id, error = %e, "Failed to relay mailbox");
        }
    }

    pub(crate) async fn handle_ping(&self, client_id: &str, timestamp: i64) {
        let msg = compose_spec_proto::Message {
            sender_id: "publisher".into(),
            payload: Some(compose_spec_proto::Payload::Pong(
                compose_spec_proto::Pong { timestamp },
            )),
        };
        let data = msg.encode_to_vec();
        if let Err(e) = self.server.send_raw(client_id, &data).await {
            warn!(client_id, error = %e, "Failed to send pong");
        }
    }

    pub async fn reap_timed_out_xts(&self) {
        let timed_out = {
            let mut state = self.state.write().await;
            state.reap_timed_out(self.scp_timeout)
        };

        for (xt_id, data) in &timed_out {
            warn!(xt_id, "SCP timeout — deciding false");
            if let Some(m) = &self.metrics {
                m.xt_decided_abort_total.inc();
            }
            self.inc_broadcasts();
            if let Err(e) = self.server.broadcast_raw(data, "").await {
                error!(xt_id, error = %e, "Failed to broadcast timeout decision");
            }
        }
    }

    pub async fn reap_expired_proofs(&self) {
        let expired = {
            let mut state = self.state.write().await;
            state.reap_expired_proofs(self.proof_window)
        };

        if expired {
            warn!("Proof window expired — triggering rollback");

            let (period_id, last_sb_num, last_sb_hash) = {
                let mut state = self.state.write().await;
                state.next_superblock_number =
                    SuperblockNumber::new(state.last_finalized_superblock_number + 1);
                (
                    state.current_period_id.get(),
                    state.last_finalized_superblock_number,
                    state.last_finalized_superblock_hash.clone(),
                )
            };

            let msg = compose_spec_proto::Message {
                sender_id: "publisher".into(),
                payload: Some(compose_spec_proto::Payload::Rollback(
                    compose_spec_proto::Rollback {
                        period_id,
                        last_finalized_superblock_number: last_sb_num,
                        last_finalized_superblock_hash: last_sb_hash,
                    },
                )),
            };
            let data = msg.encode_to_vec();

            info!(period_id, last_sb_num, "Broadcasting rollback");
            self.inc_broadcasts();
            if let Err(e) = self.server.broadcast_raw(&data, "").await {
                error!(error = %e, "Failed to broadcast rollback");
            }
        }
    }

    pub async fn handle_confirmed(&self, instance_id: &[u8], chain_id: ChainId) {
        let xt_id = hex::encode(instance_id);
        let latency = {
            let mut state = self.state.write().await;
            state.handle_confirmed(&xt_id, chain_id)
        };

        if let Some(latency) = latency {
            info!(xt_id, latency_ms = (latency * 1000.0) as u64, "All chains confirmed block inclusion");
            if let Some(m) = &self.metrics {
                m.xt_block_inclusion_latency_seconds.observe(latency);
            }
        }
    }

    pub async fn is_chain_registered(&self, chain_id: ChainId) -> bool {
        let state = self.state.read().await;
        state.is_chain_registered(chain_id)
    }

    pub async fn chain_for_client(&self, client_id: &str) -> Option<ChainId> {
        let state = self.state.read().await;
        state.client_to_chain.get(client_id).copied()
    }

    pub async fn current_superblock_number(&self) -> u64 {
        let state = self.state.read().await;
        state.next_superblock_number.get()
    }

    /// TODO: This ignores `superblock_number` matching — it collects the latest proof from
    /// each chain and submits once all chains report. Fix once op-succinct sends the
    /// publisher's global superblock number instead of chain-local `end_block`.
    pub async fn receive_proof(&self, superblock_number: u64, chain_id: u64, data: ProofData) {
        let (collected, total, ready_proofs, submit_sb_number) = {
            let mut state = self.state.write().await;
            let total = state.chain_to_client.len();

            if state.pending_proofs.contains_key(&chain_id) {
                warn!(
                    chain_id,
                    superblock_number, "Replacing existing proof for chain"
                );
            }

            if state.proof_collection_started.is_none() {
                state.proof_collection_started = Some(Instant::now());
            }

            state.pending_proofs.insert(
                chain_id,
                ChainProof {
                    superblock_number,
                    data,
                },
            );
            let collected = state.pending_proofs.len();

            if total > 0 && collected >= total {
                let proofs: HashMap<u64, ProofData> = state
                    .pending_proofs
                    .drain()
                    .map(|(cid, cp)| (cid, cp.data))
                    .collect();
                let sb = state.next_superblock_number.get();
                state.proof_collection_started = None;
                (collected, total, Some(proofs), sb)
            } else {
                (collected, total, None, 0)
            }
        };

        if let Some(proofs) = ready_proofs {
            info!(
                superblock_number = submit_sb_number,
                collected, "All chains submitted proofs"
            );

            if let Err(e) = validate_mailbox_consistency(&proofs) {
                error!(superblock_number = submit_sb_number, error = %e, "Mailbox consistency check failed");
                return;
            }

            if let Some(submitter) = self.l1_submitter.clone() {
                let state = self.state.clone();
                tokio::spawn(async move {
                    match submitter.submit(submit_sb_number, &proofs).await {
                        Ok(()) => {
                            let mut s = state.write().await;
                            s.last_finalized_superblock_number = submit_sb_number;
                            s.next_superblock_number = SuperblockNumber::new(submit_sb_number + 1);
                            info!(
                                superblock_number = submit_sb_number,
                                "L1 submission succeeded, advancing state"
                            );
                        }
                        Err(e) => {
                            warn!(superblock_number = submit_sb_number, error = %e, "L1 submission failed");
                        }
                    }
                });
            }
        } else {
            info!(
                superblock_number,
                chain_id, collected, total, "Proof received"
            );
        }
    }

    pub async fn stats(&self) -> serde_json::Value {
        let state = self.state.read().await;
        serde_json::json!({
            "active_connections": self.server.connection_count().await,
            "registered_chains": state.chain_to_client.len(),
            "active_2pc_transactions": state.active_xts.len(),
            "pending_proof_superblocks": state.pending_proofs.len(),
            "current_period_id": state.current_period_id.get(),
            "next_superblock_number": state.next_superblock_number.get(),
            "last_finalized_superblock": state.last_finalized_superblock_number,
            "messages_processed": self.messages_processed.load(Ordering::Relaxed),
            "broadcasts_sent": self.broadcasts_sent.load(Ordering::Relaxed),
            "uptime_seconds": self.start_time.elapsed().as_secs_f64(),
        })
    }
}

fn validate_mailbox_consistency(proofs: &HashMap<u64, ProofData>) -> Result<(), String> {
    for (&chain_i, proof_i) in proofs {
        let mi = &proof_i.mailbox_info;
        for (idx, inbox_chain) in mi.inbox_chains.iter().enumerate() {
            let inbox_root = mi
                .inbox_roots
                .get(idx)
                .ok_or_else(|| format!("chain {chain_i}: inbox_roots shorter than inbox_chains"))?;

            // Find the counterparty chain whose outbox to chain_i should match.
            let counterparty_proof = proofs.values().find(|p| {
                p.mailbox_info
                    .outbox_chains
                    .iter()
                    .any(|oc| oc == inbox_chain)
            });

            if let Some(cp) = counterparty_proof {
                let cp_mi = &cp.mailbox_info;
                if let Some(outbox_idx) =
                    cp_mi.outbox_chains.iter().position(|oc| oc == inbox_chain)
                {
                    if let Some(outbox_root) = cp_mi.outbox_roots.get(outbox_idx) {
                        if inbox_root != outbox_root {
                            return Err(format!(
                                "Mailbox mismatch: chain {chain_i} inbox from {inbox_chain} != counterparty outbox"
                            ));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn extract_chains(req: &compose_spec_proto::XtRequest) -> Vec<ChainId> {
    let mut seen = std::collections::HashSet::new();
    let mut chains = Vec::new();
    for tr in &req.transaction_requests {
        let cid = ChainId::new(tr.chain_id);
        if seen.insert(cid) {
            chains.push(cid);
        }
    }
    chains
}

fn proto_to_spec_xt(req: &compose_spec_proto::XtRequest) -> XtRequest {
    XtRequest {
        transactions: req
            .transaction_requests
            .iter()
            .map(|tr| compose_spec::TransactionRequest {
                chain_id: ChainId::new(tr.chain_id),
                transactions: tr.transaction.clone(),
            })
            .collect(),
    }
}
