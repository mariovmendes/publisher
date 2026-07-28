use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
};

use compose_spec::{
    chains_from_request, ChainId, Instance, PeriodId, SequenceNumber, SuperblockHash,
    SuperblockNumber, XtRequest,
};
use thiserror::Error;
use tracing::{error, info, warn};

use crate::id::generate_instance_id;

/// Errors returned by [`Publisher`] operations.
#[derive(Debug, Error)]
pub enum PublisherError {
    #[error("can not start period: target superblock is {target}, expected {expected}")]
    CannotStartPeriod { target: u64, expected: u64 },
    #[error("can not advance to older settled state")]
    OldSettledState,
    #[error("invalid request")]
    InvalidRequest,
    #[error("target superblock is less than the last finalized one")]
    InvalidInitialState,
}

/// Generates and aggregates ZK proofs for a superblock.
pub trait PublisherProver: Send + Sync {
    fn request_superblock_proof(
        &self,
        superblock_number: SuperblockNumber,
        last_superblock_hash: SuperblockHash,
        proofs: Vec<Vec<u8>>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Broadcasts protocol messages to connected sequencers.
pub trait PublisherMessenger: Send + Sync {
    fn broadcast_start_period(
        &self,
        period_id: PeriodId,
        target_superblock_number: SuperblockNumber,
    );
    fn broadcast_rollback(
        &self,
        period_id: PeriodId,
        superblock_number: SuperblockNumber,
        superblock_hash: SuperblockHash,
    );
}

/// Publishes proofs to L1.
pub trait L1Publisher: Send + Sync {
    fn publish_proof(&self, superblock_number: SuperblockNumber, proof: Vec<u8>);
}

#[allow(dead_code)]
struct PublisherState {
    period_id: PeriodId,
    target_superblock_number: SuperblockNumber,
    last_finalized_superblock_number: SuperblockNumber,
    last_finalized_superblock_hash: SuperblockHash,
    proofs: HashMap<SuperblockNumber, HashMap<ChainId, Vec<u8>>>,
    chains: HashSet<ChainId>,
    sequence_number: SequenceNumber,
    proof_window: u64,
}

/// SBCP publisher coordinator managing periods, instances, and proof aggregation.
pub struct Publisher<P: PublisherProver, M: PublisherMessenger, L: L1Publisher> {
    inner: Mutex<PublisherState>,
    prover: P,
    messenger: M,
    l1: L,
}

impl<P: PublisherProver, M: PublisherMessenger, L: L1Publisher> std::fmt::Debug
    for Publisher<P, M, L>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher").finish_non_exhaustive()
    }
}

impl<P: PublisherProver, M: PublisherMessenger, L: L1Publisher> Publisher<P, M, L> {
    /// Creates a new Publisher. Pass the *previous* period ID and target superblock number.
    /// Call `start_period()` to begin the first period.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        prover: P,
        messenger: M,
        l1: L,
        previous_period_id: PeriodId,
        previous_target_superblock_number: SuperblockNumber,
        last_finalized_superblock_number: SuperblockNumber,
        last_finalized_superblock_hash: SuperblockHash,
        proof_window: u64,
        chains: HashSet<ChainId>,
    ) -> Result<Self, PublisherError> {
        if previous_target_superblock_number < last_finalized_superblock_number {
            return Err(PublisherError::InvalidInitialState);
        }

        Ok(Self {
            inner: Mutex::new(PublisherState {
                period_id: previous_period_id,
                target_superblock_number: previous_target_superblock_number,
                last_finalized_superblock_number,
                last_finalized_superblock_hash,
                proofs: HashMap::new(),
                chains,
                sequence_number: SequenceNumber(0),
                proof_window,
            }),
            prover,
            messenger,
            l1,
        })
    }

    /// Starts a new period. Resets the sequence number and broadcasts `StartPeriod`.
    pub fn start_period(&self) -> Result<(), PublisherError> {
        let mut state = self.inner.lock().unwrap();

        let next_superblock = state.target_superblock_number + 1;

        // Proof window constraint
        if state.proof_window != 0 {
            let limit =
                state.last_finalized_superblock_number + SuperblockNumber(1 + state.proof_window);
            if next_superblock > limit {
                return Err(PublisherError::CannotStartPeriod {
                    target: state.target_superblock_number.get(),
                    expected: (state.last_finalized_superblock_number + 1).get(),
                });
            }
        }

        state.period_id = state.period_id + 1;
        state.target_superblock_number = next_superblock;

        info!(
            new_period_id = state.period_id.get(),
            target_superblock_number = state.target_superblock_number.get(),
            "Starting new period"
        );

        self.messenger
            .broadcast_start_period(state.period_id, state.target_superblock_number);

        state.sequence_number = SequenceNumber(0);
        Ok(())
    }

    /// Receives a proof from a sequencer. Aggregates and publishes when all proofs are collected.
    pub fn receive_proof(
        &self,
        period_id: PeriodId,
        superblock_number: SuperblockNumber,
        proof: Vec<u8>,
        chain_id: ChainId,
    ) {
        let mut state = self.inner.lock().unwrap();

        if superblock_number <= state.last_finalized_superblock_number {
            warn!(
                superblock_number = superblock_number.get(),
                chain_id = chain_id.get(),
                "Received proof for old superblock, ignoring"
            );
            return;
        }

        if superblock_number >= state.target_superblock_number {
            warn!(
                superblock_number = superblock_number.get(),
                chain_id = chain_id.get(),
                "Received proof for non-terminated superblock, ignoring"
            );
            return;
        }

        if superblock_number != state.last_finalized_superblock_number + 1 {
            warn!(
                superblock_number = superblock_number.get(),
                chain_id = chain_id.get(),
                "Received proof for superblock that is not the next one, ignoring"
            );
            return;
        }

        // Check period is correct
        let period_diff = state.target_superblock_number - superblock_number;
        let expected_period = state.period_id - PeriodId(period_diff.get());
        if period_id != expected_period {
            warn!(
                superblock_number = superblock_number.get(),
                chain_id = chain_id.get(),
                expected_period = expected_period.get(),
                received_period = period_id.get(),
                "Received proof for wrong period, ignoring"
            );
            return;
        }

        // Duplicate check
        let sb_proofs = state.proofs.entry(superblock_number).or_default();
        if sb_proofs.contains_key(&chain_id) {
            warn!(
                superblock_number = superblock_number.get(),
                chain_id = chain_id.get(),
                "Already received proof, ignoring"
            );
            return;
        }

        sb_proofs.insert(chain_id, proof);

        let total_chains = state.chains.len();
        let received_proofs = state.proofs[&superblock_number].len();

        if received_proofs < total_chains {
            info!(
                superblock_number = superblock_number.get(),
                chain_id = chain_id.get(),
                received_proofs,
                total_chains,
                "Received proof, waiting for more"
            );
            return;
        }

        info!(
            superblock_number = superblock_number.get(),
            chain_id = chain_id.get(),
            "Received enough proofs, generating proof"
        );

        let seq_proofs: Vec<Vec<u8>> = state.proofs[&superblock_number].values().cloned().collect();
        let last_superblock_hash = state.last_finalized_superblock_hash;
        drop(state);

        match self.prover.request_superblock_proof(
            superblock_number,
            last_superblock_hash,
            seq_proofs,
        ) {
            Ok(network_proof) => {
                let mut state = self.inner.lock().unwrap();
                state.proofs.remove(&superblock_number);
                drop(state);
                self.l1.publish_proof(superblock_number, network_proof);
            }
            Err(e) => {
                error!(
                    err = %e,
                    superblock_number = superblock_number.get(),
                    chain_id = chain_id.get(),
                    "Failed to generate network proof. Triggering rollback"
                );
                self.rollback();
            }
        }
    }

    /// Starts a new SCP instance for the given cross-chain transaction request.
    pub fn start_instance(&self, request: XtRequest) -> Result<Instance, PublisherError> {
        let mut state = self.inner.lock().unwrap();

        if request.transactions.len() < 2 {
            return Err(PublisherError::InvalidRequest);
        }

        let chains = chains_from_request(&request);

        state.sequence_number = state.sequence_number + 1;
        let instance = Instance {
            id: generate_instance_id(state.period_id, state.sequence_number, &request),
            period_id: state.period_id,
            sequence_number: state.sequence_number,
            xt_request: request,
        };

        info!(
            instance_id = %instance.id,
            period_id = instance.period_id.get(),
            sequence_number = instance.sequence_number.get(),
            "Starting new instance"
        );

        Ok(instance)
    }

    /// Advances the settled state when L1 emits a new settled event.
    pub fn advance_settled_state(
        &self,
        superblock_number: SuperblockNumber,
        superblock_hash: SuperblockHash,
    ) -> Result<(), PublisherError> {
        let mut state = self.inner.lock().unwrap();

        if superblock_number <= state.last_finalized_superblock_number {
            return Err(PublisherError::OldSettledState);
        }

        info!(
            new_finalized_superblock_number = superblock_number.get(),
            "Advancing finalized settled state"
        );

        state.last_finalized_superblock_number = superblock_number;
        state.last_finalized_superblock_hash = superblock_hash;
        Ok(())
    }

    /// Triggers a rollback to the last finalized superblock.
    pub fn proof_timeout(&self) {
        info!("Proof timeout occurred, rolling back to last finalized superblock");
        self.rollback();
    }

    fn rollback(&self) {
        let mut state = self.inner.lock().unwrap();
        state.sequence_number = SequenceNumber(0);
        state.target_superblock_number = state.last_finalized_superblock_number + 1;
        self.messenger.broadcast_rollback(
            state.period_id,
            state.last_finalized_superblock_number,
            state.last_finalized_superblock_hash,
        );
        state.proofs.clear();
    }

    /// Access the internal target superblock number (for testing).
    #[must_use]
    pub fn target_superblock_number(&self) -> SuperblockNumber {
        self.inner.lock().unwrap().target_superblock_number
    }

    /// Access the proofs map for a given superblock (for testing).
    #[must_use]
    pub fn proofs_for(&self, sb: SuperblockNumber) -> Option<HashMap<ChainId, Vec<u8>>> {
        self.inner.lock().unwrap().proofs.get(&sb).cloned()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[derive(Debug, Default)]
    struct FakeMessenger {
        start_periods: Mutex<Vec<(PeriodId, SuperblockNumber)>>,
        rollbacks: Mutex<Vec<(PeriodId, SuperblockNumber, SuperblockHash)>>,
    }

    impl PublisherMessenger for Arc<FakeMessenger> {
        fn broadcast_start_period(&self, p: PeriodId, t: SuperblockNumber) {
            self.start_periods.lock().unwrap().push((p, t));
        }
        fn broadcast_rollback(&self, p: PeriodId, s: SuperblockNumber, h: SuperblockHash) {
            self.rollbacks.lock().unwrap().push((p, s, h));
        }
    }

    type ProverCall = (SuperblockNumber, SuperblockHash, Vec<Vec<u8>>);

    #[derive(Debug, Default)]
    struct FakeProver {
        calls: Mutex<Vec<ProverCall>>,
        next_proof: Mutex<Vec<u8>>,
        err: Mutex<Option<String>>,
    }

    impl PublisherProver for Arc<FakeProver> {
        fn request_superblock_proof(
            &self,
            sb: SuperblockNumber,
            hash: SuperblockHash,
            proofs: Vec<Vec<u8>>,
        ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
            self.calls.lock().unwrap().push((sb, hash, proofs));
            if let Some(ref e) = *self.err.lock().unwrap() {
                return Err(e.clone().into());
            }
            Ok(self.next_proof.lock().unwrap().clone())
        }
    }

    #[derive(Debug, Default)]
    struct FakeL1 {
        published: Mutex<Vec<(SuperblockNumber, Vec<u8>)>>,
    }

    impl L1Publisher for Arc<FakeL1> {
        fn publish_proof(&self, sb: SuperblockNumber, proof: Vec<u8>) {
            self.published.lock().unwrap().push((sb, proof));
        }
    }

    fn chain_req(chain: u64, txs: &[&[u8]]) -> compose_spec::TransactionRequest {
        compose_spec::TransactionRequest {
            chain_id: ChainId(chain),
            transactions: txs.iter().map(|t| t.to_vec()).collect(),
        }
    }

    fn make_xt_request(entries: Vec<compose_spec::TransactionRequest>) -> XtRequest {
        XtRequest {
            transactions: entries,
        }
    }

    fn make_chain_set(ids: &[u64]) -> HashSet<ChainId> {
        ids.iter().map(|&id| ChainId(id)).collect()
    }

    fn default_chain_set() -> HashSet<ChainId> {
        make_chain_set(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    }

    type TestPublisher = Publisher<Arc<FakeProver>, Arc<FakeMessenger>, Arc<FakeL1>>;

    fn new_publisher_for_test(
        period: u64,
        target: u64,
        finalized: u64,
        hash: SuperblockHash,
        window: u64,
        chains: HashSet<ChainId>,
    ) -> (
        TestPublisher,
        Arc<FakeMessenger>,
        Arc<FakeProver>,
        Arc<FakeL1>,
    ) {
        let m = Arc::new(FakeMessenger::default());
        let p = Arc::new(FakeProver::default());
        let l1 = Arc::new(FakeL1::default());
        let pub_inst = Publisher::new(
            Arc::clone(&p),
            Arc::clone(&m),
            Arc::clone(&l1),
            PeriodId(period),
            SuperblockNumber(target),
            SuperblockNumber(finalized),
            hash,
            window,
            chains,
        )
        .unwrap();
        (pub_inst, m, p, l1)
    }

    #[test]
    fn rejects_target_lower_than_finalized() {
        let result = Publisher::new(
            Arc::new(FakeProver::default()),
            Arc::new(FakeMessenger::default()),
            Arc::new(FakeL1::default()),
            PeriodId(3),
            SuperblockNumber(4),
            SuperblockNumber(5),
            SuperblockHash([1; 32]),
            0,
            default_chain_set(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn start_period_respects_explicit_target() {
        let (pub_inst, m, _, _) =
            new_publisher_for_test(4, 10, 7, SuperblockHash([5; 32]), 0, default_chain_set());

        pub_inst.start_period().unwrap();
        let starts = m.start_periods.lock().unwrap();
        assert_eq!(starts.len(), 1);
        assert_eq!(starts[0].0, PeriodId(5));
        assert_eq!(starts[0].1, SuperblockNumber(11));
        drop(starts);

        assert_eq!(pub_inst.target_superblock_number(), SuperblockNumber(11));
    }

    #[test]
    fn start_period_rejects_initial_target_past_window() {
        let (pub_inst, m, _, _) =
            new_publisher_for_test(2, 12, 5, SuperblockHash([3; 32]), 1, default_chain_set());

        let err = pub_inst.start_period().unwrap_err();
        assert!(matches!(err, PublisherError::CannotStartPeriod { .. }));
        assert!(m.start_periods.lock().unwrap().is_empty());
    }

    #[test]
    fn start_period_basic_broadcast_and_reset() {
        let (pub_inst, m, _, _) =
            new_publisher_for_test(9, 7, 7, SuperblockHash([9; 32]), 0, default_chain_set());

        pub_inst.start_period().unwrap();
        let starts = m.start_periods.lock().unwrap();
        assert_eq!(starts.len(), 1);
        assert_eq!(starts[0].0, PeriodId(10));
        assert_eq!(starts[0].1, SuperblockNumber(8));
    }

    #[test]
    fn start_period_error_when_target_exceeds_proof_window() {
        let (pub_inst, m, _, _) =
            new_publisher_for_test(5, 7, 7, SuperblockHash([1; 32]), 1, default_chain_set());

        pub_inst.start_period().unwrap();
        pub_inst.start_period().unwrap();

        let err = pub_inst.start_period().unwrap_err();
        assert!(matches!(err, PublisherError::CannotStartPeriod { .. }));
        assert_eq!(m.start_periods.lock().unwrap().len(), 2);
    }

    #[test]
    fn start_period_no_window_constraint_when_disabled() {
        let (pub_inst, m, _, _) =
            new_publisher_for_test(5, 7, 7, SuperblockHash([1; 32]), 0, default_chain_set());

        for _ in 0..3 {
            pub_inst.start_period().unwrap();
        }

        assert_eq!(m.start_periods.lock().unwrap().len(), 3);
    }

    #[test]
    fn start_instance_disjoint_sets_allowed() {
        let (pub_inst, _, _, _) =
            new_publisher_for_test(5, 5, 5, SuperblockHash([1; 32]), 0, default_chain_set());

        let req1 = make_xt_request(vec![chain_req(1, &[b"a"]), chain_req(2, &[b"b"])]);
        let inst1 = pub_inst.start_instance(req1).unwrap();
        let chains1 = inst1.chains();
        assert_eq!(chains1.len(), 2);

        // Disjoint {3,4} should be allowed
        let req2 = make_xt_request(vec![chain_req(3, &[b"c"]), chain_req(4, &[b"d"])]);
        let inst2 = pub_inst.start_instance(req2).unwrap();
        let chains2 = inst2.chains();
        assert_eq!(chains2.len(), 2);
        assert!(chains2.contains(&ChainId(3)));
        assert!(chains2.contains(&ChainId(4)));
    }

    #[test]
    fn start_instance_overlapping_sets_allowed() {
        let (pub_inst, _, _, _) =
            new_publisher_for_test(5, 5, 5, SuperblockHash([1; 32]), 0, default_chain_set());

        let inst1 = pub_inst
            .start_instance(make_xt_request(vec![
                chain_req(1, &[b"a"]),
                chain_req(2, &[b"b"]),
            ]))
            .unwrap();

        // Overlapping {2,3} starts concurrently instead of being rejected.
        let inst2 = pub_inst
            .start_instance(make_xt_request(vec![
                chain_req(2, &[b"x"]),
                chain_req(3, &[b"y"]),
            ]))
            .unwrap();

        assert_ne!(inst1.id, inst2.id);
        assert!(inst2.sequence_number > inst1.sequence_number);
        assert_eq!(inst1.period_id, inst2.period_id);
    }

    #[test]
    fn start_instance_participant_dedup() {
        let (pub_inst, _, _, _) =
            new_publisher_for_test(2, 2, 2, SuperblockHash([1; 32]), 0, default_chain_set());

        let inst = pub_inst
            .start_instance(make_xt_request(vec![
                chain_req(7, &[b"a", b"b"]),
                chain_req(8, &[b"c"]),
            ]))
            .unwrap();
        let chains = inst.chains();
        assert_eq!(chains.len(), 2);
        assert!(chains.contains(&ChainId(7)));
        assert!(chains.contains(&ChainId(8)));
    }

    #[test]
    fn sequence_monotonic_and_resets_per_period() {
        let (pub_inst, m, _, _) =
            new_publisher_for_test(10, 9, 9, SuperblockHash([1; 32]), 0, default_chain_set());

        let i1 = pub_inst
            .start_instance(make_xt_request(vec![
                chain_req(1, &[b"a1"]),
                chain_req(2, &[b"a2"]),
            ]))
            .unwrap();
        let i2 = pub_inst
            .start_instance(make_xt_request(vec![
                chain_req(3, &[b"b1"]),
                chain_req(4, &[b"b2"]),
            ]))
            .unwrap();

        assert_eq!(i1.sequence_number, SequenceNumber(1));
        assert_eq!(i2.sequence_number, SequenceNumber(2));

        // New period resets sequence counter
        pub_inst.start_period().unwrap();
        assert_eq!(m.start_periods.lock().unwrap().len(), 1);

        let i3 = pub_inst
            .start_instance(make_xt_request(vec![
                chain_req(5, &[b"c1"]),
                chain_req(6, &[b"c2"]),
            ]))
            .unwrap();
        assert_eq!(i3.sequence_number, SequenceNumber(1));
    }

    #[test]
    fn start_instance_populates_instance_fields() {
        let (pub_inst, _, _, _) =
            new_publisher_for_test(1, 1, 1, SuperblockHash([1; 32]), 0, default_chain_set());
        let req = make_xt_request(vec![chain_req(1, &[b"x"]), chain_req(2, &[b"y"])]);

        let inst = pub_inst.start_instance(req.clone()).unwrap();
        assert_eq!(inst.period_id, PeriodId(1));
        assert_eq!(inst.sequence_number, SequenceNumber(1));
        assert_eq!(inst.xt_request, req);
    }

    #[test]
    fn advance_settled_state_monotonic() {
        let (pub_inst, _, _, _) =
            new_publisher_for_test(1, 1, 1, SuperblockHash([1; 32]), 0, default_chain_set());

        pub_inst
            .advance_settled_state(SuperblockNumber(2), SuperblockHash([9; 32]))
            .unwrap();

        let err = pub_inst
            .advance_settled_state(SuperblockNumber(2), SuperblockHash([8; 32]))
            .unwrap_err();
        assert!(matches!(err, PublisherError::OldSettledState));
    }

    #[test]
    fn proof_timeout_rolls_back_and_resets_target() {
        let finalized = SuperblockNumber(5);
        let (pub_inst, m, _, _) = new_publisher_for_test(
            3,
            finalized.get(),
            finalized.get(),
            SuperblockHash([7; 32]),
            0,
            default_chain_set(),
        );

        pub_inst
            .start_instance(make_xt_request(vec![
                chain_req(1, &[b"a"]),
                chain_req(2, &[b"b"]),
            ]))
            .unwrap();

        pub_inst.proof_timeout();

        let rollbacks = m.rollbacks.lock().unwrap();
        assert_eq!(rollbacks.len(), 1);
        assert_eq!(rollbacks[0].0, PeriodId(3));
        assert_eq!(rollbacks[0].1, finalized);
        assert_eq!(rollbacks[0].2, SuperblockHash([7; 32]));
        drop(rollbacks);

        assert_eq!(pub_inst.target_superblock_number(), finalized + 1);
    }

    #[test]
    fn start_instance_invalid_requests() {
        let (pub_inst, _, _, _) =
            new_publisher_for_test(1, 1, 1, SuperblockHash([1; 32]), 0, default_chain_set());

        // Empty request
        let err = pub_inst.start_instance(XtRequest::default()).unwrap_err();
        assert!(matches!(err, PublisherError::InvalidRequest));

        // Single-transaction request
        let err = pub_inst
            .start_instance(make_xt_request(vec![chain_req(1, &[b"only"])]))
            .unwrap_err();
        assert!(matches!(err, PublisherError::InvalidRequest));
    }

    #[test]
    fn receive_proof_aggregates_and_publishes() {
        let chains = make_chain_set(&[1, 2]);
        let (pub_inst, _, prover, l1) =
            new_publisher_for_test(10, 5, 5, SuperblockHash([1; 32]), 0, chains);
        *prover.next_proof.lock().unwrap() = b"network-proof".to_vec();
        pub_inst.start_period().unwrap();
        pub_inst.start_period().unwrap();

        pub_inst.receive_proof(
            PeriodId(11),
            SuperblockNumber(6),
            b"proof-1".to_vec(),
            ChainId(1),
        );
        assert!(prover.calls.lock().unwrap().is_empty());
        assert!(l1.published.lock().unwrap().is_empty());

        pub_inst.receive_proof(
            PeriodId(11),
            SuperblockNumber(6),
            b"proof-2".to_vec(),
            ChainId(2),
        );
        let calls = prover.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, SuperblockNumber(6));
        assert_eq!(calls[0].1, SuperblockHash([1; 32]));
        assert_eq!(calls[0].2.len(), 2);
        drop(calls);

        let published = l1.published.lock().unwrap();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].0, SuperblockNumber(6));
        assert_eq!(published[0].1, b"network-proof".to_vec());
        drop(published);

        // Proofs map should be cleared
        assert!(pub_inst.proofs_for(SuperblockNumber(6)).is_none());
    }

    #[test]
    fn receive_proof_prover_error_triggers_rollback() {
        let chains = make_chain_set(&[1, 2]);
        let (pub_inst, m, prover, l1) =
            new_publisher_for_test(10, 5, 5, SuperblockHash([9; 32]), 0, chains);
        *prover.err.lock().unwrap() = Some("boom".into());
        pub_inst.start_period().unwrap();
        pub_inst.start_period().unwrap();

        pub_inst.receive_proof(
            PeriodId(11),
            SuperblockNumber(6),
            b"proof-1".to_vec(),
            ChainId(1),
        );
        pub_inst.receive_proof(
            PeriodId(11),
            SuperblockNumber(6),
            b"proof-2".to_vec(),
            ChainId(2),
        );

        assert!(l1.published.lock().unwrap().is_empty());
        let rollbacks = m.rollbacks.lock().unwrap();
        assert_eq!(rollbacks.len(), 1);
        assert_eq!(rollbacks[0].1, SuperblockNumber(5));
        assert_eq!(rollbacks[0].2, SuperblockHash([9; 32]));
    }

    #[test]
    fn receive_proof_ignores_non_terminated_superblock() {
        let chains = make_chain_set(&[1, 2]);
        let (pub_inst, _, prover, l1) =
            new_publisher_for_test(5, 4, 4, SuperblockHash([3; 32]), 0, chains);
        pub_inst.start_period().unwrap();

        pub_inst.receive_proof(
            PeriodId(6),
            SuperblockNumber(5),
            b"proof".to_vec(),
            ChainId(1),
        );

        assert!(prover.calls.lock().unwrap().is_empty());
        assert!(l1.published.lock().unwrap().is_empty());
        assert!(pub_inst.proofs_for(SuperblockNumber(5)).is_none());
    }
}
