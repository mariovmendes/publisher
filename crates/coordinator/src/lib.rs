//! Shared publisher coordinator - manages sidecar connections and routes
//! cross-chain transactions through two-phase commit consensus.

pub mod coordinator;
pub mod handlers;
pub mod l1_submit;
pub mod proof_types;
mod xtflow;
