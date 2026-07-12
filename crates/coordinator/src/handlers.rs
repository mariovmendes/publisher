//! Inbound message dispatch.

use std::sync::Arc;

use compose_spec::ChainId;
use compose_spec_proto::{Message, Payload};
use prost::Message as _;
use tracing::{error, info, warn};

use crate::coordinator::Coordinator;
use crate::proof_types::{AggregationOutputs, MailboxInfo, ProofData};

pub async fn dispatch(coordinator: Arc<Coordinator>, client_id: String, data: Vec<u8>) {
    coordinator.inc_messages();

    let msg = match Message::decode(data.as_slice()) {
        Ok(m) => m,
        Err(e) => {
            error!(client_id, error = %e, "Failed to decode message");
            return;
        }
    };

    let Some(payload) = msg.payload else {
        warn!(client_id, "Empty payload");
        return;
    };

    match payload {
        Payload::Vote(vote) => {
            coordinator
                .handle_vote(
                    &client_id,
                    &vote.instance_id,
                    ChainId::new(vote.chain_id),
                    vote.vote,
                )
                .await;
        }
        Payload::XtRequest(xt_req) => {
            coordinator.handle_xt_request(client_id, xt_req).await;
        }
        Payload::Ping(ping) => {
            coordinator.handle_ping(&client_id, ping.timestamp).await;
        }
        Payload::HandshakeRequest(req) => {
            handle_handshake(coordinator, &client_id, &req).await;
        }
        Payload::MailboxMessage(mb) => {
            coordinator.handle_mailbox_relay(&mb).await;
        }
        Payload::Proof(proof) => {
            handle_proof(coordinator, &client_id, proof).await;
        }
        Payload::Confirmed(confirmed) => {
            coordinator.
                handle_confirmed(&confirmed.instance_id, ChainId::new(confirmed.chain_id))
                .await;
        }
        other => {
            warn!(client_id, payload_type = ?std::mem::discriminant(&other), "Unhandled payload");
        }
    }
}

async fn handle_handshake(
    coordinator: Arc<Coordinator>,
    client_id: &str,
    req: &compose_spec_proto::HandshakeRequest,
) {
    info!(client_id, requested_id = %req.client_id, "Handshake received");

    if !req.client_id.is_empty() {
        match parse_chain_id(&req.client_id) {
            Ok(chain_id) => coordinator.register_chain(client_id, chain_id).await,
            Err(e) => warn!(client_id, error = %e, "Invalid chain ID in handshake"),
        }
    }

    let resp = compose_spec_proto::Message {
        sender_id: "publisher".into(),
        payload: Some(Payload::HandshakeResponse(
            compose_spec_proto::HandshakeResponse {
                accepted: true,
                error: String::new(),
                session_id: client_id.to_string(),
            },
        )),
    };
    let data = resp.encode_to_vec();
    if let Err(e) = coordinator.server().send_raw(client_id, &data).await {
        warn!(client_id, error = %e, "Failed to send handshake response");
    }
}

/// Handles a `Proof` protobuf message received over QUIC. The `proof_data` field
/// contains a minimal payload; full proof submissions with aggregation outputs
/// should use the HTTP `/v1/proofs/op-succinct` endpoint instead.
async fn handle_proof(
    coordinator: Arc<Coordinator>,
    client_id: &str,
    proof: compose_spec_proto::Proof,
) {
    // The Proof wire message carries no chain_id; the sender chain is identified
    // by its registered connection, mirroring the SBCP publisher spec.
    let Some(chain_id) = coordinator.chain_for_client(client_id).await else {
        warn!(client_id, "Proof from unregistered client, ignoring");
        return;
    };

    let data = ProofData {
        aggregation_outputs: AggregationOutputs::default(),
        compressed_proof: proof.proof_data,
        agg_vkey_hash: Default::default(),
        mailbox_info: MailboxInfo::default(),
    };
    coordinator
        .receive_proof(proof.superblock_number, chain_id.get(), data)
        .await;
}

pub fn parse_chain_id(client_id: &str) -> Result<ChainId, ParseChainIdError> {
    let num_str: String = client_id
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();

    if num_str.is_empty() {
        return Err(ParseChainIdError(client_id.to_string()));
    }

    num_str
        .parse::<u64>()
        .map(ChainId::new)
        .map_err(|_| ParseChainIdError(client_id.to_string()))
}

#[derive(Debug)]
pub struct ParseChainIdError(pub String);

impl std::fmt::Display for ParseChainIdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "no numeric chain ID prefix in '{}'", self.0)
    }
}

impl std::error::Error for ParseChainIdError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_chain_id_numeric_prefix() {
        assert_eq!(parse_chain_id("77777").unwrap(), ChainId::new(77777));
        assert_eq!(
            parse_chain_id("88888-sidecar").unwrap(),
            ChainId::new(88888)
        );
        assert!(parse_chain_id("abc").is_err());
        assert!(parse_chain_id("").is_err());
    }
}
