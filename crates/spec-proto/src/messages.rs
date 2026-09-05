// ---------------------------------------------------------------------------
// Connection-level messages
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, prost::Message)]
pub struct HandshakeRequest {
    #[prost(int64, tag = "1")]
    pub timestamp: i64,
    #[prost(bytes = "vec", tag = "2")]
    pub public_key: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub signature: Vec<u8>,
    #[prost(string, tag = "4")]
    pub client_id: String,
    #[prost(bytes = "vec", tag = "5")]
    pub nonce: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct HandshakeResponse {
    #[prost(bool, tag = "1")]
    pub accepted: bool,
    #[prost(string, tag = "2")]
    pub error: String,
    #[prost(string, tag = "3")]
    pub session_id: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Ping {
    #[prost(int64, tag = "1")]
    pub timestamp: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Pong {
    #[prost(int64, tag = "1")]
    pub timestamp: i64,
}

// ---------------------------------------------------------------------------
// SCP messages
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, prost::Message)]
pub struct TransactionRequest {
    #[prost(uint64, tag = "1")]
    pub chain_id: u64,
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub transaction: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct XtRequest {
    #[prost(message, repeated, tag = "1")]
    pub transaction_requests: Vec<TransactionRequest>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct StartInstance {
    #[prost(bytes = "vec", tag = "1")]
    pub instance_id: Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub period_id: u64,
    #[prost(uint64, tag = "3")]
    pub sequence_number: u64,
    #[prost(message, optional, tag = "4")]
    pub xt_request: Option<XtRequest>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Vote {
    #[prost(bytes = "vec", tag = "1")]
    pub instance_id: Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub chain_id: u64,
    #[prost(bool, tag = "3")]
    pub vote: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Decided {
    #[prost(bytes = "vec", tag = "1")]
    pub instance_id: Vec<u8>,
    #[prost(bool, tag = "2")]
    pub decision: bool,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Confirmed {
    #[prost(bytes = "vec", tag = "1")]
    pub instance_id: Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub chain_id: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct MailboxMessage {
    #[prost(uint64, tag = "1")]
    pub session_id: u64,
    #[prost(bytes = "vec", tag = "2")]
    pub instance_id: Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub source_chain: u64,
    #[prost(uint64, tag = "4")]
    pub destination_chain: u64,
    #[prost(bytes = "vec", tag = "5")]
    pub source: Vec<u8>,
    #[prost(bytes = "vec", tag = "6")]
    pub receiver: Vec<u8>,
    #[prost(string, tag = "7")]
    pub label: String,
    #[prost(bytes = "vec", repeated, tag = "8")]
    pub data: Vec<Vec<u8>>,
}

// ---------------------------------------------------------------------------
// SBCP messages
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, prost::Message)]
pub struct StartPeriod {
    #[prost(uint64, tag = "1")]
    pub period_id: u64,
    #[prost(uint64, tag = "2")]
    pub superblock_number: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Rollback {
    #[prost(uint64, tag = "1")]
    pub period_id: u64,
    #[prost(uint64, tag = "2")]
    pub last_finalized_superblock_number: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub last_finalized_superblock_hash: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Proof {
    #[prost(uint64, tag = "1")]
    pub period_id: u64,
    #[prost(uint64, tag = "2")]
    pub superblock_number: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub proof_data: Vec<u8>,
}

// ---------------------------------------------------------------------------
// CDCP messages
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, prost::Message)]
pub struct NativeDecided {
    #[prost(bytes = "vec", tag = "1")]
    pub instance_id: Vec<u8>,
    #[prost(bool, tag = "2")]
    pub decision: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct WsDecided {
    #[prost(bytes = "vec", tag = "1")]
    pub instance_id: Vec<u8>,
    #[prost(bool, tag = "2")]
    pub decision: bool,
}

// ---------------------------------------------------------------------------
// Message envelope
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, prost::Message)]
pub struct Message {
    #[prost(string, tag = "1")]
    pub sender_id: String,
    #[prost(
        oneof = "Payload",
        tags = "2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16"
    )]
    pub payload: Option<Payload>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub enum Payload {
    #[prost(message, tag = "2")]
    HandshakeRequest(HandshakeRequest),
    #[prost(message, tag = "3")]
    HandshakeResponse(HandshakeResponse),
    #[prost(message, tag = "4")]
    Ping(Ping),
    #[prost(message, tag = "5")]
    Pong(Pong),
    #[prost(message, tag = "6")]
    XtRequest(XtRequest),
    #[prost(message, tag = "7")]
    StartInstance(StartInstance),
    #[prost(message, tag = "8")]
    Vote(Vote),
    #[prost(message, tag = "9")]
    Decided(Decided),
    #[prost(message, tag = "10")]
    MailboxMessage(MailboxMessage),
    #[prost(message, tag = "11")]
    StartPeriod(StartPeriod),
    #[prost(message, tag = "12")]
    Rollback(Rollback),
    #[prost(message, tag = "13")]
    Proof(Proof),
    #[prost(message, tag = "14")]
    NativeDecided(NativeDecided),
    #[prost(message, tag = "15")]
    WsDecided(WsDecided),
    #[prost(message, tag= "16")]
    Confirmed(Confirmed)
}

#[cfg(test)]
mod tests {
    use prost::Message as _;

    use super::*;

    #[test]
    fn roundtrip_message_encode_decode() {
        let msg = Message {
            sender_id: "test-sender".into(),
            payload: Some(Payload::Ping(Ping { timestamp: 42 })),
        };

        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();

        let decoded = Message::decode(buf.as_slice()).unwrap();
        assert_eq!(msg, decoded);
    }
}
