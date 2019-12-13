use rdkafka::message::{BorrowedMessage, Message as _};
use rustler::{Decoder, Encoder, Env, Error, NifStruct, OwnedBinary, Term};
use std::io::Write as _;

#[derive(NifStruct)]
#[module = "Franz.Message"]
pub struct Message {
    payload: Option<Bin>,
    key: Option<Bin>,
    topic: String,
    timestamp: Option<i64>,
    partition: i32,
    offset: i64,
    headers: Vec<(String, String)>,
}

impl<'a> From<&BorrowedMessage<'a>> for Message {
    fn from(msg: &BorrowedMessage) -> Message {
        Message {
            payload: msg.payload().map(|p| Bin(p.to_vec())),
            key: msg.key().map(|k| Bin(k.to_vec())),
            topic: msg.topic().to_owned(),
            timestamp: msg.timestamp().to_millis(),
            partition: msg.partition(),
            offset: msg.offset(),
            headers: vec![], // FIXME: copy out the headers for real
        }
    }
}

struct Bin(Vec<u8>);

impl<'a> Encoder for Bin {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        let mut bin = OwnedBinary::new(self.0.len()).expect("Failed to alloc");
        bin.as_mut_slice().write_all(&self.0).unwrap();
        bin.release(env).encode(env)
    }
}

impl<'a> Decoder<'a> for Bin {
    fn decode(term: Term<'a>) -> Result<Bin, Error> {
        Ok(Bin(term.decode::<Vec<u8>>()?))
    }
}
