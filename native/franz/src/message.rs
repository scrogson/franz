use rdkafka::message::{BorrowedMessage, Message as _};
use rustler::{Binary, Decoder, Encoder, Env, Error, NifStruct, OwnedBinary, Term};
use std::io::Write as _;

#[derive(NifStruct)]
#[module = "Franz.Message"]
pub struct Message {
    pub payload: Option<Bin>,
    pub key: Option<Bin>,
    pub topic: String,
    pub timestamp: Option<i64>,
    pub partition: i32,
    pub offset: i64,
    pub headers: Vec<(String, String)>,
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

pub struct Bin(pub Vec<u8>);

impl<'a> Encoder for Bin {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        let mut bin = OwnedBinary::new(self.0.len()).expect("Failed to alloc");
        bin.as_mut_slice().write_all(&self.0).unwrap();
        bin.release(env).encode(env)
    }
}

impl<'a> Decoder<'a> for Bin {
    fn decode(term: Term<'a>) -> Result<Bin, Error> {
        let binary: Binary = term.decode()?;
        let bytes: Vec<u8> = binary.as_slice().to_owned();
        Ok(Bin(bytes))
    }
}
