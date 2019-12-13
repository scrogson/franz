use rustler::{Encoder, NifStruct, NifUnitEnum};

#[derive(NifUnitEnum)]
pub enum AutoOffsetReset {
    Smallest,
    Earliest,
    Beginning,
    Largest,
    Latest,
    End,
    Error,
}

impl AutoOffsetReset {
    pub fn to_string(self) -> String {
        use AutoOffsetReset::*;

        match self {
            Smallest => "smallest".to_string(),
            Earliest => "earliest".to_string(),
            Beginning => "beginning".to_string(),
            Largest => "largest".to_string(),
            Latest => "latest".to_string(),
            End => "end".to_string(),
            Error => "error".to_string(),
        }
    }
}

#[derive(NifStruct)]
#[module = "Franz.Consumer.Config"]
pub struct ConsumerConfig {
    /// auto.offset.reset
    pub auto_offset_reset: AutoOffsetReset,
    /// bootstrap.servers
    pub bootstrap_servers: String,
    /// enable.auto.commit
    pub enable_auto_commit: bool,
    /// group.id
    pub group_id: Option<String>,
    pub topics: Vec<String>,
}

#[derive(NifStruct)]
#[module = "Franz.Producer.Config"]
pub struct ProducerConfig {
    /// bootstrap.servers
    pub bootstrap_servers: String,
}

#[derive(NifStruct)]
#[module = "Franz.Admin.Config"]
pub struct AdminConfig {
    /// bootstrap.servers
    pub bootstrap_servers: String,
}
