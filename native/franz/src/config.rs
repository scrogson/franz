use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rustler::{NifStruct, NifUnitEnum};

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

#[derive(NifUnitEnum, Clone)]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
    Gssapi,
    Oauthbearer,
}

impl SaslMechanism {
    pub fn to_string(&self) -> String {
        match self {
            SaslMechanism::Plain => "PLAIN".to_string(),
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256".to_string(),
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512".to_string(),
            SaslMechanism::Gssapi => "GSSAPI".to_string(),
            SaslMechanism::Oauthbearer => "OAUTHBEARER".to_string(),
        }
    }
}

#[derive(NifUnitEnum, Clone)]
pub enum SecurityProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl SecurityProtocol {
    pub fn to_string(&self) -> String {
        match self {
            SecurityProtocol::Plaintext => "plaintext".to_string(),
            SecurityProtocol::Ssl => "ssl".to_string(),
            SecurityProtocol::SaslPlaintext => "sasl_plaintext".to_string(),
            SecurityProtocol::SaslSsl => "sasl_ssl".to_string(),
        }
    }
}

#[derive(NifUnitEnum, Clone)]
pub enum SslEndpointIdentification {
    None,
    Https,
}

impl SslEndpointIdentification {
    pub fn to_string(&self) -> String {
        match self {
            SslEndpointIdentification::None => "none".to_string(),
            SslEndpointIdentification::Https => "https".to_string(),
        }
    }
}

#[derive(NifStruct, Clone)]
#[module = "Franz.SecurityConfig"]
pub struct SecurityConfig {
    pub sasl_mechanism: Option<SaslMechanism>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub security_protocol: SecurityProtocol,
    pub ssl_ca_location: Option<String>,
    pub ssl_certificate_location: Option<String>,
    pub ssl_key_location: Option<String>,
    pub ssl_key_password: Option<String>,
    pub ssl_endpoint_identification_algorithm: SslEndpointIdentification,
}

impl SecurityConfig {
    pub fn apply_to_config(&self, cfg: &mut ClientConfig) {
        cfg.set("security.protocol", &self.security_protocol.to_string());

        if let Some(mechanism) = &self.sasl_mechanism {
            cfg.set("sasl.mechanism", &mechanism.to_string());
        }

        if let Some(username) = &self.sasl_username {
            cfg.set("sasl.username", username);
        }

        if let Some(password) = &self.sasl_password {
            cfg.set("sasl.password", password);
        }

        if let Some(ca_location) = &self.ssl_ca_location {
            cfg.set("ssl.ca.location", ca_location);
        }

        if let Some(cert_location) = &self.ssl_certificate_location {
            cfg.set("ssl.certificate.location", cert_location);
        }

        if let Some(key_location) = &self.ssl_key_location {
            cfg.set("ssl.key.location", key_location);
        }

        if let Some(key_password) = &self.ssl_key_password {
            cfg.set("ssl.key.password", key_password);
        }

        cfg.set(
            "ssl.endpoint.identification.algorithm",
            &self.ssl_endpoint_identification_algorithm.to_string(),
        );
    }
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

impl Into<ClientConfig> for ConsumerConfig {
    fn into(self) -> ClientConfig {
        let mut cfg = ClientConfig::new();
        cfg.set("auto.offset.reset", &self.auto_offset_reset.to_string());
        cfg.set("bootstrap.servers", &self.bootstrap_servers);
        cfg.set("enable.auto.commit", &self.enable_auto_commit.to_string());

        if let Some(group_id) = &self.group_id {
            cfg.set("group.id", group_id);
        }

        if let Some(security) = &self.security {
            security.apply_to_config(&mut cfg);
        }

        cfg.set_log_level(RDKafkaLogLevel::Warning);
        cfg
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
    pub security: Option<SecurityConfig>,
}

#[derive(NifUnitEnum, Clone)]
pub enum Acks {
    None,
    Leader,
    All,
}

impl Acks {
    pub fn to_string(&self) -> String {
        match self {
            Acks::None => "0".to_string(),
            Acks::Leader => "1".to_string(),
            Acks::All => "-1".to_string(),
        }
    }
}

#[derive(NifUnitEnum, Clone)]
pub enum CompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl CompressionType {
    pub fn to_string(&self) -> String {
        match self {
            CompressionType::None => "none".to_string(),
            CompressionType::Gzip => "gzip".to_string(),
            CompressionType::Snappy => "snappy".to_string(),
            CompressionType::Lz4 => "lz4".to_string(),
            CompressionType::Zstd => "zstd".to_string(),
        }
    }
}

#[derive(NifStruct)]
#[module = "Franz.Producer.Config"]
pub struct ProducerConfig {
    /// bootstrap.servers
    pub bootstrap_servers: String,
    pub security: Option<SecurityConfig>,
    pub acks: Acks,
    pub compression_type: CompressionType,
    pub linger_ms: i64,
    pub batch_size: i64,
    pub max_in_flight: i64,
}

impl Into<ClientConfig> for ProducerConfig {
    fn into(self) -> ClientConfig {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &self.bootstrap_servers);
        cfg.set("acks", &self.acks.to_string());
        cfg.set("compression.type", &self.compression_type.to_string());
        cfg.set("linger.ms", &self.linger_ms.to_string());
        cfg.set("batch.size", &self.batch_size.to_string());
        cfg.set(
            "max.in.flight.requests.per.connection",
            &self.max_in_flight.to_string(),
        );

        if let Some(security) = &self.security {
            security.apply_to_config(&mut cfg);
        }

        cfg.set_log_level(RDKafkaLogLevel::Warning);
        cfg
    }
}

#[derive(NifStruct)]
#[module = "Franz.Admin.Config"]
pub struct AdminConfig {
    /// bootstrap.servers
    pub bootstrap_servers: String,
    pub security: Option<SecurityConfig>,
}
