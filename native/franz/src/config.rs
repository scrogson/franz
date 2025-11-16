use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rustler::{NifStruct, NifUnitEnum};
use std::fmt;

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

impl fmt::Display for SaslMechanism {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
            SaslMechanism::Gssapi => "GSSAPI",
            SaslMechanism::Oauthbearer => "OAUTHBEARER",
        };
        write!(f, "{}", s)
    }
}

#[derive(NifUnitEnum, Clone)]
pub enum SecurityProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl fmt::Display for SecurityProtocol {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            SecurityProtocol::Plaintext => "plaintext",
            SecurityProtocol::Ssl => "ssl",
            SecurityProtocol::SaslPlaintext => "sasl_plaintext",
            SecurityProtocol::SaslSsl => "sasl_ssl",
        };
        write!(f, "{}", s)
    }
}

#[derive(NifUnitEnum, Clone)]
pub enum SslEndpointIdentification {
    None,
    Https,
}

impl fmt::Display for SslEndpointIdentification {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            SslEndpointIdentification::None => "none",
            SslEndpointIdentification::Https => "https",
        };
        write!(f, "{}", s)
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
        cfg.set("security.protocol", self.security_protocol.to_string());

        if let Some(mechanism) = &self.sasl_mechanism {
            cfg.set("sasl.mechanism", mechanism.to_string());
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
            self.ssl_endpoint_identification_algorithm.to_string(),
        );
    }
}

impl fmt::Display for AutoOffsetReset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            AutoOffsetReset::Smallest => "smallest",
            AutoOffsetReset::Earliest => "earliest",
            AutoOffsetReset::Beginning => "beginning",
            AutoOffsetReset::Largest => "largest",
            AutoOffsetReset::Latest => "latest",
            AutoOffsetReset::End => "end",
            AutoOffsetReset::Error => "error",
        };
        write!(f, "{}", s)
    }
}

impl From<ConsumerConfig> for ClientConfig {
    fn from(val: ConsumerConfig) -> Self {
        let mut cfg = ClientConfig::new();
        cfg.set("auto.offset.reset", val.auto_offset_reset.to_string());
        cfg.set("bootstrap.servers", &val.bootstrap_servers);
        cfg.set("enable.auto.commit", val.enable_auto_commit.to_string());

        if let Some(group_id) = &val.group_id {
            cfg.set("group.id", group_id);
        }

        if let Some(security) = &val.security {
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

impl fmt::Display for Acks {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            Acks::None => "0",
            Acks::Leader => "1",
            Acks::All => "-1",
        };
        write!(f, "{}", s)
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

impl fmt::Display for CompressionType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            CompressionType::None => "none",
            CompressionType::Gzip => "gzip",
            CompressionType::Snappy => "snappy",
            CompressionType::Lz4 => "lz4",
            CompressionType::Zstd => "zstd",
        };
        write!(f, "{}", s)
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

impl From<ProducerConfig> for ClientConfig {
    fn from(val: ProducerConfig) -> Self {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &val.bootstrap_servers);
        cfg.set("acks", val.acks.to_string());
        cfg.set("compression.type", val.compression_type.to_string());
        cfg.set("linger.ms", val.linger_ms.to_string());
        cfg.set("batch.size", val.batch_size.to_string());
        cfg.set(
            "max.in.flight.requests.per.connection",
            val.max_in_flight.to_string(),
        );

        if let Some(security) = &val.security {
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
