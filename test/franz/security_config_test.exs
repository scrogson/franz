defmodule Franz.SecurityConfigTest do
  use ExUnit.Case, async: true
  alias Franz.SecurityConfig

  describe "new/1" do
    test "creates config with defaults" do
      config = SecurityConfig.new()

      assert config.security_protocol == :plaintext
      assert is_nil(config.sasl_mechanism)
      assert is_nil(config.sasl_username)
      assert is_nil(config.sasl_password)
      assert is_nil(config.ssl_ca_location)
      assert is_nil(config.ssl_certificate_location)
      assert is_nil(config.ssl_key_location)
      assert is_nil(config.ssl_key_password)
      assert config.ssl_endpoint_identification_algorithm == :https
    end

    test "creates config with SASL/PLAIN SSL" do
      config =
        SecurityConfig.new(
          security_protocol: :sasl_ssl,
          sasl_mechanism: :plain,
          sasl_username: "user",
          sasl_password: "pass"
        )

      assert config.security_protocol == :sasl_ssl
      assert config.sasl_mechanism == :plain
      assert config.sasl_username == "user"
      assert config.sasl_password == "pass"
    end

    test "creates config with SASL/SCRAM-SHA-256" do
      config =
        SecurityConfig.new(
          security_protocol: :sasl_ssl,
          sasl_mechanism: :scram_sha_256,
          sasl_username: "user",
          sasl_password: "pass"
        )

      assert config.security_protocol == :sasl_ssl
      assert config.sasl_mechanism == :scram_sha_256
    end

    test "creates config with SASL/SCRAM-SHA-512" do
      config =
        SecurityConfig.new(
          security_protocol: :sasl_plaintext,
          sasl_mechanism: :scram_sha_512,
          sasl_username: "user",
          sasl_password: "pass"
        )

      assert config.security_protocol == :sasl_plaintext
      assert config.sasl_mechanism == :scram_sha_512
    end

    test "creates config with SSL certificates" do
      config =
        SecurityConfig.new(
          security_protocol: :ssl,
          ssl_ca_location: "/path/to/ca.pem",
          ssl_certificate_location: "/path/to/cert.pem",
          ssl_key_location: "/path/to/key.pem",
          ssl_key_password: "keypass"
        )

      assert config.security_protocol == :ssl
      assert config.ssl_ca_location == "/path/to/ca.pem"
      assert config.ssl_certificate_location == "/path/to/cert.pem"
      assert config.ssl_key_location == "/path/to/key.pem"
      assert config.ssl_key_password == "keypass"
    end

    test "creates config with custom endpoint identification" do
      config = SecurityConfig.new(ssl_endpoint_identification_algorithm: :none)

      assert config.ssl_endpoint_identification_algorithm == :none
    end
  end

  describe "to_rdkafka_config/1" do
    test "converts plaintext config" do
      config = SecurityConfig.new(security_protocol: :plaintext)
      rdkafka_config = SecurityConfig.to_rdkafka_config(config)

      assert rdkafka_config["security.protocol"] == "plaintext"
      assert rdkafka_config["ssl.endpoint.identification.algorithm"] == "https"
    end

    test "converts SASL/PLAIN SSL config" do
      config =
        SecurityConfig.new(
          security_protocol: :sasl_ssl,
          sasl_mechanism: :plain,
          sasl_username: "testuser",
          sasl_password: "testpass"
        )

      rdkafka_config = SecurityConfig.to_rdkafka_config(config)

      assert rdkafka_config["security.protocol"] == "sasl_ssl"
      assert rdkafka_config["sasl.mechanism"] == "PLAIN"
      assert rdkafka_config["sasl.username"] == "testuser"
      assert rdkafka_config["sasl.password"] == "testpass"
    end

    test "converts SASL/SCRAM-SHA-256 config" do
      config =
        SecurityConfig.new(
          security_protocol: :sasl_ssl,
          sasl_mechanism: :scram_sha_256,
          sasl_username: "user",
          sasl_password: "pass"
        )

      rdkafka_config = SecurityConfig.to_rdkafka_config(config)

      assert rdkafka_config["sasl.mechanism"] == "SCRAM-SHA-256"
    end

    test "converts SASL/SCRAM-SHA-512 config" do
      config =
        SecurityConfig.new(
          security_protocol: :sasl_plaintext,
          sasl_mechanism: :scram_sha_512,
          sasl_username: "user",
          sasl_password: "pass"
        )

      rdkafka_config = SecurityConfig.to_rdkafka_config(config)

      assert rdkafka_config["sasl.mechanism"] == "SCRAM-SHA-512"
    end

    test "converts SASL/GSSAPI config" do
      config =
        SecurityConfig.new(
          security_protocol: :sasl_ssl,
          sasl_mechanism: :gssapi,
          sasl_username: "user",
          sasl_password: "pass"
        )

      rdkafka_config = SecurityConfig.to_rdkafka_config(config)

      assert rdkafka_config["sasl.mechanism"] == "GSSAPI"
    end

    test "converts SASL/OAUTHBEARER config" do
      config =
        SecurityConfig.new(
          security_protocol: :sasl_ssl,
          sasl_mechanism: :oauthbearer,
          sasl_username: "user",
          sasl_password: "token"
        )

      rdkafka_config = SecurityConfig.to_rdkafka_config(config)

      assert rdkafka_config["sasl.mechanism"] == "OAUTHBEARER"
    end

    test "converts SSL config with certificates" do
      config =
        SecurityConfig.new(
          security_protocol: :ssl,
          ssl_ca_location: "/etc/ssl/ca.pem",
          ssl_certificate_location: "/etc/ssl/cert.pem",
          ssl_key_location: "/etc/ssl/key.pem",
          ssl_key_password: "keypass"
        )

      rdkafka_config = SecurityConfig.to_rdkafka_config(config)

      assert rdkafka_config["security.protocol"] == "ssl"
      assert rdkafka_config["ssl.ca.location"] == "/etc/ssl/ca.pem"
      assert rdkafka_config["ssl.certificate.location"] == "/etc/ssl/cert.pem"
      assert rdkafka_config["ssl.key.location"] == "/etc/ssl/key.pem"
      assert rdkafka_config["ssl.key.password"] == "keypass"
    end

    test "omits nil SSL values" do
      config = SecurityConfig.new(security_protocol: :ssl)
      rdkafka_config = SecurityConfig.to_rdkafka_config(config)

      refute Map.has_key?(rdkafka_config, "ssl.ca.location")
      refute Map.has_key?(rdkafka_config, "ssl.certificate.location")
      refute Map.has_key?(rdkafka_config, "ssl.key.location")
      refute Map.has_key?(rdkafka_config, "ssl.key.password")
    end

    test "omits SASL config when mechanism is nil" do
      config = SecurityConfig.new(security_protocol: :ssl)
      rdkafka_config = SecurityConfig.to_rdkafka_config(config)

      refute Map.has_key?(rdkafka_config, "sasl.mechanism")
      refute Map.has_key?(rdkafka_config, "sasl.username")
      refute Map.has_key?(rdkafka_config, "sasl.password")
    end

    test "converts endpoint identification algorithm to string" do
      config = SecurityConfig.new(ssl_endpoint_identification_algorithm: :none)
      rdkafka_config = SecurityConfig.to_rdkafka_config(config)

      assert rdkafka_config["ssl.endpoint.identification.algorithm"] == "none"
    end
  end
end
