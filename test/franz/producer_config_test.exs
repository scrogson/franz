defmodule Franz.Producer.ConfigTest do
  use ExUnit.Case, async: true
  alias Franz.{Producer.Config, SecurityConfig}

  describe "new/1" do
    test "creates config with defaults" do
      config = Config.new()

      assert config.bootstrap_servers == ""
      assert config.acks == :all
      assert config.compression_type == :none
      assert config.linger_ms == 0
      assert config.batch_size == 16384
      assert config.max_in_flight == 5
      assert is_nil(config.security)
    end

    test "creates config with keyword list" do
      config =
        Config.new(
          bootstrap_servers: "localhost:9092",
          acks: :leader,
          compression_type: :lz4,
          linger_ms: 10,
          batch_size: 1_000_000,
          max_in_flight: 10
        )

      assert config.bootstrap_servers == "localhost:9092"
      assert config.acks == :leader
      assert config.compression_type == :lz4
      assert config.linger_ms == 10
      assert config.batch_size == 1_000_000
      assert config.max_in_flight == 10
    end

    test "accepts SecurityConfig struct" do
      security = SecurityConfig.new(security_protocol: :sasl_ssl, sasl_mechanism: :plain)
      config = Config.new(security: security)

      assert config.security == security
    end

    test "converts security keyword list to struct" do
      config = Config.new(security: [security_protocol: :sasl_ssl, sasl_mechanism: :plain])

      assert %SecurityConfig{} = config.security
      assert config.security.security_protocol == :sasl_ssl
      assert config.security.sasl_mechanism == :plain
    end
  end

  describe "fluent builders" do
    test "bootstrap_servers/2 sets bootstrap servers" do
      config =
        Config.new()
        |> Config.bootstrap_servers("localhost:9092")

      assert config.bootstrap_servers == "localhost:9092"
    end

    test "acks/2 sets acknowledgement level" do
      for level <- [:none, :leader, :all] do
        config =
          Config.new()
          |> Config.acks(level)

        assert config.acks == level
      end
    end

    test "compression_type/2 sets compression" do
      for type <- [:none, :gzip, :snappy, :lz4, :zstd] do
        config =
          Config.new()
          |> Config.compression_type(type)

        assert config.compression_type == type
      end
    end

    test "linger_ms/2 sets linger time" do
      config =
        Config.new()
        |> Config.linger_ms(100)

      assert config.linger_ms == 100
    end

    test "batch_size/2 sets batch size" do
      config =
        Config.new()
        |> Config.batch_size(2_000_000)

      assert config.batch_size == 2_000_000
    end

    test "max_in_flight/2 sets max in flight" do
      config =
        Config.new()
        |> Config.max_in_flight(20)

      assert config.max_in_flight == 20
    end

    test "security/2 sets security config" do
      security = SecurityConfig.new(security_protocol: :sasl_ssl)

      config =
        Config.new()
        |> Config.security(security)

      assert config.security == security
    end

    test "security/2 accepts nil" do
      config =
        Config.new(security: SecurityConfig.new(security_protocol: :sasl_ssl))
        |> Config.security(nil)

      assert is_nil(config.security)
    end

    test "fluent chaining works" do
      config =
        Config.new()
        |> Config.bootstrap_servers("localhost:9092")
        |> Config.acks(:leader)
        |> Config.compression_type(:lz4)
        |> Config.linger_ms(10)
        |> Config.batch_size(1_000_000)
        |> Config.max_in_flight(10)

      assert config.bootstrap_servers == "localhost:9092"
      assert config.acks == :leader
      assert config.compression_type == :lz4
      assert config.linger_ms == 10
      assert config.batch_size == 1_000_000
      assert config.max_in_flight == 10
    end
  end
end
