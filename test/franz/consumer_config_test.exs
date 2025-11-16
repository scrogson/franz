defmodule Franz.Consumer.ConfigTest do
  use ExUnit.Case, async: true
  alias Franz.{Consumer.Config, SecurityConfig}

  describe "new/1" do
    test "creates config with defaults" do
      config = Config.new()

      assert config.bootstrap_servers == ""
      assert config.auto_offset_reset == :beginning
      assert config.enable_auto_commit == true
      assert config.topics == []
      # group_id gets a random default
      assert is_binary(config.group_id)
      assert String.length(config.group_id) == 16
      assert is_nil(config.security)
    end

    test "creates config with keyword list" do
      config =
        Config.new(
          group_id: "my-group",
          bootstrap_servers: "localhost:9092",
          auto_offset_reset: :latest,
          enable_auto_commit: false,
          topics: ["events", "logs"]
        )

      assert config.group_id == "my-group"
      assert config.bootstrap_servers == "localhost:9092"
      assert config.auto_offset_reset == :latest
      assert config.enable_auto_commit == false
      assert config.topics == ["events", "logs"]
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
    test "group_id/2 sets group ID" do
      config =
        Config.new()
        |> Config.group_id("my-consumer-group")

      assert config.group_id == "my-consumer-group"
    end

    test "bootstrap_servers/2 sets bootstrap servers" do
      config =
        Config.new()
        |> Config.bootstrap_servers("localhost:9092")

      assert config.bootstrap_servers == "localhost:9092"
    end

    test "auto_offset_reset/2 sets offset reset strategy" do
      for reset <- [:smallest, :earliest, :beginning, :largest, :latest, :end, :error] do
        config =
          Config.new()
          |> Config.auto_offset_reset(reset)

        assert config.auto_offset_reset == reset
      end
    end

    test "enable_auto_commit/2 enables auto commit" do
      config =
        Config.new()
        |> Config.enable_auto_commit(true)

      assert config.enable_auto_commit == true
    end

    test "enable_auto_commit/2 disables auto commit" do
      config =
        Config.new()
        |> Config.enable_auto_commit(false)

      assert config.enable_auto_commit == false
    end

    test "topics/2 sets topics list" do
      config =
        Config.new()
        |> Config.topics(["events", "notifications", "logs"])

      assert config.topics == ["events", "notifications", "logs"]
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
        |> Config.group_id("my-group")
        |> Config.bootstrap_servers("localhost:9092")
        |> Config.auto_offset_reset(:earliest)
        |> Config.enable_auto_commit(false)
        |> Config.topics(["events", "logs"])

      assert config.group_id == "my-group"
      assert config.bootstrap_servers == "localhost:9092"
      assert config.auto_offset_reset == :earliest
      assert config.enable_auto_commit == false
      assert config.topics == ["events", "logs"]
    end
  end
end
