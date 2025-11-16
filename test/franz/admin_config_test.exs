defmodule Franz.Admin.ConfigTest do
  use ExUnit.Case, async: true
  alias Franz.{Admin.Config, SecurityConfig}

  describe "new/1" do
    test "creates config with defaults" do
      config = Config.new()

      assert config.bootstrap_servers == ""
      assert is_nil(config.security)
    end

    test "creates config with keyword list" do
      config = Config.new(bootstrap_servers: "localhost:9092")

      assert config.bootstrap_servers == "localhost:9092"
    end

    test "accepts SecurityConfig struct" do
      security = SecurityConfig.new(security_protocol: :sasl_ssl, sasl_mechanism: :plain)
      config = Config.new(security: security)

      assert config.security == security
    end

    test "converts security keyword list to struct" do
      config =
        Config.new(
          security: [
            security_protocol: :sasl_ssl,
            sasl_mechanism: :plain,
            sasl_username: "admin",
            sasl_password: "secret"
          ]
        )

      assert %SecurityConfig{} = config.security
      assert config.security.security_protocol == :sasl_ssl
      assert config.security.sasl_mechanism == :plain
      assert config.security.sasl_username == "admin"
      assert config.security.sasl_password == "secret"
    end
  end

  describe "fluent builders" do
    test "bootstrap_servers/2 sets bootstrap servers" do
      config = Config.bootstrap_servers(Config.new(), "localhost:9092")

      assert config.bootstrap_servers == "localhost:9092"
    end

    test "security/2 sets security config" do
      security =
        SecurityConfig.new(
          security_protocol: :sasl_ssl,
          sasl_mechanism: :plain,
          sasl_username: "admin"
        )

      config = Config.security(Config.new(), security)

      assert config.security == security
    end

    test "security/2 accepts nil" do
      config =
        Config.security(
          Config.new(security: SecurityConfig.new(security_protocol: :sasl_ssl)),
          nil
        )

      assert is_nil(config.security)
    end

    test "fluent chaining works" do
      security = SecurityConfig.new(security_protocol: :sasl_ssl)

      config =
        Config.new()
        |> Config.bootstrap_servers("localhost:9092")
        |> Config.security(security)

      assert config.bootstrap_servers == "localhost:9092"
      assert config.security == security
    end
  end
end
