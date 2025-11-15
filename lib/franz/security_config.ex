defmodule Franz.SecurityConfig do
  @moduledoc """
  Security configuration for Kafka connections.

  Supports SASL authentication and SSL/TLS encryption.
  """

  @type sasl_mechanism :: :plain | :scram_sha_256 | :scram_sha_512 | :gssapi | :oauthbearer | nil
  @type ssl_endpoint_identification :: :none | :https

  defstruct sasl_mechanism: nil,
            sasl_username: nil,
            sasl_password: nil,
            security_protocol: :plaintext,
            ssl_ca_location: nil,
            ssl_certificate_location: nil,
            ssl_key_location: nil,
            ssl_key_password: nil,
            ssl_endpoint_identification_algorithm: :https

  @type t :: %__MODULE__{
          # SASL authentication
          sasl_mechanism: sasl_mechanism(),
          sasl_username: String.t() | nil,
          sasl_password: String.t() | nil,
          # Security protocol
          security_protocol: :plaintext | :ssl | :sasl_plaintext | :sasl_ssl,
          # SSL/TLS configuration
          ssl_ca_location: String.t() | nil,
          ssl_certificate_location: String.t() | nil,
          ssl_key_location: String.t() | nil,
          ssl_key_password: String.t() | nil,
          ssl_endpoint_identification_algorithm: ssl_endpoint_identification()
        }

  @doc """
  Creates a new security configuration.

  ## Examples

      # SASL/PLAIN with SSL
      SecurityConfig.new(
        security_protocol: :sasl_ssl,
        sasl_mechanism: :plain,
        sasl_username: "user",
        sasl_password: "pass",
        ssl_ca_location: "/path/to/ca.pem"
      )

      # SSL only (mTLS)
      SecurityConfig.new(
        security_protocol: :ssl,
        ssl_ca_location: "/path/to/ca.pem",
        ssl_certificate_location: "/path/to/client.pem",
        ssl_key_location: "/path/to/client.key"
      )

      # SASL/SCRAM-SHA-256 without SSL
      SecurityConfig.new(
        security_protocol: :sasl_plaintext,
        sasl_mechanism: :scram_sha_256,
        sasl_username: "user",
        sasl_password: "pass"
      )
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    struct(__MODULE__, opts)
  end

  @doc """
  Converts security config to librdkafka configuration map.
  """
  @spec to_rdkafka_config(t()) :: %{String.t() => String.t()}
  def to_rdkafka_config(%__MODULE__{} = config) do
    base_config = %{
      "security.protocol" => to_string(config.security_protocol)
    }

    base_config
    |> maybe_add_sasl_config(config)
    |> maybe_add_ssl_config(config)
  end

  defp maybe_add_sasl_config(config_map, %{sasl_mechanism: nil}), do: config_map

  defp maybe_add_sasl_config(config_map, %{
         sasl_mechanism: mechanism,
         sasl_username: username,
         sasl_password: password
       })
       when not is_nil(username) and not is_nil(password) do
    mechanism_str =
      case mechanism do
        :plain -> "PLAIN"
        :scram_sha_256 -> "SCRAM-SHA-256"
        :scram_sha_512 -> "SCRAM-SHA-512"
        :gssapi -> "GSSAPI"
        :oauthbearer -> "OAUTHBEARER"
      end

    config_map
    |> Map.put("sasl.mechanism", mechanism_str)
    |> Map.put("sasl.username", username)
    |> Map.put("sasl.password", password)
  end

  defp maybe_add_sasl_config(config_map, _), do: config_map

  defp maybe_add_ssl_config(config_map, config) do
    config_map
    |> maybe_put("ssl.ca.location", config.ssl_ca_location)
    |> maybe_put("ssl.certificate.location", config.ssl_certificate_location)
    |> maybe_put("ssl.key.location", config.ssl_key_location)
    |> maybe_put("ssl.key.password", config.ssl_key_password)
    |> maybe_put(
      "ssl.endpoint.identification.algorithm",
      ssl_endpoint_id_to_string(config.ssl_endpoint_identification_algorithm)
    )
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp ssl_endpoint_id_to_string(:none), do: "none"
  defp ssl_endpoint_id_to_string(:https), do: "https"
end
