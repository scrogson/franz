# Compile test support modules
Code.require_file("support/test_helpers.ex", __DIR__)

Franz.delete_topic("127.0.0.1:9094", "test")

# Limit max concurrent tests to avoid overwhelming Kafka
ExUnit.start(exclude: [:performance, :skip], max_cases: 8)
