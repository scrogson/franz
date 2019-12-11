#! /usr/bin/env bash

export ERL_TOP="$HOME/otp"

"$ERL_TOP/bin/cerl" -valgrind -kernel shell_history enabled -pa /usr/local/lib/elixir/bin/../lib/eex/ebin /usr/local/lib/elixir/bin/../lib/elixir/ebin /usr/local/lib/elixir/bin/../lib/ex_unit/ebin /usr/local/lib/elixir/bin/../lib/iex/ebin /usr/local/lib/elixir/bin/../lib/logger/ebin /usr/local/lib/elixir/bin/../lib/mix/ebin -elixir ansi_enabled true -noshell -user Elixir.IEx.CLI -- -extra --no-halt +iex -S mix
