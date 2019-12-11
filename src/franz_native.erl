-module(franz_native).

-export([init/0]).
-export([client_start/1]).
-export([client_poll/2]).
-export([client_stop/1]).

-on_load(init/0).

client_start(_Config) ->
  err().

client_poll(_Ref, _Timeout) ->
  err().

client_stop(_Ref) ->
  err().

init() ->
  PrivDir = code:priv_dir(franz),
  SoName = filename:join(PrivDir, "native/libfranz"),
  erlang:load_nif(SoName, 0).

err() ->
  erlang:nif_error(franz_nif_not_loaded).
