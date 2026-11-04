%% A backend whose `init/1' always fails: the agent must report it, retry with
%% backoff, and leave the peer service plane alone.
-module(partisan_discovery_test_init_raise).
-behaviour(partisan_peer_discovery_agent).
-export([init/1, lookup/2]).

init(_Config) -> error(init_unavailable).
lookup(State, _Timeout) -> {ok, [], State}.
