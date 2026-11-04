%% A backend that answers rubbish: a half-valid answer is a failed observation,
%% never a partial update.
-module(partisan_discovery_test_malformed).
-behaviour(partisan_peer_discovery_agent).
-export([init/1, lookup/2]).

init(_Config) -> {ok, malformed}.
lookup(State, _Timeout) -> {ok, [not_a_node_spec], State}.
