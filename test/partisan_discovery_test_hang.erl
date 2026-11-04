%% A backend that ignores its timeout: the agent must impose its own deadline and
%% stay answerable while the worker hangs.
-module(partisan_discovery_test_hang).
-behaviour(partisan_peer_discovery_agent).
-export([init/1, lookup/2]).

init(_Config) -> {ok, hanging}.
lookup(State, _Timeout) ->
    timer:sleep(10000),
    {ok, [], State}.
