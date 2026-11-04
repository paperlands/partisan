%% A backend that answers with a structurally shaped but nonnumeric address:
%% the agent must classify it as malformed, never hand it to the manager.
-module(partisan_discovery_test_bad_ip).
-behaviour(partisan_peer_discovery_agent).
-export([init/1, lookup/2]).

init(_Config) -> {ok, bad_ip}.

lookup(State, _Timeout) ->
    {ok, [spec()], State}.

spec() ->
    #{
        name => 'bad_ip@127.0.0.1',
        listen_addrs => [#{ip => {a, b, c, d}, port => 12345}],
        channels => #{}
    }.
