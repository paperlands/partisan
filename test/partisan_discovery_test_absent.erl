%% A backend that is compiled but deliberately unloaded by the test, so the
%% invocation boundary raises `undef' exactly as a hot-reloaded module does. The
%% test loads it again to prove recovery.
-module(partisan_discovery_test_absent).
-behaviour(partisan_peer_discovery_agent).
-export([init/1, lookup/2]).

init(_Config) -> {ok, absent}.
lookup(State, _Timeout) -> {ok, [spec()], State}.

spec() ->
    #{
        name => 'present@127.0.0.1',
        listen_addrs => [#{ip => {127, 0, 0, 1}, port => 12345}],
        channels => #{}
    }.
