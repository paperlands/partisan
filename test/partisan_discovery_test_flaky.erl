%% A backend that fails while a flag is set and recovers when it clears: success
%% must reset the failure counter and adopt the peers.
-module(partisan_discovery_test_flaky).
-behaviour(partisan_peer_discovery_agent).
-export([init/1, lookup/2]).

init(_Config) -> {ok, flaky}.

lookup(State, _Timeout) ->
    case persistent_term:get({?MODULE, broken}, false) of
        true -> error(backend_down);
        false -> {ok, [spec()], State}
    end.

spec() ->
    #{
        name => 'present@127.0.0.1',
        listen_addrs => [#{ip => {127, 0, 0, 1}, port => 12345}],
        channels => #{}
    }.
