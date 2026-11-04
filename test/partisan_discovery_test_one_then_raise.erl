%% A backend that answers once and then raises: the peer it named stays a member,
%% the plane stays up, and the failure is counted.
-module(partisan_discovery_test_one_then_raise).
-behaviour(partisan_peer_discovery_agent).
-export([init/1, lookup/2]).

init(_Config) ->
    persistent_term:put({?MODULE, served}, false),
    {ok, served}.

lookup(_State, _Timeout) ->
    case persistent_term:get({?MODULE, served}) of
        true ->
            error(lookup_unavailable);
        false ->
            persistent_term:put({?MODULE, served}, true),
            {ok, [spec()], served}
    end.

spec() ->
    #{
        name => 'present@127.0.0.1',
        listen_addrs => [#{ip => {127, 0, 0, 1}, port => 12345}],
        channels => #{}
    }.
