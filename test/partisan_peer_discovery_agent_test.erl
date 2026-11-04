%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%% @doc Guards the contract of `partisan_peer_discovery_agent': a discovery
%% answer is a hint for joining, never a membership authority. A member that a
%% lookup does not mention stays a member; a peer a lookup does mention is
%% joined.
%%
%% Cloud discovery backends are readiness-gated and eventually consistent: a
%% DNS answer omits a pod that is booting or briefly unhealthy. Treating the
%% omission as a departure removes the member and gossips the removal, and
%% the victim then evicts itself ("membership doesn't contain us"). Under a
%% full-membership strategy every node that is slow to boot is thrown out of
%% the cluster once per poll until it is ready — which is what the agent did
%% while it called `update_members/1'.
%%
%% The static manager is used for the same reason as in
%% `partisan_peer_service_manager_boot_test': its membership can be seeded
%% without a real peer, and its `update_members/1' removes absent members
%% exactly like the pluggable one. The two-node reproduction with real gossip
%% and a real self-eviction is `partisan_SUITE:discovery_never_evicts_test/1'.
%% @end
%% =============================================================================
-module(partisan_peer_discovery_agent_test).

-include_lib("eunit/include/eunit.hrl").
-include("partisan.hrl").

-define(MANAGER, partisan_static_peer_service_manager).
-define(POLL, 50).
-define(DISCOVERY(Addresses), #{
    enabled => true,
    type => partisan_peer_discovery_list,
    config => #{addresses => Addresses},
    initial_delay => 0,
    polling_interval => ?POLL
}).

%% =============================================================================
%% TEST DESCRIPTOR
%% =============================================================================

discovery_agent_test_() ->
    {timeout, 60,
        {setup, fun start/0, fun cleanup/1, fun(_) ->
            [
                ?_test(a_member_absent_from_every_lookup_stays_a_member()),
                ?_test(a_peer_a_lookup_mentions_is_joined()),
                ?_test(init_failure_does_not_take_the_plane_down()),
                ?_test(lookup_raise_keeps_the_plane_and_last_peers()),
                ?_test(a_hanging_callback_is_bounded()),
                ?_test(a_malformed_reply_is_a_failure()),
                ?_test(recovery_resets_the_failure_counter()),
                ?_test(a_missing_module_does_not_restart_the_agent()),
                ?_test(disable_answers_outstanding_lookup_callers()),
                ?_test(a_malformed_address_is_a_failure()),
                ?_test(malformed_timing_options_do_not_crash()),
                {timeout, 20, ?_test(lookup_waits_past_five_seconds())},
                ?_test(terminate_kills_a_hung_worker())
            ]
        end}}.

%% =============================================================================
%% CASES
%% =============================================================================

%% The backend answers with no addresses, so every poll names the local node
%% alone. Three polls later the seeded peer must still be a member and the
%% agent must still be running. Before `add_members/1' the first poll after
%% the seed removed the peer.
a_member_absent_from_every_lookup_stays_a_member() ->
    Peer = seed_membership('absent@127.0.0.1'),
    ?assert(is_a_member(Peer)),
    ok = await_polls(3),
    ?assert(is_a_member(Peer)),
    ?assertEqual(enabled, partisan_peer_discovery_agent:status()).

%% The join half must survive the change: a peer the backend does mention is
%% put on the join path, so the manager's `connected' signal for it makes it
%% a member. The peer seeded above is not mentioned and must stay.
a_peer_a_lookup_mentions_is_joined() ->
    Absent = 'absent@127.0.0.1',
    Address = "present@127.0.0.1:12345",
    ok = restart_agent(?DISCOVERY([Address])),
    ok = await_polls(2),

    %% The spec the backend builds is what the manager holds as pending, and
    %% the `connected' signal only moves an exact match into membership.
    {ok, S} = partisan_peer_discovery_list:init(#{addresses => [Address]}),
    {ok, [Spec], _} = partisan_peer_discovery_list:lookup(S, 1000),
    signal_connected(Spec),

    ?assert(is_a_member('present@127.0.0.1')),
    ?assert(is_a_member(Absent)).



%% A callback failure is an observation failure: the plane, manager and
%% membership stay untouched, and the agent keeps answering.

init_failure_does_not_take_the_plane_down() ->
    Manager = whereis(?MANAGER),
    ok = restart_agent(opts_for(partisan_discovery_test_init_raise)),
    Agent = whereis(partisan_peer_discovery_agent),
    ok = await_polls(2),
    %% The agent pid must survive; a restart would pass every other assertion.
    ?assertEqual(Agent, whereis(partisan_peer_discovery_agent)),
    ?assertEqual(Manager, whereis(?MANAGER)),
    ?assert(is_pid(whereis(partisan_peer_discovery_agent))),
    ?assertEqual(enabled, partisan_peer_discovery_agent:status()),
    Info = partisan_peer_discovery_agent:info(),
    ?assertEqual(false, maps:get(initialised, Info)),
    ?assert(maps:get(consecutive_failures, Info) > 0),
    ?assertNotEqual(undefined, maps:get(last_error, Info)).

lookup_raise_keeps_the_plane_and_last_peers() ->
    Spec = spec(),
    ok = restart_agent(opts_for(partisan_discovery_test_one_then_raise)),
    Agent = whereis(partisan_peer_discovery_agent),
    ok = await_polls(1),
    signal_connected(Spec),
    ?assert(is_a_member(maps:get(name, Spec))),

    Manager = whereis(?MANAGER),
    ok = await_polls(2),
    ?assertEqual(Agent, whereis(partisan_peer_discovery_agent)),
    ?assertEqual(Manager, whereis(?MANAGER)),
    ?assertEqual(enabled, partisan_peer_discovery_agent:status()),
    ?assert(is_a_member(maps:get(name, Spec))),
    Info = partisan_peer_discovery_agent:info(),
    ?assert(maps:get(consecutive_failures, Info) > 0).

a_hanging_callback_is_bounded() ->
    ok = restart_agent(opts_for(partisan_discovery_test_hang, 50)),
    timer:sleep(150),
    %% The agent answers while the worker hangs; the deadline classifies it.
    ?assertEqual(enabled, partisan_peer_discovery_agent:status()),
    ?assertEqual(timed_out, maps:get(last_error, partisan_peer_discovery_agent:info())),
    ?assertEqual(ok, partisan_peer_discovery_agent:disable()),
    ?assertEqual(disabled, partisan_peer_discovery_agent:status()),
    %% Still disabled later: no timer or worker survived the transition.
    timer:sleep(?POLL * 4),
    ?assertEqual(disabled, partisan_peer_discovery_agent:status()),
    ?assertEqual(false, maps:get(pending, partisan_peer_discovery_agent:info())),
    ok = partisan_peer_discovery_agent:enable().

a_malformed_reply_is_a_failure() ->
    Manager = whereis(?MANAGER),
    ok = restart_agent(opts_for(partisan_discovery_test_malformed)),
    ok = await_polls(2),
    ?assertEqual(Manager, whereis(?MANAGER)),
    ?assertMatch({malformed, _}, maps:get(last_error, partisan_peer_discovery_agent:info())).

recovery_resets_the_failure_counter() ->
    persistent_term:put({partisan_discovery_test_flaky, broken}, true),
    ok = restart_agent(opts_for(partisan_discovery_test_flaky)),
    ok = await_polls(2),
    ?assert(maps:get(consecutive_failures, partisan_peer_discovery_agent:info()) > 0),

    persistent_term:put({partisan_discovery_test_flaky, broken}, false),
    ok = await_polls(4),
    Info = partisan_peer_discovery_agent:info(),
    ?assertEqual(0, maps:get(consecutive_failures, Info)),
    ?assertEqual(undefined, maps:get(last_error, Info)),
    persistent_term:erase({partisan_discovery_test_flaky, broken}).


%% A missing backend raises `undef' at the boundary. The agent must not restart
%% and must recover once the module returns.
a_missing_module_does_not_restart_the_agent() ->
    Mod = partisan_discovery_test_absent,
    Hidden = hide_module(Mod),
    ?assertEqual(non_existing, code:which(Mod)),

    Manager = whereis(?MANAGER),
    ok = restart_agent(opts_for(Mod)),
    Agent = whereis(partisan_peer_discovery_agent),
    ok = await_polls(12),

    ?assertEqual(Agent, whereis(partisan_peer_discovery_agent)),
    ?assertEqual(Manager, whereis(?MANAGER)),
    ?assertEqual(enabled, partisan_peer_discovery_agent:status()),
    Info = partisan_peer_discovery_agent:info(),
    ?assertEqual(module_missing, maps:get(last_error, Info)),
    %% Failures actually happened in the window; the pid assertion is the claim.
    ?assert(maps:get(consecutive_failures, Info) >= 3),

    Restored = restore_module(Hidden),
    ?assertEqual(Restored, Mod),
    ok = await_polls(6),
    Info2 = partisan_peer_discovery_agent:info(),
    ?assertEqual(undefined, maps:get(last_error, Info2)),
    ?assertEqual(0, maps:get(consecutive_failures, Info2)),
    ?assertEqual(Agent, whereis(partisan_peer_discovery_agent)).

%% Disable must answer a waiting `lookup/0' promptly, and not on re-enable.
disable_answers_outstanding_lookup_callers() ->
    ok = restart_agent(opts_for(partisan_discovery_test_hang, 5000)),
    timer:sleep(?POLL * 2),
    ?assert(maps:get(pending, partisan_peer_discovery_agent:info())),

    Parent = self(),
    spawn(fun() -> Parent ! {lookup_reply, partisan_peer_discovery_agent:lookup()} end),
    timer:sleep(20),

    ?assertEqual(ok, partisan_peer_discovery_agent:disable()),
    receive
        {lookup_reply, Reply} -> ?assertEqual({error, disabled}, Reply)
    after 1000 ->
        ?assert(false)
    end,

    ?assertEqual(disabled, partisan_peer_discovery_agent:status()),
    ok = partisan_peer_discovery_agent:enable(),
    ?assertEqual(undefined, maps:get(last_error, partisan_peer_discovery_agent:info())).

%% Move the beam aside: an unloaded module would be reloaded on first call.
hide_module(Mod) ->
    Path = code:which(Mod),
    Away = Path ++ ".away",
    ok = file:rename(Path, Away),
    _ = code:delete(Mod),
    _ = code:purge(Mod),
    {Mod, Path, Away}.

restore_module({Mod, Path, Away}) ->
    ok = file:rename(Away, Path),
    {module, Mod} = code:load_abs(filename:rootname(Path)),
    Mod.

%% A nonnumeric address is malformed, not a partial update.
a_malformed_address_is_a_failure() ->
    Manager = whereis(?MANAGER),
    ok = restart_agent(opts_for(partisan_discovery_test_bad_ip)),
    ok = await_polls(2),
    ?assertEqual(Manager, whereis(?MANAGER)),
    ?assertMatch(
        {malformed, _},
        maps:get(last_error, partisan_peer_discovery_agent:info())
    ).

%% Malformed timing values fall back instead of crashing the agent.
malformed_timing_options_do_not_crash() ->
    Manager = whereis(?MANAGER),
    Opts = maps:merge(opts_for(partisan_discovery_test_flaky), #{
        initial_delay => negative,
        polling_interval => 0,
        retry_base => not_an_integer,
        retry_max => -1,
        timeout => -1
    }),
    ok = restart_agent(Opts),
    timer:sleep(?POLL * 2),
    ?assertEqual(Manager, whereis(?MANAGER)),
    ?assertEqual(enabled, partisan_peer_discovery_agent:status()),
    ?assert(is_map(partisan_peer_discovery_agent:info())),
    ok = partisan_peer_discovery_agent:disable().

%% The caller-side bound must cover the configured callback deadline.
lookup_waits_past_five_seconds() ->
    ok = restart_agent(opts_for(partisan_discovery_test_hang, 5500)),
    Started = erlang:monotonic_time(millisecond),
    Reply = partisan_peer_discovery_agent:lookup(),
    Elapsed = erlang:monotonic_time(millisecond) - Started,
    ?assertMatch({error, timed_out}, Reply),
    ?assert(Elapsed >= 5000),
    ok = partisan_peer_discovery_agent:disable().

%% Graceful termination kills the hung worker. Pending is element 11.
terminate_kills_a_hung_worker() ->
    ok = restart_agent(opts_for(partisan_discovery_test_hang, 60000)),
    timer:sleep(?POLL * 2),
    Agent = whereis(partisan_peer_discovery_agent),
    %% gen_statem returns {StateName, StateData} here.
    SysState = sys:get_state(Agent),
    StateData =
        case SysState of
            {_Name, Data} when is_tuple(Data), element(1, Data) =:= state -> Data;
            Data when is_tuple(Data), element(1, Data) =:= state -> Data
        end,
    Pending = element(11, StateData),
    ?assertMatch({_Ref, Pid, _MRef, _TRef, lookup} when is_pid(Pid), Pending),
    {_, Worker, _, _, _} = Pending,
    ?assert(is_process_alive(Worker)),
    ok = gen_statem:stop(Agent),
    ?assertNot(is_process_alive(Worker)).

%% =============================================================================
%% UTILS
%% =============================================================================

spec() ->
    #{
        name => 'present@127.0.0.1',
        listen_addrs => [#{ip => {127, 0, 0, 1}, port => 12345}],
        channels => #{}
    }.

opts_for(Mod) ->
    opts_for(Mod, 1000).

opts_for(Mod, Timeout) ->
    #{
        enabled => true,
        type => Mod,
        config => #{},
        initial_delay => 0,
        polling_interval => ?POLL,
        timeout => Timeout,
        %% Keep retries inside the test window.
        retry_base => ?POLL,
        retry_max => ?POLL * 8
    }.

start() ->
    stop_partisan(),
    %% eunit VMs are `nonode@nohost'; give the listener a concrete address.
    %%
    %% Save the previous env: init/0 writes resolved values back, so a previous
    %% module's listen_addrs would otherwise win over our peer_ip.
    Saved = #{
        peer_ip => application:get_env(partisan, peer_ip),
        peer_port => application:get_env(partisan, peer_port),
        peer_service_manager => application:get_env(partisan, peer_service_manager),
        peer_discovery => application:get_env(partisan, peer_discovery),
        listen_addrs => application:get_env(partisan, listen_addrs)
    },
    ok = application:set_env(partisan, peer_ip, {127, 0, 0, 1}),
    ok = application:set_env(partisan, peer_port, 15000 + rand:uniform(20000)),
    ok = application:set_env(partisan, peer_service_manager, ?MANAGER),
    ok = application:set_env(partisan, peer_discovery, ?DISCOVERY([])),
    %% Drop stale listen_addrs so init/0 derives it from peer_ip.
    application:unset_env(partisan, listen_addrs),
    %% Another module may have left the previous config in persistent_term;
    %% re-read it now that our environment is in place.
    ok = partisan_config:init(),
    {ok, _} = application:ensure_all_started(partisan),
    Saved.

cleanup(Saved) ->
    stop_partisan(),
    %% Restore exactly what was there before.
    ok = lists:foreach(
        fun({Key, Value}) -> restore_env(Key, Value) end,
        maps:to_list(Saved)
    ),
    ok.

restore_env(Key, {ok, Value}) ->
    application:set_env(partisan, Key, Value);
restore_env(Key, undefined) ->
    application:unset_env(partisan, Key).

stop_partisan() ->
    case lists:keymember(partisan, 1, application:which_applications()) of
        true -> application:stop(partisan);
        false -> ok
    end.

%% The agent reads its options when it starts, so a new backend configuration
%% takes effect through a supervisor restart of the agent alone.
restart_agent(Opts) ->
    ok = partisan_config:set(peer_discovery, Opts),
    Sup = partisan_peer_service_sup,
    ok = supervisor:terminate_child(Sup, partisan_peer_discovery_agent),
    {ok, _} = supervisor:restart_child(Sup, partisan_peer_discovery_agent),
    ok.

%% Polls are timer-driven; the trailing `lookup/0' call is served by the same
%% state machine, so once it answers every poll due in the window has run.
await_polls(N) ->
    timer:sleep(?POLL * N + ?POLL),
    %% A barrier: either reply means the window's polls have run.
    _ = partisan_peer_discovery_agent:lookup(),
    ok.

seed_membership(Name) ->
    Spec = #{name => Name, listen_addrs => [], channels => #{}},
    ok = partisan_peer_service:join(Spec),
    signal_connected(Spec),
    Name.

%% The manager reacts to the `connected' signal the connection process sends
%% once its handshake completes; driving it directly avoids needing a peer.
signal_connected(Spec) ->
    Mgr = whereis(?MANAGER),
    Mgr ! {connected, Spec, ?DEFAULT_CHANNEL, undefined, undefined},
    _ = sys:get_state(Mgr),
    ok.

is_a_member(Name) ->
    {ok, Members} = partisan_peer_service:members(),
    lists:member(Name, Members).

