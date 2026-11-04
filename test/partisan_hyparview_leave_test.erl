%% =============================================================================
%% @doc Leave and admission regressions.
%%
%% Each case runs in an isolated peer so the connections and membership tables
%% are fresh; a shared eunit VM would make the seed order-dependent.
%% @end
%% =============================================================================
-module(partisan_hyparview_leave_test).

-include_lib("eunit/include/eunit.hrl").
-include("partisan.hrl").

-export([
    atom_leave_declines_and_manager_survives/0,
    map_leave_self_declines/0,
    map_leave_other_removes/0,
    replayed_old_spec_is_not_admitted/0,
    noop_leave_does_not_publish/0,
    public_disconnect_node_declines_locally/0
]).

-define(NODE, 'peer@127.0.0.1').

-define(CASES, [
    atom_leave_declines_and_manager_survives,
    map_leave_self_declines,
    map_leave_other_removes,
    replayed_old_spec_is_not_admitted,
    noop_leave_does_not_publish,
    public_disconnect_node_declines_locally
]).

leave_test_() ->
    [
        {
            atom_to_list(Case),
            {setup, fun start_peer/0, fun peer:stop/1, fun(Peer) ->
                ?_test(
                    ?assertEqual(
                        ok, peer:call(Peer, ?MODULE, Case, [], 60000)
                    )
                )
            end}
        }
     || Case <- ?CASES
    ].

start_peer() ->
    Paths = lists:append([["-pa", Dir] || Dir <- code:get_path()]),
    Opts = #{connection => standard_io, args => Paths},
    {ok, Peer, _} = peer:start_link(Opts),
    ok = peer:call(Peer, partisan_config, init, []),
    Peer.

%% =============================================================================
%% CASES
%% =============================================================================

%% {error,Reason} was once installed as manager state; it must not be.
atom_leave_declines_and_manager_survives() ->
    with_manager(fun(Mgr) ->
        ?assertEqual(
            {error, not_implemented},
            partisan_hyparview_peer_service_manager:leave()
        ),
        ?assert(is_process_alive(Mgr)),
        ?assertMatch({ok, [_ | _]}, partisan_hyparview_peer_service_manager:members()),
        %% A second call proves the state survived.
        ?assertEqual(
            {error, not_implemented},
            partisan_hyparview_peer_service_manager:leave()
        ),
        ?assert(is_process_alive(Mgr))
    end).

%% The map form is reachable directly; the public wrapper routes self to leave/0.
map_leave_self_declines() ->
    with_manager(fun(_) ->
        Self = partisan:node_spec(),
        ?assertEqual(
            {error, not_implemented},
            partisan_hyparview_peer_service_manager:leave(Self)
        ),
        ?assert(lists:member(partisan:node(), names()))
    end).

map_leave_other_removes() ->
    with_manager(fun(_) ->
        Name = 'other@127.0.0.1',
        Spec = spec(Name, 0),
        ok = partisan_hyparview_peer_service_manager:update_members([Spec]),
        {ok, Passive0} = partisan_hyparview_peer_service_manager:passive(),
        ?assert(sets:is_element(Spec, Passive0)),
        ?assertEqual(
            ok,
            partisan_hyparview_peer_service_manager:leave(#{name => Name})
        ),
        {ok, Passive1} = partisan_hyparview_peer_service_manager:passive(),
        ?assertNot(sets:is_element(Spec, Passive1))
    end).

%% The public wrapper reports false when the manager declines.
public_disconnect_node_declines_locally() ->
    with_manager(fun(Mgr) ->
        Result =
            try
                partisan:disconnect_node(node())
            catch
                Class:Reason:Stack -> error({Class, Reason, Stack})
            end,
        ?assertEqual(false, Result),
        ?assert(is_process_alive(Mgr)),
        ?assertMatch(
            {ok, [_ | _]}, partisan_hyparview_peer_service_manager:members()
        )
    end).

%% Two reachable specs of one name must not both enter the active view: the
%% second advertisement is refused before capacity or eviction is considered.
replayed_old_spec_is_not_admitted() ->
    with_manager(fun(Mgr) ->
        Old = spec(?NODE, 31111),
        New = spec(?NODE, 32222),
        OldAddr = addr(31111),
        NewAddr = addr(32222),
        [OldPid, NewPid] = [
            spawn(fun() -> receive stop -> ok end end)
         || _ <- [1, 2]
        ],
        ok = partisan_peer_connections:store(Old, OldPid, ?DEFAULT_CHANNEL, OldAddr),
        ok = partisan_peer_connections:store(New, NewPid, ?DEFAULT_CHANNEL, NewAddr),
        Self = partisan:node_spec(),

        ok = partisan_hyparview_peer_service_manager:receive_message(
            New,
            ?DEFAULT_CHANNEL,
            {neighbor, New, tag_new, undefined, Self}
        ),
        {ok, Active0} = partisan_hyparview_peer_service_manager:active(),
        ?assertEqual([New], same_name(Active0, ?NODE)),

        ok = partisan_hyparview_peer_service_manager:receive_message(
            Old,
            ?DEFAULT_CHANNEL,
            {neighbor, Old, tag_old, undefined, Self}
        ),
        {ok, Active1} = partisan_hyparview_peer_service_manager:active(),
        ?assertEqual([New], same_name(Active1, ?NODE)),
        ?assertEqual(2, sets:size(Active1)),
        ?assert(is_process_alive(Mgr)),
        [exit(Pid, kill) || Pid <- [OldPid, NewPid]],
        ok
    end).

noop_leave_does_not_publish() ->
    with_manager(fun(_) ->
        Version0 = partisan_membership:version(),
        ?assert(Version0 > 0),
        ?assertEqual(
            ok,
            partisan_hyparview_peer_service_manager:leave(
                #{name => 'absent@127.0.0.1'}
            )
        ),
        ?assertEqual(Version0, partisan_membership:version())
    end).

%% =============================================================================
%% UTILS
%% =============================================================================

with_manager(Fun) ->
    _ = application:ensure_all_started(telemetry),
    ok = partisan_config:init(),
    %% Point the public wrapper at the manager this test starts.
    ok = partisan_config:set(
        peer_service_manager, partisan_hyparview_peer_service_manager
    ),
    ok = partisan_peer_connections:init(),
    ensure_membership_table(),
    {ok, Mgr} = partisan_hyparview_peer_service_manager:start_link(),
    try
        Fun(Mgr)
    after
        _ = try gen_server:stop(Mgr) catch _:_ -> ok end
    end.

ensure_membership_table() ->
    case ets:info(partisan_membership, name) of
        undefined ->
            _ = ets:new(partisan_membership, [
                named_table,
                public,
                set,
                {read_concurrency, true}
            ]),
            true = ets:insert(partisan_membership, [{version, 0}, {members, []}]),
            ok;
        _ ->
            ok
    end.

names() ->
    {ok, Names} = partisan_hyparview_peer_service_manager:members(),
    Names.

spec(Name, Port) ->
    #{name => Name, listen_addrs => [addr(Port)], channels => #{}}.

addr(Port) ->
    #{ip => {127, 0, 0, 1}, port => Port}.

same_name(Active, Name) ->
    [
        Spec
     || Spec <- sets:to_list(Active),
        maps:get(name, Spec) =:= Name
    ].
