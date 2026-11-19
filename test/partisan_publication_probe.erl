%% Publish once per active-spec change, including address-only replacement.
%% reconnect-deferred-maintenance
-module(partisan_publication_probe).
-export([run/1]).

-define(MGR, partisan_hyparview_peer_service_manager).
-define(PEER, probe_peer).
-define(OLD_ADDR, #{ip => {127, 0, 0, 1}, port => 31111}).
-define(NEW_ADDR, #{ip => {127, 0, 0, 1}, port => 32222}).

run(Mode) when Mode =:= pinned; Mode =:= guard; Mode =:= pinned_mix ->
    Arm = atom_to_list(Mode),
    io:format("ARM ~s~n", [Arm]),
    io:format("MANAGER_BEAM ~s~n", [code:which(?MGR)]),
    io:format("MANAGER_MD5 ~s~n", [beam_md5(?MGR)]),
    {ok, _} = application:ensure_all_started(telemetry),
    ok = partisan_config:init(),
    ok = partisan_config:set(peer_service_manager, ?MGR),
    ok = partisan_config:set(hyparview, #{
        shuffle_interval => 60000,
        random_promotion => false
    }),
    ok = partisan_config:set(active_view_maintenance_interval, 60000),
    ok = partisan_peer_connections:init(),
    partisan_membership = ets:new(partisan_membership, [
        named_table, public, set
    ]),
    true = ets:insert(partisan_membership, [{version, 0}, {members, []}]),
    ok = partisan_membership:subscribe(),
    ok = add_telemetry(),
    {ok, Mgr} = ?MGR:start_link(),
    put(checks, []),
    io:format(
        "INIT version=~w published=~w active_names=~w~n",
        [partisan_membership:version(), published_addrs(), active_names(Mgr)]
    ),

    OldAddr = ?OLD_ADDR,
    NewAddr = ?NEW_ADDR,
    Old = #{name => ?PEER, listen_addrs => [OldAddr], channels => #{}},
    New = Old#{listen_addrs => [NewAddr]},

    P = spawn(fun hold/0),
    Q = spawn(fun hold/0),
    ok = partisan_peer_connections:store(Old, P, undefined, OldAddr),

    S1 = step(Mgr, initial_neighbor, fun() -> neighbor(Mgr, Old) end),
    check(S1, [
        {active, [[OldAddr]]},
        {published, [[OldAddr]]},
        {dversion, 1},
        {dnotify, 1},
        {rows, [{OldAddr, undefined}]}
    ]),

    S2 = step(Mgr, repeated_neighbor, fun() -> neighbor(Mgr, Old) end),
    check(S2, [
        {active, [[OldAddr]]},
        {published, [[OldAddr]]},
        {dversion, 0},
        {dnotify, 0},
        {rows, [{OldAddr, undefined}]}
    ]),

    ok = partisan_peer_connections:store(New, Q, undefined, NewAddr),
    S3 = step(Mgr, competing_neighbor, fun() -> neighbor(Mgr, New) end),
    check(S3, [
        {active, [[OldAddr]]},
        {published, [[OldAddr]]},
        {dversion, 0},
        {dnotify, 0},
        {rows, [{OldAddr, undefined}, {NewAddr, undefined}]}
    ]),

    true = ets:give_away(partisan_peer_connections, Mgr, probe),
    S4 = step(Mgr, old_exit, fun() -> Mgr ! {'EXIT', P, closed} end),
    check(S4, [
        {active, [[OldAddr]]},
        {published, [[OldAddr]]},
        {dversion, 0},
        {dnotify, 0},
        {rows, [{NewAddr, undefined}]}
    ]),

    S5 = step(Mgr, replacement_neighbor, fun() -> neighbor(Mgr, New) end),
    check(S5, [
        {active, [[NewAddr]]},
        {published, [[NewAddr]]},
        {dversion, 1},
        {dnotify, 1},
        {rows, [{NewAddr, undefined}]}
    ]),

    S6 = step(Mgr, repeated_new, fun() -> neighbor(Mgr, New) end),
    check(S6, [
        {active, [[NewAddr]]},
        {published, [[NewAddr]]},
        {dversion, 0},
        {dnotify, 0},
        {rows, [{NewAddr, undefined}]}
    ]),

    [emit_step(S) || S <- [S1, S2, S3, S4, S5, S6]],
    report(Arm).

step(Mgr, Label, Input) ->
    flush_notify(),
    Before = #{
        version => partisan_membership:version(), telem => telem_counts()
    },
    Input(),
    {ok, Active} = gen_server:call(Mgr, active, 5000),
    After = #{
        version => partisan_membership:version(), telem => telem_counts()
    },
    Notify = flush_notify(),
    #{
        label => Label,
        active => active_probe(Active),
        active_names => active_names(Mgr),
        published => published_addrs(),
        version => {maps:get(version, Before), maps:get(version, After)},
        dversion => maps:get(version, After) - maps:get(version, Before),
        dnotify => Notify,
        dtelem => diff_telem(maps:get(telem, Before), maps:get(telem, After)),
        rows => conn_rows()
    }.

check(Step, Preds) ->
    Label = maps:get(label, Step),
    Added =
        case Label of
            initial_neighbor -> 1;
            replacement_neighbor -> 1;
            _ -> 0
        end,
    Results = [run_pred(Label, P, Step) || P <- [{peer_added, Added} | Preds]],
    put(checks, get(checks) ++ Results),
    ok.

run_pred(Label, {peer_added, N}, Step) ->
    Got = maps:get(
        partisan_hyparview_active_view_peer_added, maps:get(dtelem, Step), 0
    ),
    expect(Label, peer_added, Got, N);
run_pred(Label, {active, Addrs}, Step) ->
    expect(Label, active, maps:get(active, Step), Addrs);
run_pred(Label, {published, Addrs}, Step) ->
    expect(Label, published, maps:get(published, Step), Addrs);
run_pred(Label, {dversion, N}, Step) ->
    expect(Label, dversion, maps:get(dversion, Step), N);
run_pred(Label, {dnotify, N}, Step) ->
    expect(Label, dnotify, maps:get(dnotify, Step), N);
run_pred(Label, {rows, Want}, Step) ->
    expect(Label, rows, maps:get(rows, Step), lists:sort(Want)).

expect(Label, Name, Got, Want) ->
    case Got =:= Want of
        true -> {ok, {Name, Got}};
        false -> {fail, Label, Name, [{got, Got}, {want, Want}]}
    end.

report(Arm) ->
    All = get(checks),
    Fails = [R || {fail, _, _, _} = R <- All],
    io:format(
        "CHECKS total=~p ok=~p fail=~p~n",
        [length(All), length(All) - length(Fails), length(Fails)]
    ),
    [io:format("FAILED ~p~n", [F]) || F <- Fails],
    case Fails of
        [] ->
            io:format("RESULT arm=~s INVARIANT=HOLDS~n", [Arm]),
            ok;
        _ ->
            io:format("RESULT arm=~s INVARIANT=VIOLATED~n", [Arm]),
            {violated, length(Fails)}
    end.

emit_step(#{label := Label} = S) ->
    T = maps:get(dtelem, S),
    io:format(
        "STEP ~w active=~w active_all_names=~w published=~w "
        "version=~w dversion=~w dnotify=~w peer_added=~w "
        "membership_changed=~w rows=~w~n",
        [
            Label,
            fmt_addrs(maps:get(active, S)),
            maps:get(active_names, S),
            fmt_addrs(maps:get(published, S)),
            maps:get(version, S),
            maps:get(dversion, S),
            maps:get(dnotify, S),
            maps:get(partisan_hyparview_active_view_peer_added, T, 0),
            maps:get(partisan_membership_changed, T, 0),
            [{fmt_addr(A), C} || {A, C} <- maps:get(rows, S)]
        ]
    ).

fmt_addrs(AddrLists) -> [[fmt_addr(A) || A <- L] || L <- AddrLists].

fmt_addr(#{ip := {127, 0, 0, 1}, port := 31111}) -> old;
fmt_addr(#{ip := {127, 0, 0, 1}, port := 32222}) -> new;
fmt_addr(Other) -> Other.

neighbor(Mgr, Spec) ->
    gen_server:cast(
        Mgr,
        {receive_message, Spec, undefined,
            {neighbor, Spec, probe_tag, undefined, partisan:node_spec()}}
    ).

hold() ->
    receive
        {system, From, {terminate, _}} -> gen:reply(From, ok);
        stop -> ok
    end.

active_probe(Specs) ->
    lists:sort([
        lists:sort(Addrs)
     || #{name := ?PEER, listen_addrs := Addrs} <- sets:to_list(Specs)
    ]).

active_names(Mgr) ->
    {ok, Specs} = gen_server:call(Mgr, active, 5000),
    lists:sort([Name || #{name := Name} <- sets:to_list(Specs)]).

published_addrs() ->
    lists:sort([
        lists:sort(Addrs)
     || #{name := ?PEER, listen_addrs := Addrs} <- partisan_membership:members()
    ]).

conn_rows() ->
    lists:sort(
        [
            {Addr, Ch}
         || {partisan_peer_connection, _Pid, ?PEER, Ch, Addr, _} <-
                partisan_peer_connections:connections(?PEER)
        ]
    ).

beam_md5(Mod) ->
    case beam_lib:md5(code:which(Mod)) of
        {ok, {Mod, Md5}} ->
            lists:flatten([io_lib:format("~2.16.0b", [X]) || <<X>> <= Md5]);
        Other ->
            Other
    end.

flush_notify() ->
    receive
        {partisan_membership, _} -> 1 + flush_notify()
    after 0 -> 0
    end.

add_telemetry() ->
    ets:new(pubab_counts, [named_table, public, set]),
    telemetry:attach_many(
        pubab,
        [
            [partisan, membership, changed],
            [partisan, hyparview, active_view, peer_added]
        ],
        fun(Event, _M, _Meta, _Config) ->
            Key = list_to_atom(
                string:join([atom_to_list(A) || A <- Event], "_")
            ),
            ets:update_counter(pubab_counts, Key, {2, 1}, {Key, 0})
        end,
        #{}
    ).

telem_counts() ->
    maps:from_list(ets:tab2list(pubab_counts)).

diff_telem(Before, After) ->
    maps:map(fun(K, V) -> V - maps:get(K, Before, 0) end, After).
