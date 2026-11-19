-module(partisan_hyparview_repair_test).
-include_lib("eunit/include/eunit.hrl").

-define(MGR, partisan_hyparview_peer_service_manager).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

stale_completion_cannot_store_test() ->
    with_manager(fun(Mgr) ->
        Old = spec(31111),
        New = spec(32222),
        NewClient = holder(),
        Candidate = holder(),
        Ref = make_ref(),
        Parent = self(),
        Worker = spawn(fun() ->
            receive
                {repair_commit, Ref, Candidate, Outcome} ->
                    exit(Candidate, shutdown),
                    Mgr ! {repair_done, Ref, self(), Old, shuffle, cancelled},
                    Parent ! {completion, Outcome}
            end
        end),
        seed(Mgr, Old, {Ref, Worker, Old}, [{New, NewClient}]),
        neighbor(Mgr, New),
        ?assertEqual([New], peer_specs(Mgr)),
        Version = partisan_membership:version(),
        Mgr !
            {repair_connected, Ref, Worker, Old, Candidate, undefined,
                addr(31111), #{parallelism => 1}},
        receive
            {completion, Outcome} -> ?assertEqual(discarded, Outcome)
        after 1000 -> error(no_completion)
        end,
        ?assertEqual(0, partisan_peer_connections:count(Old)),
        ?assertEqual(1, partisan_peer_connections:count(New)),
        ?assertEqual(Version, partisan_membership:version()),
        ?assertEqual([New], peer_specs(Mgr)),
        ?assert(wait(fun() -> not is_process_alive(Candidate) end))
    end).

current_completion_is_owner_committed_test() ->
    with_manager(fun(Mgr) ->
        Old = spec(31111),
        Candidate = holder(),
        Ref = make_ref(),
        Parent = self(),
        Worker = spawn(fun() ->
            receive
                {repair_commit, Ref, Candidate, Outcome} ->
                    Mgr ! {repair_done, Ref, self(), Old, shuffle, done},
                    Parent ! {completion, Outcome}
            end
        end),
        seed(Mgr, Old, {Ref, Worker, Old}, []),
        Version = partisan_membership:version(),
        Mgr !
            {repair_connected, Ref, Worker, Old, Candidate, undefined,
                addr(31111), #{parallelism => 1}},
        receive
            {completion, Outcome} -> ?assertEqual(committed, Outcome)
        after 1000 -> error(no_completion)
        end,
        ?assertEqual(1, partisan_peer_connections:count(Old)),
        ?assertEqual(Mgr, ets:info(partisan_peer_connections, owner)),
        ?assertEqual(
            protected, ets:info(partisan_peer_connections, protection)
        ),
        ?assertEqual(Version, partisan_membership:version())
    end).

crash_reports_one_outcome_test() ->
    with_manager(fun(Mgr) ->
        Peer = spec(31111),
        Ref = make_ref(),
        Worker = spawn(fun() ->
            receive
                stop -> ok
            end
        end),
        seed(Mgr, Peer, {Ref, Worker, Peer}, []),
        %% A real attempt is spawn_link'ed by the owner, not by the test.
        _ = sys:replace_state(Mgr, fun(State) ->
            true = link(Worker),
            State
        end),
        Parent = self(),
        Handler = {?MODULE, Ref},
        ok = telemetry:attach(
            Handler,
            [partisan, hyparview, repair, outcome],
            fun(_Event, _Measurements, Metadata, _Config) ->
                Parent ! {outcome, Metadata}
            end,
            undefined
        ),
        exit(Worker, boom),
        ?assert(wait(fun() -> repair(Mgr) =:= undefined end)),
        receive
            {outcome, #{
                attempt := Ref, peer_node := Peer, outcome := {crashed, boom}
            }} ->
                ok
        after 1000 -> error(no_crash_outcome)
        end,
        receive
            {outcome, _} -> error(duplicate_outcome)
        after 100 -> ok
        end,
        ok = telemetry:detach(Handler)
    end).

cancelled_attempt_crash_keeps_attribution_test() ->
    with_manager(fun(Mgr) ->
        Old = spec(31111),
        New = spec(32222),
        Ref = make_ref(),
        Worker = spawn(fun() ->
            receive
                stop -> ok
            end
        end),
        NewClient = holder(),
        seed(Mgr, Old, {Ref, Worker, Old}, [{New, NewClient}]),
        _ = sys:replace_state(Mgr, fun(State) ->
            true = link(Worker),
            State
        end),
        neighbor(Mgr, New),
        ?assertMatch({cancelled, _, Worker, _}, repair(Mgr)),
        Parent = self(),
        Handler = {?MODULE, {cancel_crash, Ref}},
        ok = telemetry:attach(
            Handler,
            [partisan, hyparview, repair, outcome],
            fun(_Event, _Measurements, Metadata, _Config) ->
                Parent ! {outcome, Metadata}
            end,
            undefined
        ),
        exit(Worker, boom),
        ?assert(wait(fun() -> repair(Mgr) =:= undefined end)),
        receive
            {outcome, #{
                attempt := Ref, peer_node := Old, outcome := {crashed, boom}
            }} ->
                ok
        after 1000 -> error(no_crash_outcome)
        end,
        ok = telemetry:detach(Handler)
    end).

publication_changes_once_and_noop_is_silent_test() ->
    with_manager(fun(Mgr) ->
        Old = spec(31111),
        Client = holder(),
        seed(Mgr, undefined, undefined, [{Old, Client}]),
        Version = partisan_membership:version(),
        neighbor(Mgr, Old),
        ?assertEqual(Version + 1, partisan_membership:version()),
        ?assert(lists:member(Old, partisan_membership:members())),
        neighbor(Mgr, Old),
        ?assertEqual(Version + 1, partisan_membership:version())
    end).

cancelled_attempt_cannot_regain_its_license_test() ->
    with_manager(fun(Mgr) ->
        Old = spec(31111),
        New = spec(32222),
        Ref = make_ref(),
        Worker = holder(),
        NewClient = holder(),
        seed(Mgr, Old, {Ref, Worker, Old}, [{New, NewClient}]),
        neighbor(Mgr, New),
        ?assertMatch({cancelled, _, Worker, _}, repair(Mgr)),
        sys:replace_state(Mgr, fun(State) ->
            setelement(5, State, sets:add_element(Old, element(5, State)))
        end),
        Parent = self(),
        Mgr ! {repair_dial, Ref, Worker},
        gen_server:call(Mgr, get_local_state, 1000),
        ?assertMatch({cancelled, _, Worker, _}, repair(Mgr)),
        LateClient = holder(),
        Mgr !
            {repair_connected, Ref, Worker, Old, LateClient, undefined,
                addr(31111), #{parallelism => 1}},
        gen_server:call(Mgr, get_local_state, 1000),
        ?assertEqual(0, partisan_peer_connections:count(Old)),
        gen_server:stop(LateClient),
        Mgr ! {repair_done, Ref, Worker, Old, shuffle, cancelled},
        gen_server:call(Mgr, get_local_state, 1000),
        ?assertEqual(undefined, repair(Mgr)),
        sys:replace_state(Mgr, fun(State) ->
            {noreply, Next} = ?MGR:handle_info(passive_view_maintenance, State),
            {NextRef, NextWorker, _} = element(tuple_size(Next), Next),
            Parent ! {new_license, NextRef, NextWorker},
            Next
        end),
        receive
            {new_license, NextRef, NextWorker} ->
                ?assertNotEqual(Ref, NextRef),
                ?assertNotEqual(Worker, NextWorker)
        after 1000 -> error(no_new_license)
        end,
        gen_server:stop(Worker)
    end).

plumtree_init_reads_watermark_before_names_test() ->
    %% Under cover `code:which/1' answers `cover_compiled', which has no beam
    %% file to disassemble. The on-disk artifact is the code cover wrapped.
    Beam = code:where_is_file(
        atom_to_list(partisan_plumtree_broadcast) ++ ".beam"
    ),
    ?assertNotEqual(non_existing, Beam),
    {beam_file, _, _, _, _, Functions} = beam_disasm:file(Beam),
    {function, init, 1, _, Instructions} = lists:keyfind(init, 2, Functions),
    Reads = lists:filtermap(
        fun(Instruction) ->
            case lists:last(tuple_to_list(Instruction)) of
                {extfunc, partisan_membership, Function, 0} -> {true, Function};
                _ -> false
            end
        end,
        [I || I <- Instructions, is_tuple(I)]
    ),
    ?assertEqual([version, node_names], Reads).

current_real_tcp_repair_uses_owner_commit_test() ->
    with_manager(fun(Mgr) ->
        {ok, Listen} = gen_tcp:listen(0, [
            binary, {active, false}, {ip, {127, 0, 0, 1}}
        ]),
        {ok, {_, Port}} = inet:sockname(Listen),
        Server = spawn(fun() ->
            {ok, Socket} = gen_tcp:accept(Listen),
            receive
                stop -> gen_tcp:close(Socket)
            end
        end),
        Peer = (spec(Port))#{
            channels => #{undefined => #{parallelism => 1, monotonic => false}}
        },
        try
            seed(Mgr, Peer, undefined, []),
            Version = partisan_membership:version(),
            Mgr ! passive_view_maintenance,
            ?assert(
                wait(fun() -> partisan_peer_connections:count(Peer) =:= 1 end)
            ),
            [Row] = partisan_peer_connections:connections(Peer),
            Client = partisan_peer_connections:pid(Row),
            ?assert(is_process_alive(Client)),
            {links, Links} = process_info(Client, links),
            ?assert(lists:member(Mgr, Links)),
            ?assertEqual(Mgr, ets:info(partisan_peer_connections, owner)),
            ?assertEqual(Version, partisan_membership:version()),
            ?assert(wait(fun() -> repair(Mgr) =:= undefined end))
        after
            Server ! stop,
            gen_tcp:close(Listen)
        end
    end).

with_manager(Fun) ->
    {ok, _} = application:ensure_all_started(telemetry),
    ok = partisan_config:init(),
    ok = partisan_config:set(peer_service_manager, ?MGR),
    ok = partisan_config:set(hyparview, #{
        shuffle_interval => 3600000,
        random_promotion_interval => 3600000,
        active_view_maintenance_interval => 3600000
    }),
    _ = ets:new(partisan_membership, [named_table, public, set]),
    {ok, Mgr} = ?MGR:start_link(),
    try
        Fun(Mgr)
    after
        gen_server:stop(Mgr),
        ets:delete(partisan_membership)
    end.

seed(Mgr, Peer, Repair, Rows) ->
    sys:replace_state(Mgr, fun(State) ->
        lists:foreach(
            fun({Spec, Pid}) ->
                link(Pid),
                ok = partisan_peer_connections:store(
                    Spec, Pid, undefined, hd(maps:get(listen_addrs, Spec))
                )
            end,
            Rows
        ),
        Active =
            case Peer of
                undefined -> element(5, State);
                _ -> sets:add_element(Peer, element(5, State))
            end,
        setelement(tuple_size(State), setelement(5, State, Active), Repair)
    end).

neighbor(Mgr, Peer) ->
    gen_server:cast(
        Mgr,
        {receive_message, Peer, undefined,
            {neighbor, Peer, undefined, undefined, partisan:node_spec()}}
    ),
    gen_server:call(Mgr, get_local_state, 1000).

peer_specs(Mgr) ->
    {ok, {state, Active, _}} = gen_server:call(Mgr, get_local_state, 1000),
    [S || #{name := 'repair-test@peer'} = S <- sets:to_list(Active)].

repair(Mgr) ->
    State = sys:get_state(Mgr),
    element(tuple_size(State), State).

holder() ->
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    Pid.
spec(Port) ->
    #{
        name => 'repair-test@peer',
        listen_addrs => [addr(Port)],
        channels => #{}
    }.
addr(Port) -> #{ip => {127, 0, 0, 1}, port => Port}.

wait(Fun) -> wait(Fun, 100).
wait(_Fun, 0) ->
    false;
wait(Fun, N) ->
    case Fun() of
        true ->
            true;
        false ->
            timer:sleep(5),
            wait(Fun, N - 1)
    end.

init([]) -> {ok, undefined}.
handle_call(_, _, State) -> {reply, ok, State}.
handle_cast(_, State) -> {noreply, State}.
handle_info(_, State) -> {noreply, State}.
