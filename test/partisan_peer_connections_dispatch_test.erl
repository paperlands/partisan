%% A row can outlive its process until the owner handles the EXIT. Dispatch
%% that trusts the index reports success and drops the payload. Reconsider
%% live rows in the same candidate set instead; pruning stays the owner's job.
-module(partisan_peer_connections_dispatch_test).
-include_lib("eunit/include/eunit.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(R, partisan_peer_connections).
-define(M, partisan_hyparview_peer_service_manager).

known_dead_row_redispatch_test_() ->
    {timeout, 60, fun() -> with_config_snapshot(fun known_dead_row_redispatch/0) end}.

known_dead_row_redispatch() ->
    {ok, _} = application:ensure_all_started(telemetry),
    ok = partisan_config:init(),
    ok = partisan_config:set(channels, #{
        data => #{parallelism => 2, monotonic => true}
    }),
    ok = partisan_config:set(peer_service_manager, ?M),
    ok = partisan_config:set(hyparview, #{
        shuffle_interval => 3600000,
        random_promotion => false,
        active_view_maintenance_interval => 3600000
    }),
    ok = partisan_config:set(channel_fallback, true),
    ok = partisan_config:set(disable_fast_forward, false),
    ok = reset_membership_table(),
    {ok, Owner} = ?M:start_link(),
    Parent = self(),
    Holders = [
        begin
            {ok, Pid} = gen_server:start(?MODULE, [Parent], []),
            Pid
        end
     || _ <- lists:seq(1, 4)
    ],
    [Old, New, OtherOld, Default] = Holders,
    OldAddr = #{ip => {127, 0, 0, 3}, port => 31001},
    NewAddr = #{ip => {127, 0, 0, 1}, port => 31001},
    Rows = [
        {mixed_fixture, Old, data, OldAddr},
        {mixed_fixture, New, data, NewAddr},
        {default_fixture, OtherOld, data, OldAddr},
        {default_fixture, Default, undefined, NewAddr}
    ],
    try
        _ = sys:replace_state(Owner, fun(State) ->
            lists:foreach(
                fun({Name, Pid, Channel, Addr}) ->
                    true = link(Pid),
                    ok = ?R:store(
                        #{name => Name, listen_addrs => [Addr], channels => #{}},
                        Pid,
                        Channel,
                        Addr
                    )
                end,
                Rows
            ),
            State
        end),
        Order = [?R:pid(Row) || Row <- ?R:connections(mixed_fixture, data)],
        Keys = lists:seq(0, length(Order) - 1),
        [OldKey] = [K || K <- Keys, lists:nth(K + 1, Order) =:= Old],
        ?assert(
            lists:all(
                fun(K) ->
                    {ok, lists:nth(K + 1, Order)} =:= ?R:dispatch_pid(mixed_fixture, data, K)
                end,
                Keys
            )
        ),
        ?assertEqual({ok, Default}, ?R:dispatch_pid(default_fixture, control, 0)),
        ok = partisan_config:set(channel_fallback, false),
        ?assertEqual({error, disconnected}, ?R:dispatch_pid(default_fixture, control, 0)),
        ok = partisan_config:set(channel_fallback, true),
        ok = sys:suspend(Owner),
        try
            exit_and_wait(Old),
            exit_and_wait(OtherOld),
            %% The owner is suspended: both dead rows are still indexed.
            ?assert(
                lists:member(Old, [?R:pid(Row) || Row <- ?R:connections(mixed_fixture, data)])
            ),
            ?assert(
                lists:member(OtherOld, [
                    ?R:pid(Row) || Row <- ?R:connections(default_fixture, data)
                ])
            ),
            ok = partisan_config:set(forward_options, #{partition_key => OldKey}),
            ?assertEqual(
                ok,
                ?R:dispatch({
                    forward_message,
                    mixed_fixture,
                    probe_receiver,
                    fixture_one,
                    #{channel => data}
                })
            ),
            ?assertEqual([], ?R:dispatch_many([mixed_fixture], probe_receiver, fixture_many, data)),
            ?assertEqual(
                ok,
                ?R:dispatch({
                    forward_message,
                    mixed_fixture,
                    undefined,
                    OldKey,
                    probe_receiver,
                    fixture_keyed,
                    #{channel => data}
                })
            ),
            %% Actual payloads, not just return values, reach the live replacement.
            ?assertEqual(
                [fixture_keyed, fixture_many, fixture_one], lists:sort(drain(New, 3, []))
            ),
            ?assertEqual({ok, New}, ?R:dispatch_pid(mixed_fixture, data, OldKey)),
            ?assert(
                lists:all(
                    fun(_) -> {ok, New} =:= ?R:dispatch_pid(mixed_fixture, data) end,
                    lists:seq(1, 32)
                )
            ),
            %% Exact-channel dispatch does not expand into the default channel.
            ?assertEqual({error, disconnected}, ?R:dispatch_pid(default_fixture, data, 0))
        after
            ok = sys:resume(Owner)
        end,
        ?assertEqual(ok, await_prune(mixed_fixture, Old, 200)),
        ?assertEqual(ok, await_prune(default_fixture, OtherOld, 200)),
        ?assertEqual({ok, New}, ?R:dispatch_pid(mixed_fixture, data, OldKey)),
        ?assertEqual({ok, Default}, ?R:dispatch_pid(default_fixture, data, 0))
    after
        stop_quietly(Owner),
        [stop_quietly(Holder) || Holder <- Holders],
        delete_membership_table()
    end,
    ok.

with_config_snapshot(Fun) ->
    Before = config_snapshot(),
    try
        Fun()
    after
        config_restore(Before)
    end.

%% The fixture calls init/0, which rewrites every partisan_config persistent
%% term and application env entry. Put both back so a later module in the same
%% EUnit VM sees the configuration it started with.
config_snapshot() ->
    Terms = [{Key, Value} || {Key, Value} <- persistent_term:get(), is_config_key(Key)],
    {Terms, application:get_all_env(partisan)}.

config_restore({Terms, Env}) ->
    [
        persistent_term:erase(Key)
     || Key <- [Key || {Key, _} <- persistent_term:get(), is_config_key(Key)]
    ],
    [persistent_term:put(Key, Value) || {Key, Value} <- Terms],
    [
        application:unset_env(partisan, Key)
     || Key <- proplists:get_keys(application:get_all_env(partisan))
    ],
    [application:set_env(partisan, Key, Value) || {Key, Value} <- Env],
    ok.

is_config_key({partisan_config, _}) -> true;
is_config_key(_) -> false.

reset_membership_table() ->
    case ets:info(partisan_membership, name) of
        undefined -> ok;
        _ -> delete_membership_table()
    end,
    _ = ets:new(partisan_membership, [named_table, public, set]),
    true = ets:insert(partisan_membership, [{version, 0}, {members, []}]),
    ok.

stop_quietly(Pid) ->
    try
        gen_server:stop(Pid, normal, 1000)
    catch
        _:_ -> exit(Pid, kill)
    end.

delete_membership_table() ->
    try ets:delete(partisan_membership) catch _:_ -> ok end.

init([Parent]) ->
    {ok, Parent}.

handle_call(_, _, Parent) ->
    {reply, ok, Parent}.

handle_cast({send_encoded, Data}, Parent) ->
    {forward_message, probe_receiver, Message} = binary_to_term(iolist_to_binary(Data)),
    Parent ! {received, self(), Message},
    {noreply, Parent}.

handle_info(_, Parent) ->
    {noreply, Parent}.

exit_and_wait(Pid) ->
    Monitor = monitor(process, Pid),
    exit(Pid, shutdown),
    receive
        {'DOWN', Monitor, process, Pid, shutdown} -> ok
    after 1000 -> error({no_down, Pid})
    end.

drain(_Pid, 0, Acc) ->
    Acc;
drain(Pid, N, Acc) ->
    receive
        {received, Pid, Message} -> drain(Pid, N - 1, [Message | Acc])
    after 200 -> Acc
    end.

await_prune(_Name, _Pid, 0) ->
    timeout;
await_prune(Name, Pid, Tries) ->
    case lists:any(fun(Row) -> ?R:pid(Row) =:= Pid end, ?R:connections(Name, data)) of
        false ->
            ok;
        true ->
            timer:sleep(5),
            await_prune(Name, Pid, Tries - 1)
    end.
