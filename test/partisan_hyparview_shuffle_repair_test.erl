%% A shuffle tick captures its exchange and self spec before it dials. If a
%% NEIGHBOR replaces self while the target stays active, the licensed
%% completion must rebuild the envelope: the receiver treats the sender as a
%% control address, so a stale spec recreates dial work to an old address.
-module(partisan_hyparview_shuffle_repair_test).
-include_lib("eunit/include/eunit.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(MGR, partisan_hyparview_peer_service_manager).

refresh_before_completion_sends_current_self_test_() ->
    {timeout, 30,
        fun() ->
            with_config_snapshot(fun refresh_before_completion_sends_current_self/0)
        end}.

refresh_before_completion_sends_current_self() ->
    {ok, _} = application:ensure_all_started(telemetry),
    ok = partisan_config:init(),
    ok = partisan_config:set(peer_service_manager, ?MGR),
    ok = partisan_config:set(hyparview, #{
        shuffle_interval => 3600000,
        random_promotion => false,
        active_view_maintenance_interval => 3600000
    }),
    ok = reset_membership_table(),
    {ok, Mgr} = ?MGR:start_link(),
    Parent = self(),
    {ok, Holder} = gen_server:start(?MODULE, [Parent], []),
    try
        Old = partisan:node_spec(),
        PeerAddr = #{ip => {127, 0, 0, 1}, port => 32552},
        Peer = #{name => shuffle_review_peer, listen_addrs => [PeerAddr], channels => #{}},
        Ref = make_ref(),
        %% A dialed attempt waiting for completion, with the target admitted.
        _ = sys:replace_state(Mgr, fun(State) ->
            true = link(Holder),
            ok = partisan_peer_connections:store(Peer, Holder, undefined, PeerAddr),
            Active = sets:add_element(Peer, element(5, State)),
            setelement(
                tuple_size(State), setelement(5, State, Active), {Ref, Holder, Peer}
            )
        end),
        NewAddr = #{ip => {127, 0, 0, 1}, port => 32553},
        ok = partisan_config:set(listen_addrs, [NewAddr]),
        updated = gen_server:call(Mgr, refresh_node_spec),
        #{listen_addrs := [NewAddr]} = New = partisan:node_spec(),
        ?assertNotEqual(Old, New),
        receive
            {control, {neighbor, New, _, _, _}} -> ok
        after 1000 -> error(no_refreshed_neighbor)
        end,
        %% The completion still holds the license: exact attempt, exact target
        %% spec, target active. Only the wire content was captured too early.
        Mgr ! {repair_finished, Ref, Holder, Peer, {shuffle, [Old], 1, Old}},
        receive
            {control, {shuffle, Exchange, 1, Sender}} ->
                ?assertEqual(New, Sender),
                ?assert(lists:member(New, Exchange)),
                ?assertNot(lists:member(Old, Exchange))
        after 1000 -> error(no_shuffle_completion)
        end
    after
        catch gen_server:stop(Mgr),
        catch gen_server:stop(Holder),
        catch ets:delete(partisan_membership)
    end,
    ok.

with_config_snapshot(Fun) ->
    Before = config_snapshot(),
    try
        Fun()
    after
        config_restore(Before)
    end.

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
        _ -> catch ets:delete(partisan_membership)
    end,
    _ = ets:new(partisan_membership, [named_table, public, set]),
    true = ets:insert(partisan_membership, [{version, 0}, {members, []}]),
    ok.

init([Parent]) ->
    {ok, Parent}.

handle_call({send_message, Message}, _From, Parent) ->
    Parent ! {control, Message},
    {reply, ok, Parent};
handle_call(_, _, Parent) ->
    {reply, ok, Parent}.

handle_cast(_, Parent) ->
    {noreply, Parent}.

handle_info(_, Parent) ->
    {noreply, Parent}.
