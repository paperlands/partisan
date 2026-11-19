%% A shuffle attempt carries intent only. If a NEIGHBOR replaces self while
%% the target stays active, the licensed completion must derive the envelope
%% from current state: the receiver treats the sender as a control address, so
%% a stale spec recreates dial work to an old address.
-module(partisan_hyparview_shuffle_repair_test).
-include_lib("eunit/include/eunit.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(MGR, partisan_hyparview_peer_service_manager).

intent_completion_sends_current_self_test_() ->
    {timeout, 30, fun() -> partisan_test_config:with_snapshot(fun run/0) end}.

run() ->
    {ok, _} = application:ensure_all_started(telemetry),
    ok = partisan_config:init(),
    ok = partisan_config:set(peer_service_manager, ?MGR),
    ok = partisan_config:set(hyparview, #{
        shuffle_interval => 3600000,
        random_promotion => false,
        active_view_maintenance_interval => 3600000
    }),
    ok = partisan_test_config:reset_membership_table(),
    {ok, Mgr} = ?MGR:start_link(),
    Parent = self(),
    {ok, Holder} = gen_server:start(?MODULE, [Parent], []),
    try
        Old = partisan:node_spec(),
        PeerAddr = #{ip => {127, 0, 0, 1}, port => 32552},
        Peer = #{
            name => shuffle_review_peer,
            listen_addrs => [PeerAddr],
            channels => #{}
        },
        Ref = make_ref(),
        %% A dialed attempt waiting for completion, with the target admitted.
        _ = sys:replace_state(Mgr, fun(State) ->
            true = link(Holder),
            ok = partisan_peer_connections:store(
                Peer, Holder, undefined, PeerAddr
            ),
            Active = sets:add_element(Peer, element(5, State)),
            setelement(
                tuple_size(State),
                setelement(5, State, Active),
                {Ref, Holder, Peer}
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
        Arwl = maps:get(active_rwl, partisan_config:get(hyparview)),
        %% The completion still holds the license: exact attempt, exact target
        %% spec, target active. Only the envelope is derived late.
        Mgr ! {repair_done, Ref, Holder, Peer, shuffle, done},
        receive
            {control, {shuffle, Exchange, Ttl, Sender}} ->
                ?assertEqual(New, Sender),
                ?assertEqual(Arwl, Ttl),
                ?assert(lists:member(New, Exchange)),
                ?assertNot(lists:member(Old, Exchange))
        after 1000 -> error(no_shuffle_completion)
        end
    after
        partisan_test_config:stop_quietly(Mgr),
        partisan_test_config:stop_quietly(Holder),
        partisan_test_config:delete_membership_table()
    end,
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
