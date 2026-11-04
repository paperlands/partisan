%% =============================================================================
%% @doc The config key list is exhaustive; a key read elsewhere but missing here
%% stays undefined. `tcp_keepalive` was in that position.
%% @end
%% =============================================================================
-module(partisan_config_test).

-include_lib("eunit/include/eunit.hrl").

config_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(_) ->
        [
            ?_test(keepalive_is_ingested()),
            ?_test(invalid_keepalive_yields_no_socket_options()),
            ?_test(oversized_keepalive_is_ignored()),
            ?_test(keepalive_is_absent_by_default())
        ]
    end}.

setup() ->
    Saved = application:get_all_env(partisan),
    ok = partisan_config:init(),
    Saved.

cleanup(Saved) ->
    [
        application:unset_env(partisan, Key)
     || {Key, _} <- application:get_all_env(partisan)
    ],
    [
        application:set_env(partisan, Key, Value)
     || {Key, Value} <- Saved
    ],
    ok = partisan_config:init(),
    ok.

keepalive_is_ingested() ->
    Keepalive = #{idle => 10, interval => 5, count => 3},
    application:set_env(partisan, tcp_keepalive, Keepalive),
    ok = partisan_config:init(),
    ?assertEqual(Keepalive, partisan_config:get(tcp_keepalive, undefined)),
    case os:type() of
        {unix, linux} ->
            %% Seconds: the native unit, and the v5 constants were 10/5/3.
            ?assertEqual(
                [
                    {raw, 6, 4, <<10:32/native>>},
                    {raw, 6, 5, <<5:32/native>>},
                    {raw, 6, 6, <<3:32/native>>}
                ],
                partisan_peer_socket:tcp_keepalive_opts()
            );
        {unix, darwin} ->
            ?assertEqual(
                [{raw, 6, 16#10, <<10:32/native>>}],
                partisan_peer_socket:tcp_keepalive_opts()
            );
        _ ->
            ?assertEqual([], partisan_peer_socket:tcp_keepalive_opts())
    end.

invalid_keepalive_yields_no_socket_options() ->
    application:set_env(partisan, tcp_keepalive, #{
        idle => 0,
        interval => -1,
        count => not_an_integer
    }),
    ok = partisan_config:init(),
    ?assertEqual([], partisan_peer_socket:tcp_keepalive_opts()).

oversized_keepalive_is_ignored() ->
    application:set_env(partisan, tcp_keepalive, #{
        idle => 16#100000000,
        interval => 5,
        count => 3
    }),
    ok = partisan_config:init(),
    ?assertEqual([], partisan_peer_socket:tcp_keepalive_opts()).

keepalive_is_absent_by_default() ->
    application:unset_env(partisan, tcp_keepalive),
    ok = partisan_config:init(),
    ?assertEqual(undefined, partisan_config:get(tcp_keepalive, undefined)),
    ?assertEqual([], partisan_peer_socket:tcp_keepalive_opts()).
