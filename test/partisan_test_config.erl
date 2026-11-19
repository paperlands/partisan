%% Shared fixture for tests that call partisan_config:init/0. Snapshot and
%% restore both stores, and keep the membership table's lifecycle in one place.
-module(partisan_test_config).

-export([
    with_snapshot/1,
    reset_membership_table/0,
    delete_membership_table/0,
    stop_quietly/1
]).

with_snapshot(Fun) ->
    Before = snapshot(),
    try
        Fun()
    after
        restore(Before)
    end.

snapshot() ->
    Terms = [{Key, Value} || {Key, Value} <- persistent_term:get(), is_config_key(Key)],
    {Terms, application:get_all_env(partisan)}.

restore({Terms, Env}) ->
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

delete_membership_table() ->
    try ets:delete(partisan_membership) catch _:_ -> ok end.

stop_quietly(Pid) ->
    try gen_server:stop(Pid) catch _:_ -> exit(Pid, kill) end.
