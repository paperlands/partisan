%% =============================================================================
%%  partisan_peer_discovery_agent.erl -
%%
%%  Copyright (c) 2022 Alejandro M. Ramallo. All rights reserved.
%%
%%  Licensed under the Apache License, Version 2.0 (the "License");
%%  you may not use this file except in compliance with the License.
%%  You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%%  Unless required by applicable law or agreed to in writing, software
%%  distributed under the License is distributed on an "AS IS" BASIS,
%%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%  See the License for the specific language governing permissions and
%%  limitations under the License.
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc This state machine polls a discovery backend (the callback module) and
%% joins the peers it reports.
%%
%% A discovery answer is a hint for joining, never a membership authority: the
%% agent only ever adds peers, through `partisan_peer_service:add_members/1'.
%% A member the backend stops reporting stays a member; a node leaves the
%% cluster only through `partisan_peer_service:leave/0,1'. The reason is that
%% every backend answers from a readiness- or health-gated, eventually
%% consistent source (DNS, an orchestrator's endpoint list), which omits a
%% node that is booting or briefly unhealthy. Treating that omission as a
%% departure removed the node from the membership and gossiped the removal,
%% and the node then shut its own peer service down on receiving it
%% ("membership doesn't contain us") — once per poll, for as long as it stayed
%% out of the answer. Pinned by `partisan_peer_discovery_agent_test' and
%% `partisan_SUITE:discovery_never_evicts_test/1'.
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_peer_discovery_agent).
-behaviour(gen_statem).

-include_lib("kernel/include/logger.hrl").
-include("partisan_util.hrl").

-record(state, {
    enabled :: boolean(),
    callback_mod :: module() | undefined,
    callback_config :: map() | undefined,
    callback_state :: any() | undefined,
    initial_delay :: integer() | undefined,
    polling_interval :: integer() | undefined,
    timeout :: integer() | undefined,
    peers = [] :: [partisan:node_spec()],
    %% Containment: the callback runs in a monitored worker, never here.
    initialised = false :: boolean(),
    pending = undefined :: undefined | pending(),
    retry_base = 1000 :: pos_integer(),
    retry_max = 60000 :: pos_integer(),
    consecutive_failures = 0 :: non_neg_integer(),
    %% Callers of `lookup/0' waiting for the in-flight callback.
    waiters = [] :: [gen_statem:from()],
    last_error = undefined :: undefined | term()
}).

-type state() :: #state{}.
-type pending() :: {reference(), pid(), reference(), reference(), init | lookup}.

%% API
-export([start/0]).
-export([start_link/0]).
-export([lookup/0]).
-export([enable/0]).
-export([disable/0]).
-export([status/0]).
-export([info/0]).

%% gen_statem callbacks
-export([init/1]).
-export([callback_mode/0]).
-export([terminate/3]).
-export([code_change/4]).

%% gen_statem states
-export([enabled/3]).
-export([disabled/3]).

%% =============================================================================
%% CALLBACKS
%% =============================================================================

%% -----------------------------------------------------------------------------
%% Initializes the peer discovery agent implementation
%% -----------------------------------------------------------------------------
-callback init(Opts :: map()) ->
    {ok, State :: any()}
    | {error, Reason :: any()}.

%% -----------------------------------------------------------------------------
%%
%% -----------------------------------------------------------------------------
-callback lookup(State :: any(), Timeout :: timeout()) ->
    {ok, [partisan:node_spec()], NewState :: any()}
    | {error, Reason :: any(), NewState :: any()}.

%% =============================================================================
%% API
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec start() ->
    {ok, pid()} | ignore | {error, term()}.

start() ->
    Opts = partisan_config:get(peer_discovery, #{}),
    gen_statem:start({local, ?MODULE}, ?MODULE, [Opts], []).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec start_link() ->
    {ok, pid()} | ignore | {error, term()}.

start_link() ->
    Opts = partisan_config:get(peer_discovery, #{}),
    gen_statem:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

%% -----------------------------------------------------------------------------
%% @doc Report what the discovery loop last saw.
%%
%% A barrier: joins the in-flight callback or starts one, returning
%% `{ok, Peers}' or `{error, Classified}'. For the last answer use `info/0'.
%% @end
%% -----------------------------------------------------------------------------
lookup() ->
    gen_statem:call(?MODULE, lookup, lookup_timeout()).

%% Caller-side bound. The agent's deadline is per callback; an uninitialised
%% agent may run init then lookup. Keep it finite.
lookup_timeout() ->
    Opts = partisan_config:get(peer_discovery, #{}),
    Timeout =
        case Opts of
            #{timeout := T} when is_integer(T), T > 0 -> T;
            _ -> 5000
        end,
    2 * Timeout + 5000.

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enable() -> ok.

enable() ->
    gen_statem:call(?MODULE, enable, 5000).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec disable() -> ok.

disable() ->
    gen_statem:call(?MODULE, disable, 5000).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec status() -> enabled | disabled.

status() ->
    gen_statem:call(?MODULE, status, 5000).

%% -----------------------------------------------------------------------------
%% @doc Health of the discovery loop: initialisation, consecutive failures, last
%% error, whether a callback is in flight, and the last peer list.
%% @end
-spec info() -> map().

info() ->
    gen_statem:call(?MODULE, info, 5000).

%% =============================================================================
%% GEN_STATEM CALLBACKS
%% =============================================================================

init([#{enabled := true, type := Mod} = Opts]) when is_atom(Mod) ->
    CBConfig = maps:get(config, Opts, #{}),
    Delay = normalise_delay(maps:get(initial_delay, Opts, timer:seconds(10))),
    Interval = normalise_interval(maps:get(polling_interval, Opts, timer:seconds(60))),
    Timeout = normalise_timeout(maps:get(timeout, Opts, timer:seconds(5))),
    RetryBase = normalise_retry(maps:get(retry_base, Opts, 1000), 1000),
    RetryMax = normalise_retry(maps:get(retry_max, Opts, 60000), 60000),

    %% Run init through the contained path: a failing backend must not take the
    %% permanent agent (and the plane) down with it.
    State = #state{
        enabled = true,
        callback_mod = Mod,
        callback_config = CBConfig,
        initial_delay = Delay,
        polling_interval = Interval,
        timeout = Timeout,
        retry_base = RetryBase,
        retry_max = RetryMax
    },

    Action =
        case Delay > 0 of
            true ->
                ?LOG_INFO(#{
                    description =>
                        "Peer discovery agent will start after initial delay",
                    delay_msecs => Delay
                }),
                {state_timeout, Delay, lookup, []};
            false ->
                {next_event, internal, init}
        end,

    {ok, enabled, State, [Action]};
init([#{enabled := true} = Opts]) ->
    {stop, {invalid_config, Opts}};
init([Opts]) when is_list(Opts) ->
    init([maps:from_list(Opts)]);
init(_) ->
    State = #state{enabled = false},
    {ok, disabled, State}.

callback_mode() ->
    state_functions.

terminate(_Reason, _StateName, #state{} = State) ->
    %% Kill a hung worker and cancel its timer. A brutal kill bypasses this.
    _ = cancel(State),
    ok;
terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% =============================================================================
%% STATE FUNCTIONS
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc In this state the agent uses the callback module to discover peers
%% by calling its lookup/2 callback.
%% @end
%% -----------------------------------------------------------------------------
enabled({call, From}, enable, _State) ->
    ok = gen_statem:reply(From, ok),
    keep_state_and_data;
enabled({call, From}, disable, State) ->
    ok = gen_statem:reply(From, ok),
    %% Tell waiters now, not at their call timeout or on a later enable.
    {next_state, disabled, cancel(reply_waiters(State, {error, disabled}))};
enabled({call, From}, lookup, #state{pending = Pending, waiters = Waiters} = State) ->
    %% Waiters are answered when the in-flight (or started) callback completes.
    case Pending of
        undefined ->
            {keep_state, State#state{waiters = [From | Waiters]},
                [{next_event, internal, lookup}]};
        _ ->
            {keep_state, State#state{waiters = [From | Waiters]}}
    end;
enabled(state_timeout, lookup, State) ->
    %% The polling interval timeout, we need to perform a lookup
    {keep_state, State, [{next_event, internal, lookup}]};
enabled(internal, resume, State) ->
    %% The operation is recorded in `pending'; do not re-derive it from state.
    case State#state.initialised of
        true -> start_callback(lookup, State);
        false -> start_callback(init, State)
    end;
enabled(internal, init, State) ->
    start_callback(init, State);
enabled(internal, lookup, #state{initialised = false} = State) ->
    start_callback(init, State);
enabled(internal, lookup, State) ->
    start_callback(lookup, State);
enabled(EventType, EventContent, State) ->
    handle_common_event(EventType, EventContent, enabled, State).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
disabled({call, From}, disable, _State) ->
    ok = gen_statem:reply(From, ok),
    keep_state_and_data;
disabled({call, From}, enable, #state{callback_mod = undefined}) ->
    ok = gen_statem:reply(From, {error, missing_configuration}),
    keep_state_and_data;
disabled({call, From}, enable, State) ->
    ok = gen_statem:reply(From, ok),
    {next_state, enabled, State, [{next_event, internal, resume}]};
disabled({call, From}, lookup, _) ->
    ok = gen_statem:reply(From, {error, disabled}),
    keep_state_and_data;
disabled(EventType, EventContent, State) ->
    handle_common_event(EventType, EventContent, disabled, State).

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
handle_common_event({call, From}, status, StateName, _State) ->
    ok = gen_statem:reply(From, StateName),
    keep_state_and_data;
%% The ref ties a result to its request; stale answers are ignored.
handle_common_event(info, {'$partisan_discovery_result', Ref, Result}, StateName, State) ->
    complete(Ref, Result, StateName, State);
handle_common_event(info, {'DOWN', MRef, process, _Pid, Reason}, StateName, State) ->
    complete_down(MRef, Reason, StateName, State);
handle_common_event(info, {timeout, TRef, {'$partisan_discovery_timeout', Ref}}, StateName, State) ->
    complete_timeout(TRef, Ref, StateName, State);
handle_common_event({call, From}, info, _StateName, State) ->
    ok = gen_statem:reply(From, info_map(State)),
    keep_state_and_data;
handle_common_event(EventType, EventContent, _StateName, State) ->
    case EventType of
        {call, From} ->
            ok = gen_statem:reply(From, {error, unknown_call});
        _ ->
            ok
    end,

    ?LOG_DEBUG(#{
        description => "Unhandled event",
        callback_mod => State#state.callback_mod,
        type => EventType,
        content => EventContent
    }),

    keep_state_and_data.

%% @private
%% @doc Run one callback out of process under our deadline.
%%
%% The worker is killed at the deadline; its failures become values, so nothing
%% unwinds into the agent.
start_callback(Fun, #state{pending = undefined} = State) ->
    #state{
        callback_mod = Mod,
        callback_config = CBConfig,
        callback_state = CBState,
        timeout = Timeout
    } = State,
    Args =
        case Fun of
            init -> [CBConfig];
            lookup -> [CBState, Timeout]
        end,
    Parent = self(),
    Ref = make_ref(),
    {Pid, MRef} = spawn_monitor(fun() ->
        Parent ! {'$partisan_discovery_result', Ref, safe_apply(Mod, Fun, Args)}
    end),
    TRef = erlang:start_timer(Timeout, self(), {'$partisan_discovery_timeout', Ref}),
    {keep_state, State#state{pending = {Ref, Pid, MRef, TRef, Fun}}};
start_callback(_Fun, _State) ->
    %% A request is already in flight; its answer will schedule the next one.
    keep_state_and_data.

%% @private
complete(Ref, Result, StateName, #state{pending = {Ref, _, MRef, TRef, Op}} = State) ->
    cleanup_pending(MRef, TRef, undefined),
    apply_result(Op, Result, StateName, State#state{pending = undefined});
complete(_StaleRef, _Result, _StateName, _State) ->
    %% Late answer to a cancelled or superseded request.
    keep_state_and_data.

%% @private
complete_down(MRef, Reason, StateName, #state{pending = {_, Pid, MRef, TRef, Op}} = State) ->
    cleanup_pending(MRef, TRef, Pid),
    apply_result(Op, {'$down', Reason}, StateName, State#state{pending = undefined});
complete_down(_MRef, _Reason, _StateName, _State) ->
    keep_state_and_data.

%% @private
complete_timeout(TRef, Ref, StateName, #state{pending = {Ref, Pid, MRef, TRef, Op}} = State) ->
    exit(Pid, kill),
    cleanup_pending(MRef, TRef, undefined),
    apply_result(Op, '$timeout', StateName, State#state{pending = undefined});
complete_timeout(_TRef, _Ref, _StateName, _State) ->
    keep_state_and_data.

%% @private
apply_result(init, Result, StateName, State) ->
    case classify_init(Result) of
        {ok, CBState} ->
            ?LOG_INFO(#{
                description => "Peer discovery backend initialised",
                callback_mod => State#state.callback_mod
            }),
            %% No observation yet: nobody is answered on init.
            State1 = State#state{
                initialised = true,
                callback_state = CBState,
                consecutive_failures = 0,
                last_error = undefined
            },
            %% Poll immediately.
            {keep_state, State1, [{next_event, internal, lookup}]};
        {error, Classified} ->
            failure(init, Classified, StateName, State)
    end;
apply_result(lookup, Result, StateName, #state{} = State) ->
    case classify_lookup(Result) of
        {ok, Peers, CBState} ->
            %% Hand the whole list to the manager; add_members/1 only adds.
            %%
            %% Keep the backend state even when the handoff fails, but count the
            %% handoff as a failed poll and answer waiters with it.
            State1 = State#state{callback_state = CBState},
            case safe_add_members(Peers) of
                ok ->
                    ?LOG_DEBUG(#{
                        description => "Got peer discovery lookup response",
                        callback_mod => State#state.callback_mod,
                        response => Peers
                    }),
                    State2 = reply_waiters(
                        State1#state{
                            peers = Peers,
                            consecutive_failures = 0,
                            last_error = undefined
                        },
                        {ok, Peers}
                    ),
                    schedule(State2, State2#state.polling_interval, StateName);
                {error, Classified} ->
                    failure(lookup, Classified, StateName, State1)
            end;
        {error, Classified} ->
            failure(lookup, Classified, StateName, State)
    end.

%% @private
failure(Fun, Classified, StateName, #state{consecutive_failures = N} = State) ->
    Failures = N + 1,
    Delay = backoff(Failures, State),
    ?LOG_WARNING(#{
        description => "Peer discovery callback failed; keeping last known peers",
        callback_mod => State#state.callback_mod,
        callback => Fun,
        error => Classified,
        consecutive_failures => Failures,
        retry_in_ms => Delay
    }),
    State1 = reply_waiters(
        State#state{
            consecutive_failures = Failures,
            last_error = Classified,
        %% Init failure stays uninitialised; lookup failure keeps the old state.
            initialised = State#state.initialised andalso Fun =:= lookup
        },
        {error, Classified}
    ),
    schedule(State1, Delay, StateName).

%% @private
%% add_members/1 is our machinery; a failing manager must not kill the poll.
safe_add_members(Peers) ->
    try
        partisan_peer_service:add_members(Peers)
    catch
        Class:Reason ->
            ?LOG_WARNING(#{
                description => "Peer discovery could not hand peers to the manager",
                class => Class,
                reason => Reason
            }),
            {error, {handoff, {Class, Reason}}}
    end.

%% @private
reply_waiters(#state{waiters = Waiters} = State, Reply) ->
    _ = [gen_statem:reply(From, Reply) || From <- Waiters],
    State#state{waiters = []}.

%% @private
schedule(State, Delay, StateName) ->
    Action = {state_timeout, max(Delay, 1), lookup},
    case StateName of
        enabled -> {keep_state, State, [Action]};
        disabled -> {keep_state, State}
    end.

%% @private
%% Exponential from `retry_base', capped at `retry_max'. Independent of
%% `polling_interval' so the delay grows instead of tracking the poll cadence.
backoff(Failures, #state{retry_base = Base, retry_max = Max}) ->
    min(Base * (1 bsl min(max(Failures, 1) - 1, 16)), Max).

%% @private
cancel(#state{pending = undefined} = State) ->
    State;
cancel(#state{pending = {_, Pid, MRef, TRef, _Op}} = State) ->
    cleanup_pending(MRef, TRef, Pid),
    State#state{pending = undefined}.

%% @private
cleanup_pending(MRef, TRef, Pid) ->
    case Pid of
        undefined -> ok;
        _ -> exit(Pid, kill)
    end,
    case TRef of
        undefined -> ok;
        _ -> erlang:cancel_timer(TRef)
    end,
    erlang:demonitor(MRef, [flush]),
    ok.

%% @private
%% A bad timeout would disable the containment deadline.
normalise_timeout(T) when is_integer(T), T > 0 -> T;
normalise_timeout(_) -> 5000.

%% @private
%% Timer inputs: fall back to defaults instead of crashing.
normalise_delay(Delay) when is_integer(Delay), Delay >= 0 -> Delay;
normalise_delay(_) -> timer:seconds(10).

normalise_interval(Interval) when is_integer(Interval), Interval > 0 -> Interval;
normalise_interval(_) -> timer:seconds(60).

normalise_retry(Value, _Default) when is_integer(Value), Value > 0 -> Value;
normalise_retry(_, Default) -> Default.

%% @private
safe_apply(Mod, Fun, Args) ->
    try
        apply(Mod, Fun, Args)
    catch
        Class:Reason:Stacktrace ->
            {'$exception', Class, Reason, Stacktrace}
    end.

%% @private
%% undef at the boundary means the backend is unavailable; deeper, it is a bug.
undef(Fun, [{Mod, Fun, _, _} | _]) when Fun =:= init; Fun =:= lookup ->
    case code:is_loaded(Mod) of
        false -> {error, module_missing};
        _ -> {error, {raised, error, undef}}
    end;
undef(_Fun, _Stack) ->
    {error, {raised, error, undef}}.

%% @private
classify_init({ok, CBState}) -> {ok, CBState};
classify_init({error, Reason}) -> {error, {callback_error, Reason}};
classify_init({'$exception', error, undef, Stack}) -> undef(init, Stack);
classify_init({'$exception', Class, Reason, _}) -> {error, {raised, Class, Reason}};
classify_init('$timeout') -> {error, timed_out};
classify_init({'$down', Reason}) -> {error, {crashed, Reason}};
classify_init(Other) -> {error, {malformed, Other}}.

%% @private
classify_lookup({ok, Peers, CBState}) ->
    case valid_peers(Peers) of
        true -> {ok, Peers, CBState};
        false -> {error, {malformed, Peers}}
    end;
classify_lookup({error, Reason, _CBState}) -> {error, {callback_error, Reason}};
classify_lookup({error, Reason}) -> {error, {callback_error, Reason}};
classify_lookup({'$exception', error, undef, Stack}) -> undef(lookup, Stack);
classify_lookup({'$exception', Class, Reason, _}) -> {error, {raised, Class, Reason}};
classify_lookup('$timeout') -> {error, timed_out};
classify_lookup({'$down', Reason}) -> {error, {crashed, Reason}};
classify_lookup(Other) -> {error, {malformed, Other}}.

%% @private
%% A malformed answer is a failed observation, not a partial update. The shape
%% check keeps add_members/1 from receiving a map it would crash on.
valid_peers(Peers) when is_list(Peers) ->
    lists:all(fun valid_peer/1, Peers);
valid_peers(_) ->
    false.

%% @private
valid_peer(#{name := Name, listen_addrs := Addrs} = Peer) when is_atom(Name), is_list(Addrs) ->
    IsMap =
        case maps:get(channels, Peer, #{}) of
            Channels when is_map(Channels) -> true;
            _ -> false
        end,
    IsMap andalso lists:all(fun valid_listen_addr/1, Addrs);
valid_peer(_) ->
    false.

%% @private
valid_listen_addr(#{ip := {A, B, C, D}, port := Port}) ->
    valid_octets([A, B, C, D]) andalso valid_port(Port);
valid_listen_addr(#{ip := {A, B, C, D, E, F, G, H}, port := Port}) ->
    valid_segments([A, B, C, D, E, F, G, H]) andalso valid_port(Port);
valid_listen_addr(_) ->
    false.

%% @private
valid_port(Port) ->
    is_integer(Port) andalso Port > 0 andalso Port < 65536.

%% @private
valid_octets(Values) ->
    lists:all(fun(X) -> is_integer(X) andalso X >= 0 andalso X =< 255 end, Values).

%% @private
valid_segments(Values) ->
    lists:all(fun(X) -> is_integer(X) andalso X >= 0 andalso X =< 65535 end, Values).

%% @private
info_map(State) ->
    #{
        callback_mod => State#state.callback_mod,
        initialised => State#state.initialised,
        consecutive_failures => State#state.consecutive_failures,
        last_error => State#state.last_error,
        pending => State#state.pending =/= undefined,
        peers => length(State#state.peers),
        last_peers => State#state.peers
    }.
