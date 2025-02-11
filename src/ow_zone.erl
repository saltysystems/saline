-module(ow_zone).
-behaviour(gen_server).

% gen_server
-export([start/3, start/4]).
-export([start_link/3, start_link/4]).
-export([start_monitor/3, start_monitor/4]).
-export([call/2, call/3]).
-export([cast/2]).
-export([reply/2]).
-export([stop/1, stop/3]).

-export([
    join/3,
    part/3,
    disconnect/2,
    reconnect/2,
    rpc/4,
    broadcast/2,
    send/3
]).

% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%%=======================================================================
%% Internal State, default configurations
%%=======================================================================

-define(TAG_I(Msg), {'$ow_zone_internal', Msg}).

-record(state, {
    cb_mod :: module(),
    cb_data :: term(),
    zone_data :: zone_data()
}).

% DEFAULT_LERP_MS/DEFAULT_TICK_MS = 4 packets buffered by default
-define(DEFAULT_LERP_MS, 80).
-define(DEFAULT_TICK_MS, 20).

-define(INITIAL_ZONE_DATA, #{
    clients => [],
    frame => 0,
    tick_ms => ?DEFAULT_TICK_MS,
    lerp_period => ?DEFAULT_LERP_MS
}).

%%=======================================================================
%% Types
%%=======================================================================
-type server_name() :: gen_server:server_name().
-type server_ref() :: gen_server:server_ref().
-type start_opt() :: gen_server:start_opt().
-type start_ret() ::
    {ok, pid()}
    | ignore
    | {error, term()}.
-type start_mon_ret() ::
    {ok, {pid(), reference()}}
    | ignore
    | {error, term()}.
-type state() :: #state{}.
-type from() :: gen_server:from().
-type zone_msg() :: {atom(), map()}.
-type session_pid() :: pid().
-type ow_zone_call_resp() ::
    {noreply, state()}
    | {reply, zone_msg(), state()}
    | {broadcast, zone_msg(), state()}
    | {{send, [session_pid()]}, zone_msg(), state()}.
-type ow_zone_cast_resp() ::
    {noreply, state()}
    | {broadcast, zone_msg(), state()}
    | {{send, [session_pid()]}, zone_msg(), state()}.
-type zone_data() ::
    #{
        clients => [session_pid()],
        frame => non_neg_integer(),
        tick_ms => pos_integer(),
        lerp_period => pos_integer()
    }.

%%=======================================================================
%% ow_zone callbacks
%%=======================================================================
-callback init(Args) -> Result when
    Args :: term(),
    Result ::
        {ok, InitialData}
        | {ok, InitialData, ConfigMap}
        | ignore
        | {stop, Reason},
    ConfigMap :: zone_data(),
    InitialData :: term(),
    Reason :: term().

-callback handle_join(Msg, From, ZoneData, State) -> Result when
    From :: session_pid(),
    Msg :: term(),
    ZoneData :: zone_data(),
    State :: term(),
    Result :: ow_zone_call_resp().

-callback handle_part(Msg, From, ZoneData, State) -> Result when
    From :: session_pid(),
    Msg :: term(),
    ZoneData :: zone_data(),
    State :: term(),
    Result :: ow_zone_call_resp().

-callback handle_reconnect(From, ZoneData, State) -> Result when
    From :: session_pid(),
    ZoneData :: zone_data(),
    State :: term(),
    Result :: ow_zone_call_resp().

-callback handle_disconnect(From, ZoneData, State) -> Result when
    From :: session_pid(),
    ZoneData :: zone_data(),
    State :: term(),
    Result :: ow_zone_cast_resp().

-callback handle_tick(ZoneData, State) -> Result when
    ZoneData :: zone_data(),
    State :: term(),
    Result :: ow_zone_cast_resp().

-callback handle_info(Msg, State) -> Result when
    Msg :: term(),
    State :: term(),
    Result :: ow_zone_cast_resp().

-optional_callbacks([
    handle_join/4,
    handle_part/4,
    handle_disconnect/3,
    handle_reconnect/3,
    handle_info/2
]).
-hank([{unused_callbacks, [all]}]).

%%=======================================================================
%% gen_server API functions
%%=======================================================================

-spec start(Module, Args, Opts) -> Result when
    Module :: module(),
    Args :: term(),
    Opts :: [start_opt()],
    Result :: start_ret().
start(Module, Args, Opts) ->
    gen_server:start(?MODULE, {Module, Args}, Opts).

-spec start(ServerName, Module, Args, Opts) -> Result when
    ServerName :: server_name(),
    Module :: module(),
    Args :: term(),
    Opts :: [start_opt()],
    Result :: start_ret().
start(ServerName, Module, Args, Opts) ->
    Resp = gen_server:start(ServerName, ?MODULE, {Module, Args}, Opts),
    logger:notice("Zone server started: ~p", [Resp]),
    Resp.

-spec start_link(Module, Args, Opts) -> Result when
    Module :: module(),
    Args :: term(),
    Opts :: [start_opt()],
    Result :: start_ret().
start_link(Module, Args, Opts) ->
    gen_server:start_link(?MODULE, {Module, Args}, Opts).

-spec start_link(ServerName, Module, Args, Opts) -> Result when
    ServerName :: server_name(),
    Module :: module(),
    Args :: term(),
    Opts :: [start_opt()],
    Result :: start_ret().
start_link(ServerName, Module, Args, Opts) ->
    Resp = gen_server:start_link(ServerName, ?MODULE, {Module, Args}, Opts),
    logger:notice("Zone server started: ~p", [Resp]),
    Resp.

-spec start_monitor(Module, Args, Opts) -> Result when
    Module :: module(),
    Args :: term(),
    Opts :: [start_opt()],
    Result :: start_mon_ret().
start_monitor(Module, Args, Opts) ->
    gen_server:start_monitor(?MODULE, {Module, Args}, Opts).

-spec start_monitor(ServerName, Module, Args, Opts) -> Result when
    ServerName :: server_name(),
    Module :: module(),
    Args :: term(),
    Opts :: [start_opt()],
    Result :: start_mon_ret().
start_monitor(ServerName, Module, Args, Opts) ->
    gen_server:start_monitor(ServerName, ?MODULE, {Module, Args}, Opts).

-spec call(ServerRef, Message) -> Reply when
    ServerRef :: server_ref(),
    Message :: term(),
    Reply :: term().
call(ServerRef, Msg) ->
    gen_server:call(ServerRef, Msg).

-spec call(ServerRef, Message, Timeout) -> Reply when
    ServerRef :: server_ref(),
    Message :: term(),
    Timeout :: timeout(),
    Reply :: term().
call(ServerRef, Msg, Timeout) ->
    gen_server:call(ServerRef, Msg, Timeout).

-spec cast(ServerRef, Message) -> ok when
    ServerRef :: server_ref(),
    Message :: term().
cast(ServerRef, Msg) ->
    gen_server:cast(ServerRef, Msg).

-spec reply(From, Message) -> ok when
    From :: from(),
    Message :: term().
reply(From, Reply) ->
    gen_server:reply(From, Reply).

-spec stop(ServerRef) -> ok when
    ServerRef :: server_ref().
stop(ServerRef) ->
    gen_server:stop(ServerRef).

-spec stop(ServerRef, Reason, Timeout) -> ok when
    ServerRef :: server_ref(),
    Reason :: term(),
    Timeout :: timeout().
stop(ServerRef, Reason, Timeout) ->
    gen_server:stop(ServerRef, Reason, Timeout).

%%=======================================================================
%% Public API for ow_zone
%%=======================================================================

-spec join(server_ref(), term(), session_pid()) -> ok.
join(ServerRef, Msg, SessionPID) ->
    gen_server:call(ServerRef, ?TAG_I({join, Msg, SessionPID})).

-spec part(server_ref(), term(), session_pid()) -> ok.
part(ServerRef, Msg, SessionPID) ->
    gen_server:call(ServerRef, ?TAG_I({part, Msg, SessionPID})).

-spec disconnect(server_ref(), session_pid()) -> ok.
disconnect(ServerRef, SessionPID) ->
    gen_server:cast(ServerRef, ?TAG_I({disconnect, SessionPID})).

-spec reconnect(server_ref(), session_pid()) -> ok.
reconnect(ServerRef, SessionPID) ->
    gen_server:call(ServerRef, ?TAG_I({reconnect, SessionPID})).

-spec rpc(server_ref(), atom(), term(), session_pid()) -> ok.
rpc(ServerRef, Type, Msg, SessionPID) ->
    gen_server:call(ServerRef, ?TAG_I({Type, Msg, SessionPID})).

-spec broadcast(server_ref(), term()) -> ok.
broadcast(ServerRef, Msg) ->
    gen_server:cast(ServerRef, ?TAG_I({broadcast, Msg})).

-spec send(server_ref(), [session_pid()], term()) -> ok.
send(ServerRef, SessionPIDs, Msg) ->
    gen_server:cast(ServerRef, ?TAG_I({send, SessionPIDs, Msg})).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% gen_server callback functions for internal state
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Initialize the internal state of the zone, with timer
init({CbMod, CbArgs}) ->
    init(CbMod, CbMod:init(CbArgs)).
init(CbMod, {ok, CbData}) ->
    init(CbMod, {ok, CbData, #{}});
init(CbMod, {ok, CbData, ZoneData}) ->
    Config = maps:merge(?INITIAL_ZONE_DATA, ZoneData),
    St0 = initialize_state(CbMod, CbData, Config),
    {ok, St0};
init(_CbMod, ignore) ->
    ignore;
init(_CbMod, Stop) ->
    Stop.

%%%%%%%%%%%%%%%%%%%%%%%%%%
% 1. Add the player to the player list (THIS IMPLIES VALIDATION IS DONE ELSEWHERE)
% 2. Check if callback is exported
% 3. Run the callback
% 4. Check the output which will be of form {noreply, CbData1} OR {replytype(), msg(), CbData1}
%    4a. If there is no reply, we're done
%    4b. If there is a reply, we need to handle it

handle_call(?TAG_I({join, Msg, Who}), _From, St0) ->
    #state{
        cb_mod = CbMod,
        cb_data = CbData0,
        zone_data = ZD
    } = St0,
    % Update the sessions ZonePid
    ZonePid = self(),
    % Could crash with noproc if the player has timed out meanwhile
    {ok, ZonePid} = ow_session:zone(ZonePid, Who),
    Callback =
        case erlang:function_exported(CbMod, disconnect, 1) of
            true ->
                {CbMod, disconnect, [Who]};
            false ->
                {CbMod, part, [#{}, Who]}
        end,
    {ok, Callback} = ow_session:disconnect_callback(Callback, Who),
    % Add the session PID to the clients list
    #{clients := Clients} = ZD,
    ZD1 = ZD#{clients := [Who | Clients]},
    St1 = St0#state{zone_data = ZD1},
    % Run the callback handler, if exported
    maybe
        true ?= erlang:function_exported(CbMod, handle_join, 4),
        {ReplyType, ReplyMsg, CbData1} ?= CbMod:handle_join(Msg, Who, ZD, CbData0),
        CallReply = handle_notify(ReplyType, ReplyMsg, St1),
        {reply, CallReply, St1#state{cb_data = CbData1}}
    else
        false ->
            % Handler not exported, noop.
            {reply, ok, St1};
        {noreply, CbData2} ->
            % State internal update, but no reply
            {reply, ok, St1#state{cb_data = CbData2}}
        % TODO: Placing this here in case it is needed later
        %{error, CbData2} ->
        %    % Server rejects this client join for whatever reason.
        %    % Roll back state update, and then update CbData
        %    {reply, ok, St0#state{cb_data = CbData2}}
    end;
handle_call(?TAG_I({part, Msg, Who}), _From, St0) ->
    #state{
        cb_mod = CbMod,
        cb_data = CbData0,
        zone_data = ZD
    } = St0,
    % Remove the client from the clients list
    #{clients := Clients} = ZD,
    ZD1 = ZD#{clients := lists:delete(Who, Clients)},
    St1 = St0#state{zone_data = ZD1},
    % Remove the zone from the session
    {ok, _} = ow_session:zone(undefined, Who),
    % Run the callback handler, if exported
    maybe
        true ?= is_client(Who, St0),
        true ?= erlang:function_exported(CbMod, handle_part, 4),
        {ReplyType, ReplyMsg, CbData1} ?= CbMod:handle_part(Msg, Who, ZD, CbData0),
        CallMsg = handle_notify(ReplyType, ReplyMsg, St1),
        {reply, CallMsg, St1#state{cb_data = CbData1}}
    else
        false ->
            % No update
            {reply, ok, St1};
        {noreply, CbData2} ->
            % State internal update, but no reply
            {reply, ok, St1#state{cb_data = CbData2}}
    end;
handle_call(?TAG_I({reconnect, Who}), _From, St0) ->
    #state{
        cb_mod = CbMod,
        cb_data = CbData0,
        zone_data = ZD
    } = St0,
    CbMod = St0#state.cb_mod,
    CbData0 = St0#state.cb_data,
    % Run the callback handler, if exported
    maybe
        true ?= is_client(Who, St0),
        true ?= erlang:function_exported(CbMod, handle_reconnect, 3),
        {ReplyType, ReplyMsg, CbData1} ?= CbMod:handle_reconnect(Who, ZD, CbData0),
        CallReply = handle_notify(ReplyType, ReplyMsg, St0),
        {reply, CallReply, St0#state{cb_data = CbData1}}
    else
        false ->
            % Handler not exported, noop.
            {reply, ok, St0};
        {noreply, CbData2} ->
            % State internal update, but no reply
            {reply, ok, St0#state{cb_data = CbData2}}
    end;
handle_call(?TAG_I({Type, Msg, Who}), _From, St0) ->
    #state{
        cb_mod = CbMod,
        cb_data = CbData0,
        zone_data = ZD
    } = St0,
    Handler = list_to_existing_atom("handle_" ++ atom_to_list(Type)),
    maybe
        true ?= is_client(Who, St0),
        true ?= erlang:function_exported(CbMod, Handler, 4),
        {ReplyType, ReplyMsg, CbData1} ?= CbMod:Handler(Msg, Who, ZD, CbData0),
        CallMsg = handle_notify(ReplyType, ReplyMsg, St0),
        % Replies other than noreply will probably not be sent anywhere useful
        {reply, CallMsg, St0#state{cb_data = CbData1}}
    else
        false ->
            % Handler not exported, noop.
            {reply, ok, St0};
        {noreply, CbData2} ->
            % State internal update, but no reply
            {reply, ok, St0#state{cb_data = CbData2}}
    end;
handle_call(Call, _From, St0) ->
    %TODO : Allow fall-through ?
    logger:debug(
        "Zone was called with a message it does not understand: ~p", [Call]
    ),
    {reply, ok, St0}.

handle_cast(?TAG_I({disconnect, Who}), St0) ->
    logger:notice("Received disconnect from session ~p. My state: ~p", [Who, St0]),
    #state{
        cb_mod = CbMod,
        cb_data = CbData0,
        zone_data = ZD
    } = St0,
    maybe
        true ?= erlang:function_exported(CbMod, handle_disconnect, 3),
        {ReplyType, ReplyMsg, CbData1} ?= CbMod:handle_disconnect(Who, ZD, CbData0),
        ok = handle_notify(ReplyType, ReplyMsg, St0),
        {noreply, St0#state{cb_data = CbData1}}
    else
        false ->
            {noreply, St0};
        {noreply, CbData2} ->
            {noreply, St0#state{cb_data = CbData2}}
    end;
handle_cast(?TAG_I({broadcast, Msg}), St0) ->
    ok = handle_notify(broadcast, Msg, St0),
    {noreply, St0};
handle_cast(?TAG_I({send, IDs, Msg}), St0) ->
    ok = handle_notify({send, IDs}, Msg, St0),
    {noreply, St0};
handle_cast(Cast, St0) ->
    %TODO : Allow fall-through ?
    logger:debug(
        "Zone was casted a message it does not understand: ~p", [Cast]
    ),
    {noreply, St0}.

handle_info(?TAG_I(tick), St0) ->
    #state{
        cb_mod = CbMod,
        cb_data = CbData0,
        zone_data = ZoneData
    } = St0,
    #{frame := Frame} = ZoneData,
    % Increment the frame counter
    ZoneData1 = ZoneData#{frame := Frame + 1},
    St1 = St0#state{zone_data = ZoneData1},
    % Run the callback handler
    Result = CbMod:handle_tick(ZoneData1, CbData0),
    case Result of
        {noreply, CbData1} ->
            {noreply, St1#state{cb_data = CbData1}};
        {ReplyType, Msg, CbData1} ->
            ok = handle_notify(ReplyType, Msg, St1),
            {noreply, St1#state{cb_data = CbData1}}
    end;
handle_info(Msg, #state{cb_mod = CbMod} = St0) ->
    #state{cb_mod = CbMod, cb_data = CbData0} = St0,
    maybe
        true ?= erlang:function_exported(CbMod, handle_info, 2),
        {noreply, CbData1} ?= CbMod:handle_info(Msg, CbData0),
        {noreply, St0#state{cb_data = CbData1}}
    else
        false ->
            {noreply, St0};
        {reply, _Msg, CbDataN} ->
            logger:warning("Dropping 'reply' message sent to info handler in zone ~p", [self()]),
            {noreply, St0#state{cb_data = CbDataN}};
        {MsgType, Msg1, CbDataN} ->
            St1 = St0#state{cb_data = CbDataN},
            handle_notify(MsgType, Msg1, St1),
            {noreply, St1}
    end.

terminate(_Reason, _St0) -> ok.
code_change(_OldVsn, St0, _Extra) -> {ok, St0}.

%%=======================================================================
%% Internal functions
%%=======================================================================

handle_notify({send, IDs}, Msg, _St0) ->
    % Filter down to only the clients specified
    ok = ow_session_util:notify_clients(Msg, IDs),
    ok;
handle_notify(broadcast, Msg, St0) ->
    #{clients := Clients} = St0#state.zone_data,
    ok = ow_session_util:notify_clients(Msg, Clients),
    ok;
handle_notify(reply, {MsgType, Msg}, _St0) ->
    {MsgType, Msg}.

initialize_state(CbMod, CbData, ZoneData) ->
    #{tick_ms := TickMs} = ZoneData,
    % setup the timer
    timer:send_interval(TickMs, self(), ?TAG_I(tick)),
    #state{
        cb_mod = CbMod,
        cb_data = CbData,
        zone_data = ZoneData
    }.

is_client(Who, St0) ->
    #{clients := Clients} = St0#state.zone_data,
    lists:member(Who, Clients).
