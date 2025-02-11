%%=========================================================================
%% Overworld Session Utilities
%%
%% This module holds various RPCs for sessions and utilty functions for
%% handling sessions and session-related messages
%%
%%=========================================================================

-module(ow_session_util).
% RPC functions
-export([session_ping/2, session_request/2]).
% Utility functions
-export([disconnect/1, notify_clients/2]).

%%===========================================================================
%% RPC API
%%===========================================================================

-rpc_encoder(#{app => overworld, lib => overworld_pb, interface => ow_msg}).
-rpc_client([session_beacon, session_new, session_pong]).
-rpc_server([session_request, session_ping]).

%%----------------------------------------------------------------------------
%% @doc Calculate the latency based on the RTT to the client
%% @end
%%----------------------------------------------------------------------------
-spec session_ping(map(), pid()) -> {atom(), map()}.
session_ping(Msg, SessionPID) ->
    BeaconID = maps:get(id, Msg),
    Last = ow_beacon:get_by_id(BeaconID),
    Now = erlang:monotonic_time(),
    % NOTE: This change to RTT compared to ow_session (v1)
    Latency = erlang:convert_time_unit(
        round(Now - Last), native, millisecond
    ),
    {ok, Latency} = ow_session:latency(Latency, SessionPID),
    {session_pong, #{latency => Latency}}.

%%----------------------------------------------------------------------------
%% @doc Request a new session, or rejoin an existing one
%% @end
%%----------------------------------------------------------------------------
-spec session_request(map(), pid()) -> ok.
session_request(Msg, SessionPID) ->
    logger:debug("Got session request: ~p", [Msg]),
    Token = maps:get(token, Msg, undefined),
    case Token of
        undefined ->
            logger:debug("No token defined, starting a new session"),
            % No session existing, start a new one
            NewToken = ow_token_serv:new(SessionPID),
            ID = ow_session:id(SessionPID),
            Reply = #{id => ID, reconnect_token => NewToken},
            % Send the reply back through the proxy
            notify_clients({session_new, Reply}, [SessionPID]);
        _ ->
            {PrevSessionPID, NewToken} = ow_token_serv:exchange(Token),
            % Inform the proxy process to update its session ID to refer to the
            % previous, existing one. Internal clients will probably(?) never
            % need to do this
            ProxyPID = ow_session:proxy(SessionPID),
            ProxyPID ! {reconnect_session, PrevSessionPID},
            % Inform the zone that the client has reconnected
            ZonePid = ow_session:zone(PrevSessionPID),
            ow_zone:reconnect(ZonePid, PrevSessionPID),
            % Update the session server with the new token
            {ok, NewToken} = ow_session:token(NewToken, PrevSessionPID),
            % Stop the temporary session
            ok = ow_session_sup:delete(SessionPID)
    end,
    % Register this session in the client list
    ok = pg:join(overworld, clients, SessionPID).

%%===========================================================================
%% Utility API
%%===========================================================================

%%----------------------------------------------------------------------------
%% @doc Set the session to disconnected state and run the appropriate callback
%%      handler
%% @end
%%----------------------------------------------------------------------------
-spec disconnect(pid()) -> ok.
disconnect(SessionPID) ->
    % We've caught an error or otherwise asked to stop, clean up the session
    case ow_session:disconnect_callback(SessionPID) of
        {Module, Fun, Args} ->
            logger:notice("Calling: ~p:~p(~p)", [Module, Fun, Args]),
            erlang:apply(Module, Fun, Args);
        undefined ->
            ok
    end,
    % Reset the session proxy
    {ok, _} = ow_session:disconnect(SessionPID),
    ok.

%%----------------------------------------------------------------------------
%% @doc Send a message to a list of clients
%% @end
%%----------------------------------------------------------------------------
-spec notify_clients({atom(), map()}, [pid()]) -> ok.
notify_clients({_MsgType, _Msg}, []) ->
    ok;
notify_clients({MsgType, Msg}, [SessionPID | Rest]) ->
    logger:debug("Notifying client ~p: ~p", [SessionPID, {MsgType, Msg}]),
    try
        ProxyPID = ow_session:proxy(SessionPID),
        case ProxyPID of
            undefined ->
                logger:debug("no proxy defined for session ~p (noop)", [SessionPID]),
                ok;
            _ ->
                % Send a message to the client, let the connection handler figure
                % out how to serialize it further
                logger:debug("Sending client message to ~p", [ProxyPID]),
                ProxyPID ! {self(), ow_msg, {MsgType, Msg}}
        end
    catch
        exit:{noproc, _} ->
            logger:debug("Couldn't communicate with session ~p: noproc", [SessionPID]),
            ok
    end,
    notify_clients({MsgType, Msg}, Rest).
