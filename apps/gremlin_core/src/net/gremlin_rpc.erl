-module(gremlin_rpc).

% Functions for handling RPCs
-export([
    opcode/1,
    c2s_call/1,
    c2s_handler/1,
    s2c_call/1,
    encoder/1
]).

-type rpc() :: #{
    opcode := 16#0000..16#FFFF,
    c2s_call := atom() | undefined,
    c2s_handler := mfa() | undefined,
    s2c_call := atom() | undefined,
    encoder := atom() | undefined
}.
-export_type([rpc/0]).

-type callbacks() :: [rpc(), ...].

-export_type([callbacks/0]).

-callback rpc_info() ->
    Callbacks :: [rpc(), ...].

-spec opcode(map()) -> 16#0000..16#FFFF.
opcode(Map) ->
    maps:get(opcode, Map, undefined).

-spec c2s_call(map()) -> atom() | undefined.
c2s_call(Map) ->
    maps:get(c2s_call, Map, undefined).

-spec c2s_handler(map()) -> mfa() | undefined.
c2s_handler(Map) ->
    maps:get(c2s_handler, Map, undefined).

-spec s2c_call(map()) -> atom() | undefined.
s2c_call(Map) ->
    maps:get(s2c_call, Map, undefined).

-spec encoder(map()) -> atom() | undefined.
encoder(Map) ->
    maps:get(encoder, Map, undefined).
