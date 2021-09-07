-module(goblet_script_srv).
-behaviour(gen_server).

-include("api/goblet_api_funs.hrl").

-define(SERVER(Name), {via, gproc, {n, l, {?MODULE, Name}}}).

% public interface
-export([start/1, stop/1, do/2]).

% genserver callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

% API
start(ID) ->
    gen_server:start_link(?SERVER(ID), ?MODULE, [], []).
stop(ID) ->
    gen_server:stop(?SERVER(ID)),
    ok.

do(What, ID) ->
    gen_server:call(?SERVER(ID), {do, What}). 

% Callbacks

init([]) ->
    % gdminus uses the process dictionary to store state !
    gdminus_int:init(),
    setup_api(),
    {ok, []}.

handle_call({do, What}, _From, State) -> 
    Reply = gdminus_int:do(What),
    {reply, Reply, State};
handle_call(stop, _From, State) ->
    {stop, normal, stopped, State}.

handle_cast(_Request, State) ->
    {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%---------------------------------------------------------------------------
%% Internal functions
%%---------------------------------------------------------------------------

setup_api() ->
    Funs = ?API_FUNS,
    setup_api(Funs).
setup_api([]) ->
    ok;
setup_api([{Name, Fun}|T]) ->
    gdminus_int:insert_function(Name, Fun),
    setup_api(T).
