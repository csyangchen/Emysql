%% -*- mode: erlang;erlang-indent-level: 8;indent-tabs-mode:t -*-
%% Copyright (c) 2009-2012
%% Bill Warnecke <bill@rupture.com>
%% Jacob Vorreuter <jacob.vorreuter@gmail.com>
%% Henning Diedrich <hd2010@eonblast.com>
%% Eonblast Corporation <http://www.eonblast.com>
%%
%% Permission is  hereby  granted,  free of charge,  to any person
%% obtaining  a copy of this software and associated documentation
%% files (the "Software"),to deal in the Software without restric-
%% tion,  including  without  limitation the rights to use,  copy,
%% modify, merge,  publish,  distribute,  sublicense,  and/or sell
%% copies  of the  Software,  and to  permit  persons to  whom the
%% Software  is  furnished  to do  so,  subject  to the  following
%% conditions:
%%
%% The above  copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF  MERCHANTABILITY,  FITNESS  FOR  A  PARTICULAR  PURPOSE  AND
%% NONINFRINGEMENT. IN  NO  EVENT  SHALL  THE AUTHORS OR COPYRIGHT
%% HOLDERS  BE  LIABLE FOR  ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT,  TORT  OR OTHERWISE,  ARISING
%% FROM,  OUT OF OR IN CONNECTION WITH THE SOFTWARE  OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
%% @private

-module(emysql_pool).
-behaviour(gen_server).

-export([start_link/1, init/1, handle_call/3, handle_cast/2, handle_info/2]).
-export([terminate/2, code_change/3]).

-export([
    increment_pool_size/2, decrement_pool_size/2,
    lock_connection/1, wait_for_connection/1, wait_for_connection/2,
    pass_connection/1, replace_connection_as_locked/2, replace_connection_as_available/2,
    give_manager_control/2]).

-include("emysql.hrl").
-include("emysql_internal.hrl").

-record(state, {
    pool :: #pool{},
    available = queue:new() :: queue:queue(#emysql_connection{}),
    waiting = queue:new() :: queue:queue(pid()),
    locked = gb_trees:empty() :: gb_trees:tree(),
    lockers = dict:new()
}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(#pool{pool_id = PoolId} = Pool) ->
    gen_server:start_link({local, PoolId}, ?MODULE, Pool, []).

increment_pool_size(PoolId, Num) ->
    do_gen_call(PoolId, {increment_pool_size, Num}).

decrement_pool_size(PoolId, Num) ->
    do_gen_call(PoolId, {decrement_pool_size, Num}).

lock_connection(PoolId) ->
    do_gen_call(PoolId, {lock_connection, false, self()}).

wait_for_connection(PoolId) ->
    wait_for_connection(PoolId, lock_timeout()).

wait_for_connection(PoolId, Timeout) ->
    %% try to lock a connection. if no connections are available then
    %% wait to be notified of the next available connection
    ?DEBUG("~p waits for connection to pool ~p~n", [self(), PoolId]),
    case do_gen_call(PoolId, {lock_connection, true, self()}) of
        unavailable ->
            ?DEBUG("~p is queued~n", [self()]),
            receive
                {connection, Connection} -> Connection
            after Timeout ->
                do_gen_call(PoolId, abort_wait),
                receive
                    {connection, Connection} -> Connection
                after
                    0 -> exit(connection_lock_timeout)
                end
            end;
        Connection ->
            ?DEBUG("~p gets connection~n", [self()]),
            Connection
    end.

pass_connection(#emysql_connection{pool = #pool{pool_id = PoolId}} = Connection) ->
    do_gen_call(PoolId, {{replace_connection, available}, Connection, Connection}).

replace_connection_as_available(#emysql_connection{pool = #pool{pool_id = PoolId}} = OldConn, NewConn) ->
    do_gen_call(PoolId, {{replace_connection, available}, OldConn, NewConn}).

replace_connection_as_locked(#emysql_connection{pool = #pool{pool_id = PoolId}} = OldConn, NewConn) ->
    do_gen_call(PoolId, {{replace_connection, locked}, OldConn, NewConn}).

give_manager_control(PoolId, Socket) ->
    case whereis(PoolId) of
        undefined -> {error, failed_to_find_conn_mgr};
        MgrPid -> gen_tcp:controlling_process(Socket, MgrPid)
    end.

%% the stateful loop functions of the gen_server never
%% want to call exit/1 because it would crash the gen_server.
%% instead we want to return error tuples and then throw
%% the error once outside of the gen_server process
do_gen_call(PoolId, Msg) ->
    case gen_server:call(PoolId, Msg, infinity) of
        {error, Reason} ->
            exit(Reason);
        Result ->
            Result
    end.

init(Pool) ->
    case open_connections(#state{pool = Pool}) of
        {ok, State} ->
            {ok, State};
        {error, Reason} ->
            {stop, Reason}
    end.

open_connections(#state{pool = Pool, available = Available} = State) ->
    ?DEBUG("open connections loop: .. ", []),
    case queue:len(Available) < Pool#pool.size of
        true ->
            case catch emysql_conn:open_connection(Pool) of
                #emysql_connection{} = Conn ->
                    open_connections(State#state{available = queue:in(Conn, Available)});
                {'EXIT', Reason} ->
                    lists:foreach(fun emysql_conn:close_connection/1, queue:to_list(Available)),
                    {error, Reason}
            end;
        false ->
            {ok, State}
    end.

handle_call({increment_pool_size, PoolId, Num}, _From, State) ->
    %% TODO
    {reply, ok, State};

handle_call({decrement_pool_size, PoolId, Num}, _From, State) ->
    %% TODO
    {reply, ok, State};

handle_call({lock_connection, Wait, Who}, {From, _Mref}, State) ->
    case lock_next_connection(State#state.available, State#state.locked, Who) of
        {ok, Connection, OtherAvailable, NewLocked, {MonitorRef, Data}} ->
            Lockers = State#state.lockers,
            {reply, Connection, State#state{available = OtherAvailable, locked = NewLocked, lockers = dict:store(MonitorRef, Data, Lockers)}};
        unavailable when Wait =:= true ->
            {reply, unavailable, State#state{waiting = queue:in(From, State#state.waiting)}};
        unavailable when Wait =:= false ->
            {reply, unavailable, State}
    end;

handle_call(abort_wait, {From, _Mref}, State) ->
    QueueNow = queue:filter(
        fun(Pid) ->
            Pid =/= From end,
        State#state.waiting),
    OldLen = queue:len(State#state.waiting),
    NewLen = queue:len(QueueNow),
    if
        OldLen =:= NewLen ->
            Reply = not_waiting;
        true ->
            Reply = ok
    end,
    {reply, Reply, State#state{waiting = QueueNow}};

handle_call({{replace_connection, Kind}, OldConn, NewConn}, _From, State) ->
    %% if an error occurs while doing work over a connection then
    %% the connection must be closed and a new one created in its
    %% place. The calling process is responsible for creating the
    %% new connection, closing the old one and replacing it in state.
    %% This function expects a new, available connection to be
    %% passed in to serve as the replacement for the old one.
    %% But i.e. if the sql server is down, it can be fed a dead
    %% old connection as new connection, to preserve the pool size.
    OldRef = OldConn#emysql_connection.monitor_ref,
    Stripped = gb_trees:delete_any(OldConn#emysql_connection.id, State#state.locked),
    {NewState, NewMonitors} =
        case Kind of
            available ->
                State2 = State#state{
                    locked = Stripped,
                    available = queue:in(NewConn#emysql_connection{locked_at = undefined, monitor_ref = undefined}, State#state.available)
                },
                serve_waiting_pids(State2);
            locked ->
                State2 = State#state{
                    locked = gb_trees:enter(NewConn#emysql_connection.id, NewConn#emysql_connection{monitor_ref = OldRef}, Stripped)
                },
                {State2, []} % There are no new monitors set up
        end,
    Lockers = State#state.lockers,
    NewLockers = case Kind of
        available ->
            %% Remove the current monitor as the connection has been freed
            %% Add the monitors for pids that has been take from the waiting queue
            erlang:demonitor(OldRef, [flush]),
            lists:foldl(fun({MonitorRef, ConnInfo}, Dict) ->
                dict:store(MonitorRef, ConnInfo, Dict)
            end, dict:erase(OldRef, Lockers), NewMonitors);
        locked ->
            %% We need to keep the monitor here
            dict:store(OldRef, NewConn#emysql_connection.id, Lockers)
    end,
    {reply, ok, NewState#state{lockers = NewLockers}};

handle_call(_, _From, State) -> {reply, {error, invalid_call}, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({'DOWN', MonitorRef, _, _, _}, State) ->
    case dict:find(MonitorRef, State#state.lockers) of
        {ok, ConnId} ->
            case gb_trees:lookup(ConnId, State#state.locked) of
                {value, Conn} ->
                    async_reset_conn(Conn);
                _ ->
                    ok
            end;
        _ ->
            ok
    end,
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

async_reset_conn(Conn) ->
    spawn(fun() ->
        %% This interacts with the conn mgr so needs to be spawned
        %% TODO: refactor
        emysql_conn:reset_connection(Conn, pass)
    end).
%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
initialize_pools() ->
    %% TODO
    %% if the emysql application values are not present in the config
    %% file we will initialize and empty set of pools. Otherwise, the
    %% values defined in the config are used to initialize the state.
    [
        #pool{
            pool_id = PoolId,
            size = proplists:get_value(size, Props, 1),
            user = proplists:get_value(user, Props),
            password = proplists:get_value(password, Props),
            host = proplists:get_value(host, Props),
            port = proplists:get_value(port, Props),
            database = proplists:get_value(database, Props),
            encoding = proplists:get_value(encoding, Props),
            start_cmds = proplists:get_value(start_cmds, Props, [])
        } || {PoolId, Props} <- emysql_app:pools()
    ].

lock_next_connection(Available, Locked, Who) ->
    case queue:out(Available) of
        {{value, Conn}, OtherAvailable} ->
            MonitorRef = erlang:monitor(process, Who),
            NewConn = connection_locked_at(Conn, MonitorRef),
            NewLocked = gb_trees:enter(NewConn#emysql_connection.id, NewConn, Locked),
            MonitorTuple = {MonitorRef, NewConn#emysql_connection.id},
            {ok, NewConn, OtherAvailable, NewLocked, MonitorTuple};
        {empty, _} ->
            unavailable
    end.

connection_locked_at(Conn, MonitorRef) ->
    Conn#emysql_connection{locked_at = lists:nth(2, tuple_to_list(now())),
        monitor_ref = MonitorRef}.

serve_waiting_pids(State) ->
    {Waiting, Available, Locked, NewRefs} = serve_waiting_pids(State#state.waiting, State#state.available, State#state.locked, []),
    {State#state{waiting = Waiting, available = Available, locked = Locked}, NewRefs}.

serve_waiting_pids(Waiting, Available, Locked, MonitorRefs) ->
    case queue:is_empty(Waiting) of
        false ->
            Who = queue:get(Waiting),
            case lock_next_connection(Available, Locked, Who) of
                {ok, Connection, OtherAvailable, NewLocked, NewRef} ->
                    {{value, Pid}, OtherWaiting} = queue:out(Waiting),
                    case erlang:is_process_alive(Pid) of
                        true ->
                            erlang:send(Pid, {connection, Connection}),
                            serve_waiting_pids(OtherWaiting, OtherAvailable, NewLocked, [NewRef | MonitorRefs]);
                        _ ->
                            serve_waiting_pids(OtherWaiting, Available, Locked, MonitorRefs)
                    end;
                unavailable ->
                    {Waiting, Available, Locked, MonitorRefs}
            end;
        true ->
            {Waiting, Available, Locked, MonitorRefs}
    end.

lock_timeout() ->
    emysql_app:lock_timeout().
