%% Copyright (c) 2009-2012
%% Bill Warnecke <bill@rupture.com>,
%% Jacob Vorreuter <jacob.vorreuter@gmail.com>,
%% Henning Diedrich <hd2010@eonblast.com>,
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
-module(emysql_conn).
-export([set_database/2, set_encoding/2,
    execute/3, prepare/3, unprepare/2,
    open_connection/1,
    reset_connection/2, close_connection/1,
    hstate/1,
    test_connection/2, need_test_connection/1
]).

-include("emysql.hrl").
-include("emysql_internal.hrl").

%% MYSQL COMMANDS
-define(COM_SLEEP, 16#00).
-define(COM_QUIT, 16#01).
-define(COM_INIT_DB, 16#02).
-define(COM_QUERY, 16#03).
-define(COM_FIELD_LIST, 16#04).
-define(COM_CREATE_DB, 16#05).
-define(COM_DROP_DB, 16#06).
-define(COM_REFRESH, 16#07).
-define(COM_SHUTDOWN, 16#08).
-define(COM_STATISTICS, 16#09).
-define(COM_PROCESS_INFO, 16#0a).
-define(COM_CONNECT, 16#0b).
-define(COM_PROCESS_KILL, 16#0c).
-define(COM_DEBUG, 16#0d).
-define(COM_PING, 16#0e).
-define(COM_TIME, 16#0f).
-define(COM_DELAYED_INSERT, 16#10).
-define(COM_CHANGE_USER, 16#11).
-define(COM_BINLOG_DUMP, 16#12).
-define(COM_TABLE_DUMP, 16#13).
-define(COM_CONNECT_OUT, 16#14).
-define(COM_REGISTER_SLAVE, 16#15).
-define(COM_STMT_PREPARE, 16#16).
-define(COM_STMT_EXECUTE, 16#17).
-define(COM_STMT_SEND_LONG_DATA, 16#18).
-define(COM_STMT_CLOSE, 16#19).
-define(COM_STMT_RESET, 16#1a).
-define(COM_SET_OPTION, 16#1b).
-define(COM_STMT_FETCH, 16#1c).

set_database(_, undefined) -> ok;
set_database(_, Empty) when Empty == ""; Empty == <<>> -> ok;
set_database(Connection, Database) ->
    Packet = [?COM_QUERY, "USE `", Database, "`"],  % todo: utf8?
    emysql_tcp:send_and_recv_packet(Connection#emysql_connection.socket, Packet, 0).

set_encoding(_, undefined) -> ok;
set_encoding(Connection, {Encoding, Collation}) ->
    Packet = [?COM_QUERY, "SET NAMES '", erlang:atom_to_binary(Encoding, utf8),
        "' COLLATE '", erlang:atom_to_binary(Collation, utf8), "'"],
    emysql_tcp:send_and_recv_packet(Connection#emysql_connection.socket, Packet, 0);
set_encoding(Connection, Encoding) ->
    Packet = [?COM_QUERY, "SET NAMES '", erlang:atom_to_binary(Encoding, utf8), "'"],
    emysql_tcp:send_and_recv_packet(Connection#emysql_connection.socket, Packet, 0).

execute(Connection, StmtName, []) when is_atom(StmtName) ->
    prepare_statement(Connection, StmtName),
    StmtNameBin = atom_to_binary(StmtName, utf8),
    Packet = [?COM_QUERY, "EXECUTE ", StmtNameBin],
    send_recv(Connection, Packet);
execute(Connection, Fun, Args) when is_function(Fun), is_list(Args) ->
    erlang:apply(Fun, [Connection | Args]);
execute(Connection, Query, []) when ?is_query(Query) ->
    Packet = [?COM_QUERY, Query],
    send_recv(Connection, Packet);
execute(Connection, Query, Args) when ?is_query(Query) andalso is_list(Args) ->
    StmtName = "stmt_" ++ integer_to_list(erlang:phash2(Query)),
    ok = prepare(Connection, StmtName, Query),
    Ret =
        case set_params(Connection, 1, Args, undefined) of
            OK when is_record(OK, ok_packet) ->
                ParamNamesBin = join([[$@ | integer_to_list(I)] || I <- lists:seq(1, length(Args))], ", "),  % todo: utf8?
                Packet = [?COM_QUERY, "EXECUTE ", StmtName, " USING ", ParamNamesBin],  % todo: utf8?
                send_recv(Connection, Packet);
            Error ->
                Error
        end,
    unprepare(Connection, StmtName),
    Ret;

execute(Connection, StmtName, Args) when is_atom(StmtName), is_list(Args) ->
    prepare_statement(Connection, StmtName),
    case set_params(Connection, 1, Args, undefined) of
        OK when is_record(OK, ok_packet) ->
            ParamNamesBin = join([[$@ | integer_to_list(I)] || I <- lists:seq(1, length(Args))], ", "),  % todo: utf8?
            StmtNameBin = atom_to_binary(StmtName, utf8),
            Packet = [?COM_QUERY, "EXECUTE ", StmtNameBin, " USING ", ParamNamesBin],
            send_recv(Connection, Packet);
        Error ->
            Error
    end.

prepare(Connection, Name, Statement) when is_atom(Name) ->
    prepare(Connection, atom_to_list(Name), Statement);
prepare(Connection, Name, Statement) ->
    StatementBin = encode(Statement, binary),
    Packet = [?COM_QUERY, "PREPARE ", Name, " FROM ", StatementBin],  % todo: utf8?
    case send_recv(Connection, Packet) of
        OK when is_record(OK, ok_packet) ->
            ok;
        Err when is_record(Err, error_packet) ->
            exit({failed_to_prepare_statement, Err#error_packet.msg})
    end.

unprepare(Connection, Name) when is_atom(Name) ->
    unprepare(Connection, atom_to_list(Name));
unprepare(Connection, Name) ->
    Packet = [?COM_QUERY, "DEALLOCATE PREPARE ", Name],  % todo: utf8?
    send_recv(Connection, Packet).

open_connection(#pool{pool_id = PoolId, host = Host, port = Port, user = User,
    password = Password, database = Database, encoding = Encoding,
    start_cmds = StartCmds, connect_timeout = ConnectTimeout} = Pool) ->
    ?DEBUG("~p open connection for pool ~p host ~p port ~p user ~p base ~p~n", [self(), PoolId, Host, Port, User, Database]),
    case gen_tcp:connect(Host, Port, [binary, {packet, raw}, {active, false}, {recbuf, ?TCP_RECV_BUFFER}], ConnectTimeout) of
        {ok, Sock} ->
            #greeting{
                server_version = Version,
                thread_id = ThreadId,
                caps = Caps,
                language = Language
            } = handshake(Sock, User, Password),
            Connection = #emysql_connection{
                id = erlang:port_to_list(Sock), %% TODO
                pool = Pool,
                socket = Sock,
                version = Version,
                thread_id = ThreadId,
                caps = Caps,
                language = Language,
                test_period = Pool#pool.conn_test_period,
                last_test_time = now_seconds()
            },
            ?DEBUG("~p open connection: ... set db ...~n", [self()]),
            ok = set_database_or_die(Connection, Database),
            ok = set_encoding_or_die(Connection, Encoding),
            ok = run_startcmds_or_die(Connection, StartCmds),
            ok = give_manager_control(PoolId, Sock),
            Connection;
        {error, Reason} ->
            ?DEBUG("~p open connection: ... ERROR ~p~n", [self(), Reason]),
            exit({failed_to_connect_to_database, Reason})
    end.

handshake(Sock, User, Password) ->
    case emysql_auth:handshake(Sock, User, Password) of
        {ok, #greeting{} = G} -> G;
        {error, Reason} ->
            gen_tcp:close(Sock),
            exit(Reason)
    end.

give_manager_control(PoolId, Socket) ->
    case emysql_pool:give_manager_control(PoolId, Socket) of
        {error, Reason} ->
            gen_tcp:close(Socket),
            exit({Reason,
                "Failed to find conn mgr when opening connection. Make sure crypto is started and emysql.app is in the Erlang path."});
        ok -> ok
    end.

set_database_or_die(#emysql_connection{socket = Socket} = Connection, Database) ->
    case set_database(Connection, Database) of
        ok -> ok;
        OK1 when is_record(OK1, ok_packet) -> ok;
        Err1 when is_record(Err1, error_packet) ->
            gen_tcp:close(Socket),
            exit({failed_to_set_database, Err1#error_packet.msg})
    end.

run_startcmds_or_die(#emysql_connection{socket = Socket}, StartCmds) ->
    lists:foreach(
        fun(Cmd) ->
            Packet = [?COM_QUERY, Cmd],
            case emysql_tcp:send_and_recv_packet(Socket, Packet, 0) of
                OK when OK =:= ok orelse is_record(OK, ok_packet) ->
                    ok;
                #error_packet{msg = Msg} ->
                    gen_tcp:close(Socket),
                    exit({failed_to_run_cmd, Msg})
            end
        end,
        StartCmds
    ).

set_encoding_or_die(#emysql_connection{socket = Socket} = Connection, Encoding) ->
    case set_encoding(Connection, Encoding) of
        ok -> ok;
        OK2 when is_record(OK2, ok_packet) -> ok;
        Err2 when is_record(Err2, error_packet) ->
            gen_tcp:close(Socket),
            exit({failed_to_set_encoding, Err2#error_packet.msg})
    end.

reset_connection(Conn, StayLocked) ->
    %% if a process dies or times out while doing work
    %% the socket must be closed and the connection reset
    %% in the conn_mgr state. Also a new connection needs
    %% to be opened to replace the old one. If that fails,
    %% we queue the old as available for the next try
    %% by the next caller process coming along. So the
    %% pool can't run dry, even though it can freeze.
    ?DEBUG("resetting connection~n", []),
    MonitorRef = Conn#emysql_connection.monitor_ref,
    close_connection(Conn),
    %% OPEN NEW SOCKET
    case catch open_connection(Conn#emysql_connection.pool) of
        #emysql_connection{} = NewConn when StayLocked == pass ->
            NewConn2 = add_monitor_ref(NewConn, MonitorRef),
            ok = emysql_pool:replace_connection_as_available(Conn, NewConn2),
            NewConn2;
        #emysql_connection{} = NewConn when StayLocked == keep ->
            NewConn2 = add_monitor_ref(NewConn, MonitorRef),
            ok = emysql_pool:replace_connection_as_locked(Conn, NewConn2),
            NewConn2;
        {'EXIT', Reason} ->
            DeadConn = Conn#emysql_connection{alive = false, last_test_time = 0},
            emysql_pool:replace_connection_as_available(Conn, DeadConn),
            {error, {cannot_reopen_in_reset, Reason}}
    end.

add_monitor_ref(Conn, MonitorRef) ->
    Conn#emysql_connection{monitor_ref = MonitorRef}.

close_connection(Conn) ->
    %% garbage collect statements
    emysql_statements:remove(Conn#emysql_connection.id),
    ok = gen_tcp:close(Conn#emysql_connection.socket).

test_connection(Conn, StayLocked) ->
    case catch emysql_tcp:send_and_recv_packet(Conn#emysql_connection.socket, <<?COM_PING>>, 0) of
        {'EXIT', _} ->
            case reset_connection(Conn, StayLocked) of
                NewConn when is_record(NewConn, emysql_connection) ->
                    NewConn;
                {error, FailedReset} ->
                    exit({connection_down, {and_conn_reset_failed, FailedReset}})
            end;
        _ ->
            NewConn = Conn#emysql_connection{last_test_time = now_seconds()},
            case StayLocked of
                pass -> emysql_pool:replace_connection_as_available(Conn, NewConn);
                keep -> emysql_pool:replace_connection_as_locked(Conn, NewConn)
            end,
            NewConn
    end.

need_test_connection(Conn) ->
    (Conn#emysql_connection.test_period =:= 0) orelse
        (Conn#emysql_connection.last_test_time =:= 0) orelse
        (Conn#emysql_connection.last_test_time + Conn#emysql_connection.test_period < now_seconds()).

now_seconds() ->
    {M, S, _} = erlang:now(),
    M * 1000000 + S.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

%% @doc A wrapper for emysql_tcp:send_and_recv_packet/3 that may log warnings if any.
send_recv(#emysql_connection{socket = Socket, pool = #pool{warnings = Warnings}}, Packet) ->
    Ret = emysql_tcp:send_and_recv_packet(Socket, Packet, 0),
    Warnings andalso log_warnings(Socket, Ret),
    Ret.

log_warnings(Socket, #ok_packet{warning_count = WarningCount}) when WarningCount > 0 ->
    %% Fetch the warnings and log them in the OTP way.
    #result_packet{rows = WarningRows} =
        emysql_tcp:send_and_recv_packet(Socket, [?COM_QUERY, "SHOW WARNINGS"], 0),
    WarningMessages = [Message || [_Level, _Code, Message] <- WarningRows],
    error_logger:warning_report({emysql_warnings, WarningMessages});
log_warnings(_Sock, _OtherPacket) ->
    ok.

set_params(_, _, [], Result) -> Result;
set_params(Connection, Num, Values, _) ->
    Packet = set_params_packet(Num, Values),
    emysql_tcp:send_and_recv_packet(Connection#emysql_connection.socket, Packet, 0).

set_params_packet(NumStart, Values) ->
    BinValues = [encode(Val, binary) || Val <- Values],
    BinNums = [encode(Num, binary) || Num <- lists:seq(NumStart, NumStart + length(Values) - 1)],
    BinPairs = lists:zip(BinNums, BinValues),
    Parts = [["@", NumBin, "=", ValBin] || {NumBin, ValBin} <- BinPairs],
    Sets = join(Parts, ","),
    [?COM_QUERY, "SET ", Sets].

%% @doc Join elements of list with Sep
%%
%% 1> join([1,2,3], 0).
%% [1,0,2,0,3]

join([], _Sep) -> [];
join(L, Sep) -> join(L, Sep, []).

join([H], _Sep, Acc) -> lists:reverse([H | Acc]);
join([H | T], Sep, Acc) -> join(T, Sep, [Sep, H | Acc]).

prepare_statement(Connection, StmtName) ->
    case emysql_statements:fetch(StmtName) of
        undefined ->
            exit(statement_has_not_been_prepared);
        {Version, Statement} ->
            case emysql_statements:version(Connection#emysql_connection.id, StmtName) of
                Version ->
                    ok;
                _ ->
                    ok = prepare(Connection, StmtName, Statement),
                    emysql_statements:prepare(Connection#emysql_connection.id, StmtName, Version)
            end
    end.

% human readable string rep of the server state flag
%% @private
hstate(State) ->
    case (State band ?SERVER_STATUS_AUTOCOMMIT) of 0 -> ""; _ -> "AUTOCOMMIT " end
        ++ case (State band ?SERVER_MORE_RESULTS_EXIST) of 0 -> ""; _ -> "MORE_RESULTS_EXIST " end
        ++ case (State band ?SERVER_QUERY_NO_INDEX_USED) of 0 -> ""; _ -> "NO_INDEX_USED " end.

%% @doc Encode a value so that it can be included safely in a MySQL query.
%% @spec encode(term(), list | binary) -> string() | binary() | {error, Error}
encode(null, list) ->
    "null";
encode(undefined, list) ->
    "null";
encode(null, binary) ->
    <<"null">>;
encode(undefined, binary) ->
    <<"null">>;
encode(Val, list) when is_binary(Val) ->
    quote(binary_to_list(Val));
encode(Val, binary) when is_atom(Val) ->
    encode(atom_to_list(Val), binary);
encode(Val, binary) when is_list(Val) ->
    list_to_binary(quote(Val));
encode(Val, binary) when is_binary(Val) ->
    list_to_binary(quote(binary_to_list(Val)));
encode(Val, list) when is_list(Val) ->
    quote(Val);
encode(Val, list) when is_integer(Val) ->
    integer_to_list(Val);
encode(Val, binary) when is_integer(Val) ->
    list_to_binary(integer_to_list(Val));
encode(Val, list) when is_float(Val) ->
    [Res] = io_lib:format("~w", [Val]),
    Res;
encode(Val, binary) when is_float(Val) ->
    iolist_to_binary(io_lib:format("~w", [Val]));
encode({datetime, Val}, ReturnType) ->
    encode(Val, ReturnType);
encode({date, Val}, ReturnType) ->
    encode(Val, ReturnType);
encode({time, Val}, ReturnType) ->
    encode(Val, ReturnType);
encode({{Year, Month, Day}, {Hour, Minute, Second}}, list) ->
    Res = io_lib:format("'~4.4.0w-~2.2.0w-~2.2.0w ~2.2.0w:~2.2.0w:~2.2.0w'",
        [Year, Month, Day, Hour, Minute, Second]),
    lists:flatten(Res);
encode({{_Year, _Month, _Day}, {_Hour, _Minute, _Second}} = Val, binary) ->
    list_to_binary(encode(Val, list));
encode({Time1, Time2, Time3}, list) ->
    Res = two_digits([Time1, Time2, Time3]),
    lists:flatten(Res);
encode({_Time1, _Time2, _Time3} = Val, binary) ->
    list_to_binary(encode(Val, list));
encode(Val, _) ->
    {error, {unrecognized_value, Val}}.

%% @private
two_digits(Nums) when is_list(Nums) ->
    [two_digits(Num) || Num <- Nums];
two_digits(Num) ->
    [Str] = io_lib:format("~b", [Num]),
    case length(Str) of
        1 -> [$0 | Str];
        _ -> Str
    end.

%% @doc Quote a string or binary value so that it can be included safely in a
%% MySQL query. For the quoting, a binary is converted to a list and back.
%% For this, it's necessary to know the encoding of the binary.
%% @spec quote(x()) -> x()
%%       x() = list() | binary()
%% @end
%% hd/11,12
quote(String) when is_list(String) ->
    [39 | lists:reverse([39 | quote_loop(String)])]. %% 39 is $'

%% @doc  Make MySQL-safe backslash escapes before 10, 13, \, 26, 34, 39.
%% @spec quote_loop(list()) -> list()
%% @private
%% @end
%% hd/11,12
quote_loop(List) ->
    quote_loop(List, []).

quote_loop([], Acc) ->
    Acc;

quote_loop([0 | Rest], Acc) ->
    quote_loop(Rest, [$0, $\\ | Acc]);

quote_loop([10 | Rest], Acc) ->
    quote_loop(Rest, [$n, $\\ | Acc]);

quote_loop([13 | Rest], Acc) ->
    quote_loop(Rest, [$r, $\\ | Acc]);

quote_loop([$\\ | Rest], Acc) ->
    quote_loop(Rest, [$\\, $\\ | Acc]);

quote_loop([39 | Rest], Acc) -> %% 39 is $'
    quote_loop(Rest, [39, $\\ | Acc]); %% 39 is $'

quote_loop([34 | Rest], Acc) -> %% 34 is $"
    quote_loop(Rest, [34, $\\ | Acc]); %% 34 is $"

quote_loop([26 | Rest], Acc) ->
    quote_loop(Rest, [$Z, $\\ | Acc]);

quote_loop([C | Rest], Acc) ->
    quote_loop(Rest, [C | Acc]).
