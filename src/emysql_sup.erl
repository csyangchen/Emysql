%% @private
-module(emysql_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_all, 10, 10}, [
        {emysql_statements, {emysql_statements, start_link, []}, permanent, 5000, worker, [emysql_statements]},
        {emysql_pool_sup, {emysql_pool_sup, start_link, []}, permanent, 5000, supervisor, [emysql_pool_sup]}
    ]}}.
