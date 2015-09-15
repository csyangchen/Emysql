%% @private
-module(emysql_pool_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).
-export([start_child/1]).
-include("emysql.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Pool) ->
    supervisor:start_child(?MODULE, [Pool]).

init([]) ->
    {ok, {{simple_one_for_one, 10, 10}, [
        {emysql_pool, {emysql_pool, start_link, []}, permanent, 5000, worker, [emysql_pool]}
    ]}}.