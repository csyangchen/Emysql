%% RESPONSE
-define(RESP_OK, 0).
-define(RESP_EOF, 254).
-define(RESP_ERROR, 255).

%% AUTH PLUGIN
-define(MYSQL_OLD_PASSWORD, <<"mysql_old_password">>).

-define(is_timeout(Timeout), (is_integer(Timeout) andalso Timeout > 0 orelse Timeout =:= infinity)).
-define(is_query(Query), (is_list(Query) orelse is_binary(Query))).

-ifdef(DEBUG).
-define(DEBUG(Str, Args), io:format(Str, Args)).
-else.
-define(DEBUG(Str, Args), ok).
-endif.