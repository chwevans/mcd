%%% vim: set ts=4 sts=4 sw=4 et:
-module(mcd_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    test_local/1,
    test_do/1,
    test_api/1,
    test_common_errors/1
]).

-define(KEY, list_to_binary(?MODULE_STRING ++ "_" ++ "key")).
-define(VALUE, <<"value">>).
-define(TTL, 1).

all() -> [
    test_local,
    test_do,
    test_api,
    test_common_errors
].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(mcd),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(mcd).

% Tests

test_do(_Config) ->
    Pid = get_pid(),
    {ok, [_ | _]} = mcd:do(Pid, version),
    {ok, flushed} = mcd:do(Pid, flush_all),
    {ok, flushed} = mcd:do(Pid, {flush_all, 10}),
    {error, notfound} = mcd:do(Pid, get, ?KEY),
    {error, notfound} = mcd:do(Pid, delete, ?KEY),
    try
        {ok, ?VALUE} = mcd:do(Pid, set, ?KEY, ?VALUE),
        {error, notstored} = mcd:do(Pid, add, ?KEY, ?VALUE),
        {ok, ?VALUE} = mcd:do(Pid, replace, ?KEY, ?VALUE),
        {ok, ?VALUE} = mcd:do(Pid, {set, 0, ?TTL}, ?KEY, ?VALUE),
        {error, notstored} = mcd:do(Pid, {add, 0, ?TTL}, ?KEY, ?VALUE),
        {ok, ?VALUE} = mcd:do(Pid, {replace, 0, ?TTL}, ?KEY, ?VALUE),
        {ok, deleted} = mcd:do(Pid, delete, ?KEY)
    after
        mcd:do(Pid, delete, ?KEY),
        mcd:stop(Pid)
    end.

test_api(_Config) ->
    Pid = get_pid(),
    {ok, [_ | _]} = mcd:version(Pid),

    GetFun = fun() -> mcd:get(Pid, ?KEY) end,
    DeleteFun = fun() -> mcd:delete(Pid, ?KEY) end,

    test_set(GetFun, DeleteFun, fun() -> mcd:set(Pid, ?KEY, ?VALUE) end),
    test_set_expiration(GetFun, DeleteFun, fun() -> mcd:set(Pid, ?KEY, ?VALUE, ?TTL) end),
    test_set_expiration(GetFun, DeleteFun, fun() -> mcd:set(Pid, ?KEY, ?VALUE, ?TTL, 0) end),
    test_set(GetFun, DeleteFun, fun() -> {ok, mcd:async_set(Pid, ?KEY, ?VALUE)} end),
    test_set_expiration(GetFun, DeleteFun, fun() -> {ok, mcd:async_set(Pid, ?KEY, ?VALUE, ?TTL)} end),
    test_set_expiration(GetFun, DeleteFun, fun() -> {ok, mcd:async_set(Pid, ?KEY, ?VALUE, ?TTL, 0)} end),
    mcd:stop(Pid).

test_common_errors(_Config) ->
    Pid = get_pid(),

    {error, timeout} = mcd:version(self()),
    {error, noproc} = mcd:version(undefined),

    {ok, [_ | _]} = mcd:version(Pid),
    {error, not_broken} = mcd:fix_connection(Pid),
    ok = mcd:break_connection(Pid),
    try
        {error, noconn} = mcd:version(Pid)
    after
        ok = mcd:fix_connection(Pid),
        ok = wait_connection(Pid, 5)
    end,

    {ok, [_ | _]} = mcd:version(Pid),
    {error, not_overloaded} = mcd:unload_connection(Pid),
    ok = mcd:overload_connection(Pid),
    try
        {error, overload} = mcd:version(Pid)
    after
        ok = mcd:unload_connection(Pid)
    end,

    {ok, [_ | _]} = mcd:version(Pid),
    mcd:stop(Pid).

test_local(_Config) ->
    Pid = get_pid(),
    GetFun = fun() -> mcd:get(Pid, ?KEY) end,
    DeleteFun = fun() -> mcd:delete(Pid, ?KEY) end,

    test_set(GetFun, DeleteFun, fun() -> mcd:set(Pid, ?KEY, ?VALUE) end),
    test_set_expiration(GetFun, DeleteFun, fun() -> mcd:set(Pid, ?KEY, ?VALUE, ?TTL) end),

    {error, notfound} = mcd:get(Pid, ?KEY),
    try
        {ok, ?VALUE} = mcd:set(Pid, ?KEY, ?VALUE),
        {ok, flushed} = mcd:flush_all(Pid),
        {error, notfound} = mcd:get(Pid, ?KEY)
    after
        mcd:delete(Pid, ?KEY)
    end,
    mcd:stop(Pid).


% private functions

test_set(GetFun, DeleteFun, SetFun) ->
    {error, notfound} = GetFun(),
    try
        {ok, ?VALUE} = SetFun(),
        {ok, ?VALUE} = GetFun(),
        {ok, deleted} = DeleteFun(),
        {error, notfound} = GetFun()
    after
        DeleteFun()
    end.

test_set_expiration(GetFun, DeleteFun, SetFun) ->
    {error, notfound} = GetFun(),
    try
        {ok, ?VALUE} = SetFun(),
        {ok, ?VALUE} = GetFun(),
        timer:sleep((?TTL + 1) * 1000),
        {error, notfound} = GetFun()
    after
        DeleteFun()
    end.

get_pid() ->
	{ok, Pid} = mcd:start_link("127.0.0.1", 11211),
	ok = wait_connection(Pid, 10),
	Pid.

wait_connection(_Pid, 0) ->
	{error, timeout};
wait_connection(Pid, Tries) ->
    case {mcd:version(Pid), Tries} of
        {{ok, [_ | _]}, _} ->
            ok;
        {Error, 0} ->
            lager:error("Unexpected mcd answer: ~p~n", [Error]),
			{error, timeout};
        {_Error, Tries} ->
			timer:sleep(10),
            wait_connection(Pid, Tries - 1)
	end.
