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
  test_common_errors/1,
  test_expires/1
]).

-define(KEY, list_to_binary(?MODULE_STRING ++ "_" ++ "key")).
-define(VALUE, <<"value">>).
-define(TTL, 1).

all() -> [
  test_local,
  test_do,
  test_api,
  test_common_errors,
  test_expires
].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(mcd),
  Config.

end_per_suite(_Config) ->
  ok = application:stop(mcd).

% Tests

test_do(_Config) ->
  Pid = get_pid(),
  {ok, [_ | _]} = mcd:version(Pid),
  {ok, flushed} = mcd:flush_all(Pid),
  {ok, flushed} = mcd:flush_all(Pid, 10),
  {error, notfound} = mcd:get(Pid, ?KEY),
  {error, notfound} = mcd:multi_get(Pid, [?KEY]),
  {error, notfound} = mcd:get(Pid, ?KEY),
  {error, notfound} = mcd:delete(Pid, ?KEY),
  try
      {ok, ?VALUE} = mcd:set(Pid, ?KEY, ?VALUE),
      {ok, ?VALUE} = mcd:set(Pid, ?KEY, ?VALUE, ?TTL),
      {ok, deleted} = mcd:delete(Pid, ?KEY)
  after
      mcd:delete(Pid, ?KEY),
      mcd:stop(Pid)
  end.

test_api(_Config) ->
  Pid = get_pid(),
  {ok, [_ | _]} = mcd:version(Pid),

  GetFun = fun() -> mcd:get(Pid, ?KEY) end,
  DeleteFun = fun() -> mcd:delete(Pid, ?KEY) end,

  test_set(GetFun, DeleteFun, fun() -> mcd:set(Pid, ?KEY, ?VALUE) end),
  test_set_expiration(GetFun, DeleteFun, fun() -> mcd:set(Pid, ?KEY, ?VALUE, ?TTL) end),
  mcd:stop(Pid).

test_expires(_Config) ->
  Pid = get_pid(),
  {error, notfound} = mcd:get(Pid, <<"a">>),
  {ok, <<"b">>} = mcd:set(Pid, <<"a">>, <<"b">>, 0),
  {ok, <<"b">>} = mcd:get(Pid, <<"a">>),
  timer:sleep(1500),
  {error, notfound} = mcd:get(Pid, <<"a">>).

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
