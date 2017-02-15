%%%
%%% This module uses memcached protocol to interface memcached daemon:
%%% http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt
%%%
%%% EXPORTS:
%%%     mcd:start_link()
%%%     mcd:start_link([Address])
%%%     mcd:start_link([Address, Port])
%%%
%%% 	mcd:do(ServerRef, SimpleRequest)
%%% 	mcd:do(ServerRef, KeyRequest, Key)
%%% 	mcd:do(ServerRef, KeyDataRequest, Key, Data)
%%%	Type
%%%             ServerRef = as defined in gen_server(3)
%%%		SimpleRequest = version | flush_all | {flush_all, Expiration}
%%%		KeyRequest = get | delete
%%%		KeyDataRequest = Command | {Command, Flags, Expiration}
%%%		Command = set | add | replace
%%%
%%% Client may also use gen_server IPC primitives to request this module to
%%% perform storage and retrieval. Primitives are described in gen_server(3),
%%% that is, gen_server:call, gen_server:cast and others, using ServerRef
%%% returned by start_link(). Example: gen_server:cast(Server, Query).
%%%
%%% Recognized queries:
%%%   {Command, Key, Data}
%%%   {Command, Key, Data, Flags, Expiration}
%%%   {get, Key}
%%%   {delete, Key}
%%%   {incr, Key, Value}	% not implemented yet
%%%   {decr, Key, Value}	% not implemented yet
%%%   {version}
%%%   {flush_all}
%%%   {flush_all, Expiration}
%%% Return values:
%%%   {ok, Data}
%%%   {error, Reason}
%%% Where:
%%%   Command: set | add | replace
%%%   Key: term()
%%%   Data: term()
%%%   Flags: int()>=0
%%%   Expiration: int()>=0
%%%   Value: int()>=0
%%%   Time: int()>=0
%%%   Reason: noconn | notfound | notstored | overload | timeout | noproc | all_nodes_down
%%%
-module(mcd).
-behavior(gen_server).

-export([start_link/2, stop/1]).
% <BC>
-export([do/2, do/3, do/4]).
% </BC>
-export([
	get/2,
  get_multi/2,
	set/3,
	set/4,
	set/5,
	delete/2,
  flush_all/1,
	async_set/3,
	async_set/4,
	async_set/5,
	version/1
]).
-export([monitor/3]).
-export([data_receiver_loop/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export_type([
	server/0,
	expiration/0,
	flags/0,
	start_params/0,

	get_errors/0,
	set_errors/0,
	delete_errors/0,

	get_result/0,
	set_result/0,
	delete_result/0,
	flush_result/0,
	version_result/0
]).

-define('MAX_OUTSTANDING_REQUESTS', 1024).

% For selftesting
-define('UNKNOWN_HOST_NOCONN', "unknown_host_noconn_").
-define('UNKNOWN_HOST_OVERLOAD', "unknown_host_overload_").
-export([break_connection/1, fix_connection/1, overload_connection/1, unload_connection/1]).

-type common_errors() :: 'overload' | 'noconn' | 'timeout' | 'noproc' | 'all_nodes_down'
                                | {'server_error' | 'client_error', nonempty_string()}
                                | {'all_nodes_down', list(), node()}.
-type get_errors() :: common_errors() | 'notfound'.
-type set_errors() :: common_errors() | 'notstored'.
-type delete_errors() :: get_errors().

-type server() :: atom() | {atom(), node()} | pid().
-type expiration() :: non_neg_integer().
-type flags() :: 0..65535.

-type get_result() :: {'ok', term()} | {'error', get_errors()}.
-type set_result() :: {'ok', term()} | {'error', set_errors()}.
-type delete_result() :: {'ok', 'deleted'} | {'error', delete_errors()}.
-type flush_result() :: {'ok', 'flushed'} | {'error', common_errors()}.
-type version_result() :: {'ok', nonempty_string()} | {'error', common_errors()}.

% do() types

-type start_params() :: [nonempty_string() | pos_integer()].
-type start_result() :: {'ok', pid()} | 'ignore' | {'error', term()}.

-type simple_command() :: 'version' | 'flush_all'.
-type simple_request() :: simple_command() | {simple_command()} | {'flush_all', expiration()}.

-type key_command() :: 'get' | 'delete'.
-type key_request() :: key_command() | {key_command()}.

-type key_data_command() :: 'set' | 'add' | 'replace'.
-type key_data_request() :: key_data_command() | {key_data_command()} | {key_data_command(), flags(), expiration()}.

-type do_result() :: {'ok', term()} | {'error', get_errors() | set_errors() | delete_errors()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%
%% Start an anymous gen_server attached to a specified real memcached server.
%% Assumes localhost:11211 if no server address is given.
%%

-spec start_link(inet:ip_address(), inet:port_number()) -> start_result().
start_link(Address, Port) ->
	gen_server:start_link(?MODULE, {Address, Port}, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
  gen_server:stop(Pid).

%%
%% Call the specified memcached client gen_server with a request to ask
%% something from the associated real memcached process.
%%
%% The do/{2,3,4} is lighter than direct gen_server:call() to memcached
%% gen_server, since it spends some CPU in the requestor processes instead.
%%
%% See the file banner for possible requests.
%%

-spec do(ServerRef :: server(), SimpleRequest :: simple_request()) -> do_result().
do(ServerRef, SimpleRequest) when is_atom(SimpleRequest) ->
	do_forwarder(call, ServerRef, {SimpleRequest});
do(ServerRef, SimpleRequest) when is_tuple(SimpleRequest) ->
	do_forwarder(call, ServerRef, SimpleRequest).

-spec do(ServerRef :: server(), KeyRequest :: key_request(), Key :: term()) -> do_result().
do(ServerRef, KeyRequest, Key) when is_atom(KeyRequest) ->
	do_forwarder(call, ServerRef, {KeyRequest, Key});
do(ServerRef, {KeyRequest}, Key) ->
	do_forwarder(call, ServerRef, {KeyRequest, Key}).

-spec do(ServerRef :: server(), KeyDataReq :: key_data_request(), Key :: term(), Data :: term()) -> do_result().
do(ServerRef, KeyDataReq, Key, Data) when is_atom(KeyDataReq) ->
	do_forwarder(call, ServerRef, {KeyDataReq, Key, Data});
do(ServerRef, {Cmd}, Key, Data) ->
	do_forwarder(call, ServerRef, {Cmd, Key, Data});
do(ServerRef, {Cmd, Flag, Expires}, Key, Data) when is_integer(Flag), is_integer(Expires), Flag >= 0, Flag < 65536, Expires >= 0 ->
	do_forwarder(call, ServerRef, {Cmd, Key, Data, Flag, Expires}).

%% These helper functions provide more self-evident API.
-spec get(ServerRef :: server(), Key :: term()) -> get_result().
get(ServerRef, Key) when is_binary(Key) -> do(ServerRef, get, Key).

-spec get_multi(ServerRef :: server(), Keys :: [term()]) -> get_result().
get_multi(ServerRef, Keys) -> do(ServerRef, multi_get, Keys).

-spec set(ServerRef :: server(), Key :: term(), Data :: term()) -> set_result().
set(ServerRef, Key, Data) when is_binary(Key) -> do(ServerRef, set, Key, Data).

-spec set(ServerRef :: server(), Key :: term(), Data :: term(), Expiration :: expiration()) -> set_result().
set(ServerRef, Key, Data, Expiration) -> do(ServerRef, {set, 0, Expiration}, Key, Data).

-spec set(ServerRef :: server(), Key :: term(), Data :: term(), Expiration :: expiration(), Flags :: flags()) -> set_result().
% <BC>
set(ServerRef, Key, Data, 0, Expiration) -> do(ServerRef, {set, 0, Expiration}, Key, Data);
% </BC>
set(ServerRef, Key, Data, Expiration, Flags) -> do(ServerRef, {set, Flags, Expiration}, Key, Data).

-spec delete(ServerRef :: server(), Key :: term()) -> delete_result().
delete(ServerRef, Key) -> do(ServerRef, delete, Key).

-spec version(ServerRef :: server()) -> version_result().
version(ServerRef) -> do(ServerRef, version).

-spec flush_all(ServerRef :: server()) -> flush_result().
flush_all(ServerRef) -> do(ServerRef, flush_all).

% async functions

-spec async_set(ServerRef :: server(), Key :: term(), Data :: term()) -> term().
async_set(ServerRef, Key, Data) ->
	do_forwarder(cast, ServerRef, {set, Key, Data}),
	Data.

-spec async_set(ServerRef :: server(), Key :: term(), Data :: term(), Expiration :: expiration()) -> term().
async_set(ServerRef, Key, Data, Expiration) ->
	async_set(ServerRef, Key, Data, Expiration, 0).

-spec async_set(ServerRef :: server(), Key :: term(), Data :: term(), Expiration :: expiration(), Flags :: flags()) -> term().
% <BC>
async_set(ServerRef, Key, Data, 0, Expiration) ->
	do_forwarder(cast, ServerRef, {set, Key, Data, 0, Expiration}),
	Data;
% </BC>
async_set(ServerRef, Key, Data, Expiration, Flags) ->
	do_forwarder(cast, ServerRef, {set, Key, Data, Flags, Expiration}),
	Data.

%%
%% Enroll a specified monitoring process (MonitorPid) to receive
%% notifications about memcached state transitions and other anomalies.
%% This call sets or replaces the previous set of items to monitor for.
%%
%% @spec monitor(ServerRef, MonitorPid, MonitorItems)
%% Type MonitorPid = pid() | atom()
%%      MonitorItems = [MonitorItem]
%%      MonitorItem = state | overload
%%
monitor(ServerRef, MonitorPid, MonitorItems) when is_list(MonitorItems) ->
	gen_server:call(ServerRef, {set_monitor, MonitorPid, MonitorItems});
monitor(ServerRef, MonitorPid, MonitorItem) when is_atom(MonitorItem) ->
	?MODULE:monitor(ServerRef, MonitorPid, [MonitorItem]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% for selftesting
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec break_connection(ServerRef :: pid()) -> 'ok'.
break_connection(ServerRef) ->
    gen_server:call(ServerRef, break_connection).

-spec fix_connection(ServerRef :: pid()) -> 'ok' | {'error', 'not_broken'}.
fix_connection(ServerRef) ->
    gen_server:call(ServerRef, fix_connection).

-spec overload_connection(ServerRef :: pid()) -> 'ok'.
overload_connection(ServerRef) ->
    gen_server:call(ServerRef, overload_connection).

-spec unload_connection(ServerRef :: pid()) -> 'ok' | {'error', 'not_overloaded'}.
unload_connection(ServerRef) ->
    gen_server:call(ServerRef, unload_connection).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
	address, port = 11211, socket = nosocket,
	receiver,		% data receiver process
	requests = 0,		% client requests received
	outstanding = 0,	% client requests outstanding
	anomalies = {0, 0, 0},	% {queue overloads, reconnects, unused}
	status = disabled,	% connection status:
				%   disabled | ready
				%   | {connecting, Since, {Pid,MRef}}
				%   | {testing, Since}	% testing protocol
				%   | {wait, Since}	% wait between connects
	monitored_by = []	% monitoring processes to receive anomalies
	}).


init({Address, Port}) ->
  Receiver = start_data_receiver(self()),
  InitialState = #state{
    address = Address,
    port = Port,
    receiver = Receiver
  },
  FinalState = reconnect(InitialState),
  {ok, FinalState}.

start_data_receiver(Parent) ->
	spawn_monitor(fun() ->
		ParentMon = erlang:monitor(process, Parent),
		data_receiver_loop(Parent, ParentMon, undefined)
	end).

handle_call(status, _From, State) ->
	#state{requests = QTotal, outstanding = QOut, anomalies = {QOV, REC, _},
		status = Status} = State,
	{reply,
		[{requests, QTotal}, {outstanding, QOut}, {overloads, QOV},
		{reconnects, REC}, {status, Status}],
	State};
handle_call({set_monitor, MonitorPid, Items}, _From, #state{monitored_by=CurMons} = State) ->
	MonRef = erlang:monitor(process, MonitorPid),
	NewMons = add_monitor_pid_items(demonitor_pid(CurMons, MonitorPid),
			MonitorPid, MonRef, Items),
	MonitoredItemsForPid = collect_monitored_items(NewMons, MonitorPid),
	case MonitoredItemsForPid of
		[] -> erlang:demonitor(MonRef);
		_ -> ok
	end,
	{reply, MonitoredItemsForPid, State#state{monitored_by = NewMons}};


% unexpected disconnect simulation
handle_call(break_connection, _From, #state{socket = Socket, address = Address} = State) ->
    catch gen_tcp:close(Socket),
    {reply, ok, State#state{address = ?UNKNOWN_HOST_NOCONN ++ Address}};
handle_call(fix_connection, _From, #state{address = ?UNKNOWN_HOST_NOCONN ++ Address} = State) ->
    {reply, ok, State#state{address = Address}};
handle_call(fix_connection, _From, #state{} = State) ->
    {reply, {error, not_broken}, State};

handle_call(overload_connection, _From, #state{outstanding = QOut, address = Address} = State) ->
    {reply, ok, State#state{outstanding = QOut + ?MAX_OUTSTANDING_REQUESTS, address = ?UNKNOWN_HOST_OVERLOAD ++ Address}};
handle_call(unload_connection, _From, #state{outstanding = QOut, address = ?UNKNOWN_HOST_OVERLOAD ++ Address} = State) ->
    {reply, ok, State#state{outstanding = QOut - ?MAX_OUTSTANDING_REQUESTS, address = Address}};
handle_call(unload_connection, _From, #state{} = State) ->
    {reply, {error, not_overloaded}, State};

handle_call(Query, From, State) -> {noreply, schedule_query(State, Query, From)}.

% <BC>
handle_cast(restart_receiver, #state{socket = Socket, receiver = {Pid, MonRef}} = State) ->
	error_logger:info_msg("Restart memcached receiver ~p~n", [Pid]),
	erlang:demonitor(MonRef, [flush]),
	timer:apply_after(10000, erlang, exit, [Pid, kill]),
	{RcvrPid, _} = Rcvr = start_data_receiver(self()),
	RcvrPid ! {switch_receiving_socket, self(), Socket},
	{noreply, State#state{receiver = Rcvr}};
% </BC>
handle_cast({connected, Pid, nosocket},
		#state{socket = nosocket,
			status = {connecting, _, {Pid,_}}} = State) ->
	{Since, ReconnectDelay} = compute_next_reconnect_delay(State),
	erlang:start_timer(ReconnectDelay, self(), { may, reconnect }),
	{noreply, State#state { status = {wait, Since} }};
handle_cast({connected, Pid, NewSocket},
		#state{socket = nosocket,
			receiver = {RcvrPid, _},
			status = {connecting, _, {Pid,_}}} = State) ->

	RcvrPid ! {switch_receiving_socket, self(), NewSocket},

	{Since, ReconnectDelay} = compute_next_reconnect_delay(State),

	ReqId = State#state.requests,

	% We ask for version information, which will set our status to ready
	{Socket, NewStatus} = case construct_and_send_query(
				{self(), {connection_tested, NewSocket}},
				{version},
				NewSocket, State#state.receiver) of
		ok -> {NewSocket, {testing, Since}};
		{ error, _ } ->
			gen_tcp:close(NewSocket),
			erlang:start_timer(ReconnectDelay, self(),
				{ may, reconnect }),
			{nosocket, {wait, Since}}
	end,

	% Remember this socket in a new state.
	{noreply, State#state { socket = Socket,
		status = NewStatus,
		requests = ReqId + 1,
		outstanding = 1
		}};

handle_cast({connected, _, nosocket}, State) -> {noreply, State};
handle_cast({connected, _, Socket}, State) ->
	gen_tcp:close(Socket),
	{noreply, State};
handle_cast(Query, State) -> {noreply, schedule_query(State, Query, anon)}.

handle_info({request_served, Socket}, #state{socket=Socket, outstanding=QOut}=State) -> {noreply, State#state{outstanding=QOut - 1}};
handle_info({{connection_tested, Socket}, {ok, _Version}}, #state{socket = Socket, status = {testing, _}} = State) ->
	report_event(State, state, up),
	{noreply, State#state{status = ready}};
handle_info({timeout, _, {may, reconnect}}, State) -> {noreply, reconnect(State)};
handle_info({tcp_closed, Socket}, #state{socket = Socket} = State) ->
	{noreply, reconnect(State#state{socket = nosocket})};
handle_info({'DOWN', MonRef, process, Pid, _Info}, #state{status={connecting,_,{Pid,MonRef}}}=State) ->
	error_logger:info_msg("Memcached connector died (~p),"
			" simulating nosock~n", [_Info]),
	handle_cast({connected, Pid, nosocket}, State);
handle_info({'DOWN', MonRef, process, Pid, _Info} = Info, #state{receiver={Pid,MonRef}}=State) ->
	error_logger:error_msg("Memcached receiver died (~p)~n", [Info]),
	{stop, {receiver_down, _Info}, State};

handle_info({'DOWN', MonRef, process, Pid, _Info}, #state{monitored_by=Mons}=State) ->
	{noreply, State#state{
		monitored_by = remove_monitor_pid_and_ref(Mons, Pid, MonRef)
		} };
handle_info(_Info, State) ->
	io:format("Some info: ~p~n", [_Info]),
	{noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
terminate(_Reason, _State) -> ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Remove the specified pid from the lists of nodes monitoring this gen_server.
demonitor_pid(Monitors, MonitorPid) ->
	[{Item, NewPids}
		|| {Item, PidRefs} <- Monitors,
		   NewPids <- [[PM || {P, MonRef} = PM <- PidRefs,
				erlang:demonitor(MonRef) == true,
				P /= MonitorPid]],
		   NewPids /= []
	].

remove_monitor_pid_and_ref(Monitors, Pid, MonRef) ->
	[{Item, NewPids}
		|| {Item, PidRefs} <- Monitors,
		   NewPids <- [[PM || {P, MR} = PM <- PidRefs,
				P /= Pid, MR /= MonRef]],
		   NewPids /= []
	].

% Add the specified pid to the lists of nodes monitoring this gen_server.
add_monitor_pid_items(Monitors, MonitorPid, MonRef, Items) ->
	lists:foldl(fun(Item, M) ->
		add_monitor_pid_item(M, MonitorPid, MonRef, Item)
	end, Monitors, Items).

add_monitor_pid_item(Monitors, Pid, MonRef, I) when I == state; I == overload ->
	NewMons = [{Item, NewPids}
		|| {Item, Pids} <- Monitors,
		   NewPids <- [case Item of
				I -> [{Pid, MonRef} | Pids];
				_ -> Pids
				end]
	],
	case lists:keysearch(I, 1, NewMons) of
		false -> [{I, [{Pid, MonRef}]}|NewMons];
		{value, _} -> NewMons
	end;
add_monitor_pid_item(Monitors, _Pid, _MonRef, _Item) -> Monitors.

report_event(#state{monitored_by = Mons} = State, Event, Info) ->
	[P ! {memcached, self(), Event, Info}
		|| {Item, Pids} <- Mons, Item == Event, {P, _} <- Pids],
	State.

% Figure out what items this pid monitors.
collect_monitored_items(Monitors, MonitorPid) ->
	[Item || {Item, Pids} <- Monitors,
		lists:keysearch(MonitorPid, 1, Pids) /= false].

% @spec utime(now()) -> int()
utime({Mega, Secs, _}) -> 1000000 * Mega + Secs.

incr_anomaly({QOverloads, Reconnects, Unused}, overloads) ->
	{QOverloads + 1, Reconnects, Unused};
incr_anomaly({QOverloads, Reconnects, Unused}, reconnects) ->
	{QOverloads, Reconnects + 1, Unused};
incr_anomaly(Anomaly, FieldName) ->
	error_logger:error_msg("Anomaly ~p couldn't be increased in ~p~n",
		[FieldName, Anomaly]),
	Anomaly.

%% Destroy the existing connection and create a new one based on State params
%% @spec reconnect(record(state)) -> record(state)
reconnect(#state{status = {connecting, _, {_Pid,_MRef}}} = State) ->
	% Let the reconnect process continue.
	State;
reconnect(#state{address = Address, port = Port, socket = OldSock} = State) ->
	% Close the old socket, if available
	case OldSock of
		nosocket -> ok;
		_ -> gen_tcp:close(OldSock)
	end,

	Self = self(),
	{Pid,MRef} = spawn_monitor(fun() ->
			reconnector_process(Self, Address, Port) end),

	% We want to reconnect but we can't do it immediately, since
	% the tcp connection could be failing right after connection attempt.
	% So let it cook for a period of time before the next retry.
	{Since, _ReconnectDelay} = compute_next_reconnect_delay(State),

	NewAnomalies = case is_atom(State#state.status) of
		false -> State#state.anomalies;
		true ->
			report_event(State, state, down),
			incr_anomaly(State#state.anomalies, reconnects)
	end,

	State#state { socket = nosocket,
		status = {connecting, Since, {Pid, MRef}},
		outstanding = 0,
		anomalies = NewAnomalies }.

compute_next_reconnect_delay(#state{status = Status}) ->
	ComputeReconnectDelay = fun(Since) ->
		% Wait increasingly longer,
		% but no longer than 5 minutes.
		case (utime(os:timestamp()) - utime(Since)) of
			N when N > 300 -> 300 * 1000;
			N -> N * 1000
		end
	end,
	case Status of
		{connecting, Since, _} -> {Since, ComputeReconnectDelay(Since)};
		{testing, Since} -> {Since, ComputeReconnectDelay(Since)};
		{wait, Since} -> {Since, ComputeReconnectDelay(Since)};
		_ -> {os:timestamp(), 1000}
	end.

reconnector_process(MCDServerPid, Address, Port) ->
	error_logger:info_msg("Creating interface ~p to memcached on ~p:~p~n",
          [MCDServerPid, Address,Port]),

	Socket = case gen_tcp:connect(Address, Port,
			[{packet, line}, binary, {active, false}], 5000) of
		{ ok, Sock } ->
			gen_tcp:controlling_process(Sock, MCDServerPid),
			Sock;
		{ error, _Reason } -> nosocket
	end,
	gen_server:cast(MCDServerPid, {connected, self(), Socket}).


%%
%% Send a query to the memcached server and add it to our local table
%% to capture corresponding server response.
%% This asynchronous process provides necessary pipelining for remote or
%% lagging memcached processes.
%%

schedule_query(#state{requests = QTotal, outstanding = QOut, receiver = Rcvr, socket = Socket, status = ready} = State, Query, From) when QOut < ?MAX_OUTSTANDING_REQUESTS ->
	case construct_and_send_query(From, Query, Socket, Rcvr) of
		ok -> State#state{requests = QTotal+1, outstanding = QOut+1};
		{error, _Reason} -> reconnect(State)
	end;
schedule_query(State, _Query, From) ->
	#state{outstanding = QOut, anomalies = An, status = Status} = State,
	if
		QOut >= ?MAX_OUTSTANDING_REQUESTS ->
			reply_back(From, {error, overload}),
			report_event(State, overload, []),
			State#state{anomalies = incr_anomaly(An, overloads)};
		Status =/= ready ->
			reply_back(From, {error, noconn}),
			State
	end.

construct_and_send_query(From, {'$constructed_query', _KeyMD5, {OTARequest, ReqType, ExpectationFlags}}, Socket, {RcvrPid, _}) ->
	RcvrPid ! {accept_response, From, ReqType, ExpectationFlags},
	gen_tcp:send(Socket, OTARequest);
construct_and_send_query(From, Query, Socket, {RcvrPid, _}) ->
	{_MD5Key, OTARequest, ReqType} = construct_memcached_query(Query),
	RcvrPid ! {accept_response, From, ReqType, []},
	gen_tcp:send(Socket, OTARequest).

%%
%% Format the request and call the server synchronously
%% or cast a message asynchronously, without waiting for the result.
%%
do_forwarder(Method, ServerRef, Req) ->
  case construct_memcached_query(Req) of
    {error, bad_arg} -> {error, bad_arg};
    {KeyMD5, IOL, T} ->
      Q = iolist_to_binary(IOL),
      try gen_server:Method(ServerRef,
        {'$constructed_query', KeyMD5, {Q, T, [raw_blob]}}) of

        % Return the actual Data piece which got stored on the
        % server. Since returning Data happens inside the single
        % process, this has no copying overhead and is nicer than
        % returning {ok, stored} to successful set/add/replace commands.
        {ok, stored} when T == rtCmd -> {ok, element(3, Req)};

        % Memcached returns a blob which needs to be converted
        % into to an Erlang term. It's better to do it in the requester
        % process space to avoid inter-process copying of potentially
        % complex data structures.
        {ok, {'$value_blob', B}} -> {ok, B};

        Response -> Response
      catch
        exit:{timeout, {gen_server, call, _}} ->
          {error, timeout};
        exit:{noproc, {gen_server, call, _}} ->
          {error, noproc}
      end
  end.

%% Translate a query tuple into memcached protocol string and the
%% atom suggesting a procedure for parsing memcached server response.
%%
%% @spec construct_memcached_query(term()) -> {md5(), iolist(), ResponseKind}
%% Type ResponseKind = atom()
%%
construct_memcached_query({version}) -> {<<>>, [<<"version\r\n">>], rtVer};
construct_memcached_query({set, Key, Data}) ->
	construct_memcached_query_cmd("set", Key, Data);
construct_memcached_query({set, Key, Data, Flags, Expiration}) ->
	construct_memcached_query_cmd("set", Key, Data, Flags, Expiration);
construct_memcached_query({add, Key, Data}) ->
	construct_memcached_query_cmd("add", Key, Data);
construct_memcached_query({add, Key, Data, Flags, Expiration}) ->
	construct_memcached_query_cmd("add", Key, Data, Flags, Expiration);
construct_memcached_query({replace, Key, Data}) ->
	construct_memcached_query_cmd("replace", Key, Data);
construct_memcached_query({replace, Key, Data, Flags, Expiration}) ->
	construct_memcached_query_cmd("replace", Key, Data, Flags, Expiration);
construct_memcached_query({get, Key}) ->
  case parse_to_binary_if_possible(Key) of
    error -> {error, bad_arg};
    BinaryKey -> {BinaryKey, ["get ", BinaryKey, "\r\n"], rtGet}
  end;
construct_memcached_query({multi_get, Keys}) ->
  JoinedKeys = binary_join(Keys, <<" ">>, <<>>),
  {JoinedKeys, ["get ", JoinedKeys, "\r\n"], rtMultiGet};
% <BC>
construct_memcached_query({delete, Key, _}) ->
	construct_memcached_query({delete, Key});
% </BC>
construct_memcached_query({delete, Key}) ->
	{Key, ["delete ", Key, "\r\n"], rtDel};
construct_memcached_query({incr, Key, Value})
		when is_integer(Value), Value >= 0 ->
	{Key, ["incr ", Key, " ", integer_to_list(Value), "\r\n"], rtInt};
construct_memcached_query({decr, Key, Value})
		when is_integer(Value), Value >= 0 ->
	{Key, ["decr ", Key, " ", integer_to_list(Value), "\r\n"], rtInt};
construct_memcached_query({flush_all, Expiration})
		when is_integer(Expiration), Expiration >= 0 ->
	{<<>>, ["flush_all ", integer_to_list(Expiration), "\r\n"], rtFlush};
construct_memcached_query({flush_all}) -> {<<>>, ["flush_all\r\n"], rtFlush}.

%% The "set", "add" and "replace" queries do get optional
%% "flag" and "expiration time" attributes. So these commands fall into
%% their own category of commands (say, ternary command). These commads'
%% construction is handled by this function.
%%
%% @spec construct_memcached_query(term()) -> {md5(), iolist(), ResponseKind}
%% Type ResponseKind = atom()
%%
construct_memcached_query_cmd(Cmd, Key, Data) ->
	construct_memcached_query_cmd(Cmd, Key, Data, 0, 0).
construct_memcached_query_cmd(Cmd, Key, Data, Flags, Exptime)
	when is_list(Cmd), is_integer(Flags), is_integer(Exptime),
	Flags >= 0, Flags < 65536, Exptime >= 0 ->
  case parse_to_binary_if_possible(Key) of
    error -> {error, bad_arg};
    BinaryKey ->
      case parse_to_binary_if_possible(Data) of
        error -> {error, bad_arg};
        BinData ->
          {Key, [Cmd, " ", BinaryKey, " ", integer_to_list(Flags), " ", integer_to_list(Exptime),
            " ", integer_to_list(size(BinData)),
            "\r\n", BinData, "\r\n"
          ], rtCmd}
      end
  end.

parse_to_binary_if_possible(Data) when is_binary(Data)->
	Data;
parse_to_binary_if_possible(Data) when is_list(Data) ->
	list_to_binary(Data);
parse_to_binary_if_possible(Data) when is_integer(Data) ->
	integer_to_binary(Data);
parse_to_binary_if_possible(Data) when is_atom(Data) ->
	atom_to_binary(Data, utf8);
parse_to_binary_if_possible(Data) ->
	error_logger:error_msg("Failed to parse ~60p to binary", [Data]),
	error.

-spec binary_join([binary()], binary(), binary()) -> binary().
binary_join([], _Sep, Current) ->
  Current;
binary_join([SinglePart], _Sep, Current) ->
  case parse_to_binary_if_possible(SinglePart) of
    error -> <<>>;
    BinaryPart -> <<Current/binary, BinaryPart/binary>>
  end;
binary_join([Head|Tail], Sep, Current) ->
  case parse_to_binary_if_possible(Head) of
    error -> <<>>;
    BinaryHead ->
      NewBits = <<Current/binary, BinaryHead/binary, Sep/binary>>,
      binary_join(Tail, Sep, NewBits)
  end.

reply_back(anon, _) -> true;
reply_back(From, Result) -> gen_server:reply(From, Result).

data_receiver_loop(Parent, ParentMon, Socket) ->
	NewSocket = receive
	  {accept_response, RequestorFrom, Operation, Opts} when Socket /= undefined ->
		try data_receiver_accept_response(Operation, Opts, Socket) of
		  Value ->
			Parent ! {request_served, Socket},
			reply_back(RequestorFrom, Value),
			Socket
		catch
		  error:{badmatch,{error,_}} ->
			Parent ! {tcp_closed, Socket},
			reply_back(RequestorFrom, {error, noconn}),
			undefined
		end;
	  {accept_response, RequestorFrom, _, _} ->
		reply_back(RequestorFrom, {error, noconn}),
		Socket;
	  {switch_receiving_socket, Parent, ReplaceSocket} ->
		ReplaceSocket;
	  {'DOWN', ParentMon, process, Parent, _} -> exit(normal);
	  _Message -> Socket
	after 1000 -> Socket
	end,
	?MODULE:data_receiver_loop(Parent, ParentMon, NewSocket).

data_receiver_accept_response(rtVer, _, Socket) ->
	{ok, Response} = gen_tcp:recv(Socket, 0),
	case string:tokens(binary_to_list(Response), " \r\n") of
		["VERSION", Value | _] -> {ok, Value};
		_ -> data_receiver_error_reason(Response)
	end;
data_receiver_accept_response(rtGet, ExpFlags, Socket) ->
  case retrieve_one_get_result(ExpFlags, Socket, single) of
    response_end -> {error, notfound};
    response_notfound -> {error, notfound};
    {ok, {'$value_blob', {_Key, Val}}} -> {ok, {'$value_blob', Val}};
    {ok, {_Key, Val}} -> {ok, Val}
  end;
data_receiver_accept_response(rtMultiGet, ExpFlags, Socket) ->
  case multi_get_helper(ExpFlags, Socket, []) of
    {error, notfound} -> {error, notfound};
    [] -> {error, notfound};
    %% Since we prepend each element in multi_get_helper, we need to do a reverse here to make sure that the values
    %% returned are align with the keys in the request
    Vals -> {ok, lists:reverse(Vals)}
  end;
data_receiver_accept_response(rtInt, _, Socket) ->
	{ok, Response} = gen_tcp:recv(Socket, 0),
	case string:to_integer(binary_to_list(Response)) of
	  {Int, "\r\n"} when is_integer(Int) -> {ok, Int};
	  {error, _} when Response == <<"NOT_FOUND\r\n">> -> {error, notfound};
	  {error, _} -> data_receiver_error_reason(Response)
	end;
data_receiver_accept_response(rtCmd, _, Socket) ->
	data_receiver_accept_choice(Socket,
		[ {<<"STORED\r\n">>, {ok, stored}},
		  {<<"NOT_STORED\r\n">>, {error, notstored}} ]);
data_receiver_accept_response(rtDel, _, Socket) ->
	data_receiver_accept_choice(Socket,
		[ {<<"DELETED\r\n">>, {ok, deleted}},
		  {<<"NOT_FOUND\r\n">>, {error, notfound}} ]);
data_receiver_accept_response(rtFlush, _, Socket) ->
	data_receiver_accept_choice(Socket, [ {<<"OK\r\n">>, {ok, flushed}} ]).

data_receiver_accept_choice(Socket, Alternatives) ->
	{ok, Response} = gen_tcp:recv(Socket, 0),
	case lists:keysearch(Response, 1, Alternatives) of
		{value, {_, Answer}} -> Answer;
		false -> data_receiver_error_reason(Response)
	end.

data_receive_binary(Socket, DataSize) when is_integer(DataSize) ->
	{ok, Binary} = gen_tcp:recv(Socket, DataSize),
	Binary.

data_receiver_error_reason(<<"SERVER_ERROR ", Reason/binary>>) ->
	data_receiver_error_reason(server_error, Reason);
data_receiver_error_reason(<<"CLIENT_ERROR ", Reason/binary>>) ->
	data_receiver_error_reason(client_error, Reason).

data_receiver_error_reason(Code, Reason) ->
	{error, {Code, [C || C <- binary_to_list(Reason), C >= $ ]}}.

retrieve_one_get_result(ExpFlags, Socket, Flag) ->
  {ok, HeaderLine} = gen_tcp:recv(Socket, 0),
  case HeaderLine of
  % Quick test before embarking on tokenizing
    <<"END\r\n">> -> response_end;
    <<"SERVER_ERROR ",_/binary>> -> response_notfound;
    _ ->
      ["VALUE", Key, _Flag, DataSizeStr]
        = string:tokens(binary_to_list(HeaderLine), " \r\n"),
      ok = inet:setopts(Socket, [{packet, raw}]),
      Bin = data_receive_binary(Socket, list_to_integer(DataSizeStr)),
      % Read out other useless bytes
      case Flag of
        single ->
          <<"\r\nEND\r\n">> = data_receive_binary(Socket, 7);
        multi ->
          <<"\r\n">> = data_receive_binary(Socket, 2)
      end,
      ok = inet:setopts(Socket, [{packet, line}]),
      case proplists:get_value(raw_blob, ExpFlags) of
        true -> {ok, {'$value_blob', {list_to_binary(Key), Bin}}};
        _ -> {ok, {list_to_binary(Key), Bin}}
      end
  end.

multi_get_helper(ExpFlags, Socket, CurrentResult) ->
  case retrieve_one_get_result(ExpFlags, Socket, multi) of
    response_end -> CurrentResult;
    response_notfound -> {error, notfound};
    {ok, {'$value_blob', {Key, Val}}} -> multi_get_helper(ExpFlags, Socket, [{Key, Val}|CurrentResult]);
    {ok, {Key, Val}} -> multi_get_helper(ExpFlags, Socket, [{Key, Val}|CurrentResult])
  end.
