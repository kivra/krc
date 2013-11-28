%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc K Riak Client.
%%% The K Riak Client adds a higher-level API and a connection pool to the
%%% official Riak protobuf client. KRC does not pretend to be a generic client
%%% library but rather the simplest thing which works for us - our setup is
%%% described below.
%%%
%%% We have a cluster of N machines. Each machine hosts two BEAM emulators, one
%%% runs our application server and the other runs a Riak server. The Riak
%%% servers form a Riak cluster.
%%% Load-balancers distribute incoming requests amongst those application
%%% servers which are currently up.
%%%
%%% Each application server runs one instance of the gen_server defined in this
%%% file (globally registered name). The KRC gen_server maintains a number of
%%% TCP/IP connections to the Riak node co-located on its machine (localhost).
%%%
%%% The message flow is depicted below.
%%%
%%%
%%% application    ------------------------------------
%%%                \        |
%%% krc_server     ---------+--------------------------
%%%                  \      |
%%% connection     ------------------------------------
%%%                    \   /
%%% riak_pb_socket ------------------------------------
%%%                      \/
%%% riak server    ------------------------------------
%%%
%%%
%%% The application makes a request to the krc_server, which the krc_server
%%% forwards to one of its connection processes.
%%% Requests are buffered in the connection processes' message queues.
%%% Each connection talks to a riak_pb_socket process, which talks to the Riak
%%% server over TCP/IP.
%%%
%%% The failure modes are handled as follows:
%%%   - If an application process crashes, we drop any queued requests so as
%%%     not to send buffered write requests to the Riak server.
%%%   - If krc_server cannot reach its local Riak node, it crashes and the
%%%     application server goes down (this is mainly to avoid having to
%%%     maintain knowledge of the state of the Riak cluster locally, and may be
%%%     changed in a future release).
%%%   - The connection and riak_pb_socket processes are linked, so if either
%%%     dies, the other will be killed as well and all requests in the
%%%     connection's message queue will time out.
%%%
%%% Copyright 2013 Kivra AB
%%% Copyright 2011-2013 Klarna AB
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(krc_server).
-behaviour(gen_server).

%%%_* Exports ==========================================================
%% krc_server API
-export([ start/1
        , start/2
        , start_link/1
        , start_link/2
        , stop/1
        ]).

%% Riak API
-export([ delete/3
        , get/3
	, get_bucket/2
        , get_index/4
        , get_index/5
        , put/2
	, set_bucket/3
        ]).

%% gen_server callbacks
-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

%% Internal exports
-export([ connection/3
        ]).

%%%_* Includes =========================================================
-include("krc.hrl").
-include_lib("stdlib2/include/prelude.hrl").

%%%_* Macros ===========================================================
%% Make sure we time out internally before our clients time out.
-define(TIMEOUT,         120000). %gen_server:call/3
-define(QUEUE_TIMEOUT,   60000).
-define(CALL_TIMEOUT,    60000).
-define(MAX_DISCONNECTS, 3).
-define(FAILURES,        100). %max number of worker failures to tolerate
%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s,
        { client     :: atom()             %krc_riak_client
        , ip         :: inet:ip_address()  %\ Riak
        , port       :: inet:port_number() %/ server
        , pids       :: [pid()]            %Connections
        , failures=0 :: non_neg_integer()  %Connection crash counter
        , free       :: [pid()]
        , busy=[]    :: [pid()]
        , queue=queue:new()
        }).

-record(req,
        { from
        , ts          = throw('stamp')
        , req         = throw('req')
        , disconnects = 0
        }).

%%%_ * API -------------------------------------------------------------
delete(GS, B, K)          -> call(GS, {delete,    [B, K]   }).
get(GS, B, K)             -> call(GS, {get,       [B, K]   }).
get_bucket(GS, B)         -> call(GS, {get_bucket,[B]      }).
get_index(GS, B, I, K)    -> call(GS, {get_index, [B, I, K]}).
get_index(GS, B, I, L, U) -> call(GS, {get_index, [B, I, L, U]}).
put(GS, O)                -> call(GS, {put,       [O]      }).
set_bucket(GS, B, P)      -> call(GS, {set_bucket,[B, P]}).

start(A)            -> gen_server:start(?MODULE, A, []).
start(Name, A)      -> gen_server:start({local, Name}, ?MODULE, A, []).
start_link(A)       -> gen_server:start_link(?MODULE, A, []).
start_link(Name, A) -> gen_server:start_link({local, Name}, ?MODULE, A, []).
stop(GS)            -> gen_server:call(GS, stop).

call(GS, Req) -> gen_server:call(
                   GS, #req{ts=s2_time:stamp(),req=Req}, ?TIMEOUT).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  process_flag(trap_exit, true),
  Client   = s2_env:get_arg(Args, ?APP, client,    krc_pb_client),
  IP       = s2_env:get_arg(Args, ?APP, riak_ip,   "127.0.0.1"),
  Port     = s2_env:get_arg(Args, ?APP, riak_port, 8081),
  PoolSize = s2_env:get_arg(Args, ?APP, pool_size, 5),
  Pids     = [connection_start(Client, IP, Port, self()) ||
               _ <- lists:seq(1, PoolSize)],
  {ok, #s{client=Client, ip=IP, port=Port, pids=Pids, free=Pids}}.

terminate(_, #s{}) -> ok.

code_change(_, S, _) -> {ok, S}.

handle_call(stop, _From, S) ->
  {stop, stopped, ok, S}; %workers linked
handle_call(Req, From, #s{free=[]} = S) ->
  {noreply, S#s{queue=queue:in(Req#req{from=From}, S#s.queue)}};
handle_call(Req0, From, #s{free=[Pid|Pids]} = S) ->
  ?hence(queue:is_empty(S#s.queue)),
  Req = Req0#req{from=From},
  Pid ! {handle, Req},
  {noreply, S#s{free=Pids, busy=[{Pid,Req}|S#s.busy]}}.

handle_cast(_Msg, S) -> {stop, bad_cast, S}.

handle_info({'EXIT', Pid, disconnected}, #s{pids=Pids} = S) ->
  ?hence(lists:member(Pid, Pids)),
  case lists:keytake(Pid, 1, S#s.busy) of
    {value, {Pid, #req{disconnects=N}=Req}, Busy}
      when N+1 >= ?MAX_DISCONNECTS ->
      ?critical("EXIT disconnected: ~p", [Pid]),
      gen_server:reply(Req#req.from, {error, disconnected}),
      {stop, disconnected, S#s{busy=Busy, pids=Pids--[Pid]}};
    {value, {Pid, #req{disconnects=N}=Req}, Busy} ->
      NewPid = connection_start(S#s.client, S#s.ip, S#s.port, self()),
      ?info("Retrying disconncted request: ~p", [NewPid]),
      NewPid ! {handle, Req},
      {noreply, S#s{ pids = [NewPid|Pids] -- [Pid]
                   , busy = [{NewPid,Req#req{disconnects=N+1}}|Busy]}};
    false ->
      %% TODO: Since we don't have a limit on how many times
      %% a worker (without work) can reconnect we rely on
      %% that Riak is sensible here and don't disconnect
      %% us right away.
      %% This should possibly be replaced with a counter that
      %% can tell us how many disconnects we have had the last X
      %% minutes.
      NewPid = connection_start(S#s.client, S#s.ip, S#s.port, self()),
      ?info("Reconnecting disconnected worker: ~p", [NewPid]),
      {noreply, S#s{ pids = [NewPid|Pids] -- [Pid]
                   , free = [NewPid|S#s.free] -- [Pid]}}
  end;
handle_info({'EXIT', Pid, Rsn}, #s{failures=N} = S) when N > ?FAILURES ->
  %% We assume that the system is restarted occasionally anyway (for upgrades
  %% and such), so we don't bother resetting the counter.
  ?critical("EXIT ~p: ~p: too many failures", [Pid, Rsn]),
  ?increment([exits, failures]),
  {stop, failures, S};
handle_info({'EXIT', Pid, Rsn},
            #s{client=Client, ip=IP, port=Port, failures=N} = S) ->
  ?hence(lists:member(Pid, S#s.pids)),
  ?error("EXIT ~p: ~p", [Pid, Rsn]),
  ?increment([exits, other]),
  NewPid = connection_start(Client, IP, Port, self()),
  %% TODO: If this was a request in progress we might
  %% possibly want to respond something to the client
  {Free, Busy, Queue} = next_task([NewPid|S#s.free] -- [Pid],
                                  lists:keydelete(Pid, 1, S#s.busy),
                                  S#s.queue),
  {noreply, S#s{ pids     = [NewPid|S#s.pids] -- [Pid]
               , free     = Free
               , busy     = Busy
               , queue    = Queue
               , failures = N+1
               }};
handle_info({free, Pid, Res}, S) ->
  ?hence(lists:member(Pid, S#s.pids)),
  {value, {Pid, #req{from=From}}, Busy0} = lists:keytake(Pid, 1, S#s.busy),
  [gen_server:reply(From, Res) || Res =/= {error, dropped}],
  {Free, Busy, Queue} = next_task(S#s.free ++ [Pid],
                                  Busy0,
                                  S#s.queue),
  {noreply, S#s{ free  = Free
               , busy  = Busy
               , queue = Queue}};
handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

%%%_ * Internals -------------------------------------------------------
next_task([Pid|Free]=Free0, Busy, Queue0) ->
  case queue:out(Queue0) of
    {{value, Req}, Queue} ->
      Pid ! {handle, Req},
      {Free, [{Pid,Req}|Busy], Queue};
    {empty, Queue0} ->
      {Free0, Busy, Queue0}
  end.

%%%_  * Connections ----------------------------------------------------
connection_start(Client, IP, Port, Daddy) ->
  proc_lib:spawn_link(?thunk(
    {ok, Pid} = Client:start_link(IP, Port, copts()),
    connection(Client, Pid, Daddy))).

connection(Client, Pid, Daddy) ->
  receive
    {handle, #req{ts=TS, req=Req, from={Caller, _}}} ->
      case {s2_procs:is_up(Caller), time_left(TS)>0} of
        {true, true} ->
          case ?lift(do(Client, Pid, Req)) of
            {error, disconnected} ->
              %% special case due to shitty overload protection on riak side?
              %% request will possibly be retried
              exit(disconnected);
            {error, timeout} = Err ->
              ?error("timeout", []),
              ?increment([requests, timeouts]),
              Daddy ! {free, self(), Err};
            {error, notfound} = Err ->
              ?debug("notfound", []),
              ?increment([requests, notfound]),
              Daddy ! {free, self(), Err};
            {error, Rsn} = Err ->
              ?error("error: ~p", [Rsn]),
              ?increment([requests, errors]),
              Daddy ! {free, self(), Err};
            {ok, ok} ->
              ?increment([requests, ok]),
              Daddy ! {free, self(), ok};
            {ok, _} = Ok ->
              ?increment([requests, ok]),
              Daddy ! {free, self(), Ok}
          end;
        {false, _} ->
          ?info("dropping request ~p from ~p: DOWN", [Req, Caller]),
          ?increment([requests, dropped]),
          Daddy ! {free, self(), {error, dropped}};
        {_, false} ->
          ?info("dropping request ~p from ~p: out of time", [Req, Caller]),
          ?increment([requests, out_of_time]),
          Daddy ! {free, self(), {error, timeout}}
      end;
    Msg ->
      ?warning("~p", [Msg])
  end,
  ?MODULE:connection(Client, Pid, Daddy).

time_left(T0) ->
  T1        = s2_time:stamp(),
  ElapsedMs = (T1 - T0) / 1000,
  lists:max([?QUEUE_TIMEOUT - ElapsedMs, 0]).

-spec do(atom(), pid(), {atom(), [_]}) -> maybe(_, _).
do(Client, Pid, {F, A}) ->
  Args = [Pid] ++ A ++ opts(F) ++ [?CALL_TIMEOUT],
  ?debug("apply(~p, ~p, ~p)", [Client, F, Args]),
  apply(Client, F, Args).

opts(delete)    -> [dopts()];
opts(get)       -> [ropts()];
opts(get_bucket)-> [];
opts(get_index) -> [];
opts(put)       -> [wopts()];
opts(set_bucket)-> [].

%%%_  * Config ---------------------------------------------------------
%% Our app.config sets:
%%   n_val           : 3
%%   allow_mult      : true
%%   last_write_wins : false

%% Connections
copts() ->
  [ {auto_reconnect,  false}         %exit on TCP/IP error
  ].

%% Reads
ropts() ->
  [ {r,               quorum}        %\ Majority
  , {pr,              1}             %/ reads
  , {basic_quorum,    false}
  , {notfound_ok,     true}
  ].

%% Writes
wopts() ->
  [ {w,               quorum}        %\  Majority
  , {pw,              1}             % } disk
  , {dw,              quorum}        %/  writes
  ].

%% Deletes
dopts() ->
  [ {r,               quorum}        %\
  , {pr,              1}             % \
  , {rw,              quorum}        %  \ Majority
  , {w,               quorum}        %  / deletes
  , {pw,              1}             % /
  , {dw,              quorum}        %/
  ].

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test cases.
basic_test() ->
  krc_test:with_mock(?thunk(
    krc_test:spawn_sync(1000, ?thunk(
      Obj       = put_req(),
      {ok, Obj} = get_req())))).

client_down_test() ->
  krc_test:with_mock([{pool_size, 1}], ?thunk(
    krc_mock_client:lag(10),
    Pids = krc_test:spawn_async(10, ?thunk(put_req())), %Fill queue
    [P]  = krc_test:spawn_async(?thunk(timer:sleep(10), put_req())),
    timer:sleep(20),
    s2_procs:kill(P, [unlink]), %\ Request
    krc_test:sync(Pids))).      %/ dropped


out_of_time_test_() ->
  {timeout, 120,
   ?thunk(
      krc_test:with_mock([{pool_size, 1}], ?thunk(
      krc_mock_client:lag(?QUEUE_TIMEOUT - 1000),
      krc_test:spawn_async(?thunk({error, notfound} = get_req())),
      krc_test:spawn_async(?thunk({error, notfound} = get_req())),
      krc_test:spawn_sync(?thunk({error, timeout} = get_req()))))
     )}.

timeout_test_() ->
  {timeout, 120,
   ?thunk(
      krc_test:with_mock(?thunk(
      krc_mock_client:lag(?CALL_TIMEOUT + 1000),
      krc_test:spawn_sync(?thunk({error, timeout} = get_req()))))
     )}.

failures_test() ->
  ?MODULE:start([{riak_port, 6666}]).

worker_crash_test() ->
  krc_test:with_mock([{pool_size, 1}], ?thunk(
    krc_mock_client:lag(500),
    krc_test:spawn_sync(?thunk({error, notfound} = get_req())),
    {links, [Pid]} = erlang:process_info(whereis(krc_server), links),
    exit(Pid, die),
    timer:sleep(100), %make sure request is routed to new worker
    krc_test:spawn_async(?thunk({error, notfound} = get_req())),
    krc_test:spawn_sync(?thunk({error, notfound} = get_req())))).

disconnected_request_test() ->
  krc_test:with_mock(?thunk(
    krc_mock_client:disconnect(),
    krc_test:spawn_sync(?thunk({error, disconnected} = get_req())),
    timer:sleep(100))). %wait for 'EXIT' message

disconnected_test() ->
  krc_test:with_mock([{pool_size, 2}], ?thunk(
    {links, [Pid1,Pid2]} = erlang:process_info(whereis(krc_server), links),
    exit(Pid1, disconnected),
    exit(Pid2, disconnected),
    krc_test:spawn_sync(?thunk({error, notfound} = get_req()))
    )).


get_index_delete_test() ->
  krc_test:with_mock(?thunk(
    {ok, []} = ?MODULE:get_index(?MODULE, mah_bucket, mah_index, 42),
    ok       = ?MODULE:delete(?MODULE, mah_bucket, mah_key))).

coverage_test() ->
  krc_test:with_mock(?thunk(
     process_flag(trap_exit, true),
     {ok, Pid} = start_link([{client, krc_mock_client}]),
     {ok, _}   = start_link(mah_krc, [{client, krc_mock_client}]),
     Pid ! foo,
     gen_server:cast(mah_krc, foo),
     {ok, bar} = code_change(foo,bar,baz))).

%% Requests.
put_req() ->
  Obj = krc_obj:new(mah_bucket, self(), 42),
  ok  = ?MODULE:put(?MODULE, Obj),
  Obj.

get_req() -> ?MODULE:get(?MODULE, mah_bucket, self()).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
