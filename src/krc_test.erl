%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Test support library.
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
-module(krc_test).

%%%_* Exports ==========================================================
-export([ gen_inputs/1
        , spawn_sync/1
        , spawn_sync/2
        , spawn_async/1
        , spawn_async/2
        , sync/1
        , with_mock/1
        , with_mock/2
        , with_pb/2
        , with_pb/3
        ]).

%%%_* Includes =========================================================
-include_lib("stdlib2/include/prelude.hrl").

%%%_* Code =============================================================
%%%_ * spawn_* ---------------------------------------------------------
spawn_sync(Thunk)     -> do_spawn([], Thunk).
spawn_sync(N, Thunk)  -> do_spawn([{n, N}], Thunk).

spawn_async(Thunk)    -> do_spawn([{sync, false}], Thunk).
spawn_async(N, Thunk) -> do_spawn([{sync, false}, {n, N}], Thunk).

do_spawn(Opts, Thunk) ->
  N    = s2_lists:assoc(Opts, n,    1),
  Sync = s2_lists:assoc(Opts, sync, true),
  Self = self(),
  Pids = [proc_lib:spawn_link(?thunk(Thunk(), Self ! {self(), sync})) ||
           _ <- lists:seq(1, N)],
  if Sync -> sync(Pids);
     true -> Pids
  end.

sync([])   -> ok;
sync(Pids) -> receive {Pid, sync} -> sync(Pids -- [Pid]) end.

%%%_ * with_* ----------------------------------------------------------
with_mock(Thunk) ->
  with_mock([], Thunk).
with_mock(Opts, Thunk) ->
  try
    {ok, _} = krc_mock_client:start(),
    s2_procs:spinlock(?thunk(lists:member(krc_mock_client, registered()))),
    {ok, _} = krc_server:start(krc_server, [{client, krc_mock_client}|Opts]),
    s2_procs:spinlock(?thunk(lists:member(krc_server, registered()))),
    Thunk()
  after
    catch krc_server:stop(krc_server),
    s2_procs:spinlock(?thunk(not lists:member(krc_server, registered()))),
    krc_mock_client:stop(),
    s2_procs:spinlock(?thunk(not lists:member(krc_mock_client, registered())))
  end.


with_pb(N, Fun) ->
  with_pb([], N, Fun).
with_pb(Opts, N, Fun) ->
  try
    {ok, _} = krc_server:start(krc_server, [{client, krc_pb_client}|Opts]),
    s2_procs:spinlock(?thunk(lists:member(krc_server, registered()))),
    Inputs = fresh_inputs(krc_server, N),
    Fun(Inputs)
  after
    krc_server:stop(krc_server),
    s2_procs:spinlock(?thunk(not lists:member(krc_server, registered())))
  end.

fresh_inputs(Pid, N) ->
  Inputs = gen_inputs(N),
  [ok = krc:delete(Pid, B, K) || {B, K, _, _, _} <- Inputs],
  Inputs.

gen_inputs(N) ->
  [{genbucket(), genkey(), genidx(), genidxkey(), genval()} ||
    _ <- lists:seq(1, N)].

genbucket()  -> gensym(bucket).
genkey()     -> gensym(key).
genidx()     -> gensym(index).
genidxkey()  -> gensym(index_key).
genval()     -> gensym(val).

gensym(Stem) -> s2_atoms:catenate([?MODULE, '_', Stem, '_', rand()]).
rand()       -> crypto:rand_uniform(0, 1 bsl 128).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
