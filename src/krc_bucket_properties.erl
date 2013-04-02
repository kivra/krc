%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc K Riak Client.
%%%
%%% Copyright 2013 Kivra AB
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

%%%_* Module declaration ===============================================
-module(krc_bucket_properties).

%%%_* Exports ==========================================================
-export([ encode/1
	, decode/1
	]).

%%%_* Code =============================================================
%%%_ * API -------------------------------------------------------------
encode(Props) ->
  case lists:all(fun is_valid/1, Props) of
    true  -> {ok, lists:map(fun to_riakc_pb/1, Props)};
    false -> {error, illegal_bucket_property}
  end.

decode(Props) ->
  lists:map(fun from_riakc_pb/1, Props).

%%%_ * Internals encode/input validation -------------------------------
is_valid({n_val,           N})     -> is_integer(N) andalso N > 0;
is_valid({allow_mult,      Flag})  -> is_boolean(Flag);
is_valid({last_write_wins, Flag})  -> is_boolean(Flag);
is_valid({precommit,       Hooks}) -> commit_hooks(Hooks);
is_valid({postcommit,      Hooks}) -> commit_hooks(Hooks);
is_valid({pr,              Q})     -> n_val(Q);
is_valid({r,               Q})     -> n_val(Q);
is_valid({w,               Q})     -> n_val(Q);
is_valid({pw,              Q})     -> n_val(Q);
is_valid({dw,              Q})     -> n_val(Q);
is_valid({rw,              Q})     -> n_val(Q);
is_valid({basic_quorum,    Flag})  -> is_boolean(Flag);
is_valid({notfound_ok,     Flag})  -> is_boolean(Flag);
is_valid({backend,         _B})    -> true;
is_valid(_)                        -> false.
%% Existing but not implemented.
%% is_valid({chash_keyfun,    _ModFun})     -> true;
%% is_valid({linkfun,         _ModFun})     -> true;
%% is_valid({old_vclock,      _Num})        -> true;
%% is_valid({young_vclock,    _Num})        -> true;
%% is_valid({big_vclock,      _Num})        -> true;
%% is_valid({small_vclock,    _Num})        -> true;
%%is_valid({search,          Flag})        -> is_boolean(Flag);
%%is_valid({repl,            _Atom})       -> true;

commit_hooks([])        -> true;
commit_hooks([{M,F}|T])
  when is_atom(M)
     , is_atom(F)       -> commit_hooks(T);
commit_hooks(_)         -> false.

n_val(all)                  -> true;
n_val(quorum)               -> true;
n_val(one)                  -> true;
n_val(N) when is_integer(N) -> N > 0;
n_val(_)                    -> false.

%%%_ * Internals encode ------------------------------------------------
to_riakc_pb({precommit,  Hooks}) -> encode_hooks(Hooks);
to_riakc_pb({postcommit, Hooks}) -> encode_hooks(Hooks);
to_riakc_pb(Opt)                 -> Opt.

encode_hooks(Hooks) ->
  [{struct, [{<<"mod">>, M}, {<<"fun">>, F}]} || {M,F} <- Hooks].

%%%_ * Internals decode ------------------------------------------------
%% TODO
from_riakc_pb(Opt) -> Opt.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
