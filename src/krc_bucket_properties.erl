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
%% This is not a complete input validation but a precheck before
%% handing over the request.
is_valid({n_val,           N})     -> is_integer(N) andalso N > 0;
is_valid({allow_mult,      Flag})  -> is_boolean(Flag);
is_valid({last_write_wins, Flag})  -> is_boolean(Flag);
is_valid({precommit,       Hooks}) -> is_commit_hooks(Hooks);
is_valid({postcommit,      Hooks}) -> is_commit_hooks(Hooks);
is_valid({pr,              Q})     -> is_n_val(Q);
is_valid({r,               Q})     -> is_n_val(Q);
is_valid({w,               Q})     -> is_n_val(Q);
is_valid({pw,              Q})     -> is_n_val(Q);
is_valid({dw,              Q})     -> is_n_val(Q);
is_valid({rw,              Q})     -> is_n_val(Q);
is_valid({basic_quorum,    Flag})  -> is_boolean(Flag);
is_valid({notfound_ok,     Flag})  -> is_boolean(Flag);
is_valid({backend,         B})     -> is_list(B);
is_valid(_)                        -> false.
%% Existing but not implemented.
%% chash_keyfun
%% linkfun
%% old_vclock
%% young_vclock
%% big_vclock
%% small_vclock
%% search
%% repl

is_commit_hooks([])        -> true;
is_commit_hooks([{M,F}|T])
  when is_atom(M)
     , is_atom(F)          -> is_commit_hooks(T);
is_commit_hooks(_)         -> false.

is_n_val(all)                  -> true;
is_n_val(quorum)               -> true;
is_n_val(one)                  -> true;
is_n_val(N) when is_integer(N) -> N > 0;
is_n_val(_)                    -> false.

to_riakc_pb({precommit,  Hooks}) -> {precommit,  encode_hooks(Hooks)};
to_riakc_pb({postcommit, Hooks}) -> {postcommit, encode_hooks(Hooks)};
to_riakc_pb(Opt)                 -> Opt.

encode_hooks(Hooks) ->
  lists:map(fun({Mod,Fun}) ->
               M = list_to_binary(atom_to_list(Mod)),
               F = list_to_binary(atom_to_list(Fun)),
               {struct, [{<<"mod">>, M}, {<<"fun">>, F}]}
	    end, Hooks).

%%%_ * Internals decode ------------------------------------------------
from_riakc_pb({precommit,  Hooks}) -> {precommit,  decode_hooks(Hooks)};
from_riakc_pb({postcommit, Hooks}) -> {postcommit, decode_hooks(Hooks)};
from_riakc_pb({backend, B})        -> {backend, binary_to_list(B)};
from_riakc_pb(Opt)                 -> Opt.

decode_hooks(Hooks) ->
  lists:map(fun({struct, Props}) ->
               {ok, M} = s2_lists:assoc(Props, <<"mod">>),
               {ok, F} = s2_lists:assoc(Props, <<"fun">>),
               {list_to_atom(binary_to_list(M)),
                list_to_atom(binary_to_list(F))}
	    end, Hooks).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

commit_hook_encode_decode_test() ->
  Dec = [{foo,bar}],
  Enc = [{struct, [{<<"mod">>, <<"foo">>}, {<<"fun">>, <<"bar">>}]}],
  {ok, [{precommit,  Enc}]} = encode([{precommit,  Dec}]),
  {ok, [{postcommit, Enc}]} = encode([{postcommit, Dec}]),
  [{postcommit, Dec}] = decode([{postcommit, Enc}]),
  [{precommit,  Dec}] = decode([{precommit,  Enc}]).

illegal_property_test() ->
  {error, illegal_bucket_property} = encode([{blah, foo}]),
  {error, illegal_bucket_property} = encode([{precommit, blah}]),
  {error, illegal_bucket_property} = encode([{w, ten}]).

cover_test() ->
  {ok, _} = encode([ {n_val, 3}
		   , {allow_mult, true}
		   , {last_write_wins, false}
		   , {pr, all}
		   , {r,  one}
		   , {w,  quorum}
		   , {pw, 2}
		   , {dw, 1}
		   , {rw, 3}
		   , {basic_quorum, true}
		   , {notfound_ok, true}
		   , {backend, "blah"}
		   ]).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
