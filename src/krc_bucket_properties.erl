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
-export([ valid/1
	]).

%%%_* Code =============================================================
%%%_ * API -------------------------------------------------------------
valid(Props) ->
  lists:all(fun is_valid/1, Props).

%%%_ * Internals -------------------------------------------------------
%% This is by no means a complete input validation.
is_valid({n_val,           _N})          -> true;
is_valid({allow_mult,      Flag})        -> is_boolean(Flag);
is_valid({last_write_wins, Flag})        -> is_boolean(Flag);
is_valid({precommit,       _Precommit})  -> true;
is_valid({postcommit,      _Postcommit}) -> true;
is_valid({chash_keyfun,    _ModFun})     -> true;
is_valid({linkfun,         _ModFun})     -> true;
is_valid({old_vclock,      _Num})        -> true;
is_valid({young_vclock,    _Num})        -> true;
is_valid({big_vclock,      _Num})        -> true;
is_valid({small_vclock,    _Num})        -> true;
is_valid({pr,              _Q})          -> true;
is_valid({r,               _Q})          -> true;
is_valid({w,               _Q})          -> true;
is_valid({pw,              _Q})          -> true;
is_valid({dw,              _Q})          -> true;
is_valid({rw,              _Q})          -> true;
is_valid({basic_quorum,    _BQ})         -> true;
is_valid({notfound_ok,     Flag})        -> is_boolean(Flag);
is_valid({backend,         _B})          -> true;
is_valid({search,          Flag})        -> is_boolean(Flag);
is_valid({repl,            _Atom})       -> true;
is_valid(_)                              -> false.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
