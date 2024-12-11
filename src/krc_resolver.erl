%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Conflict-resolution procedures.
%%%
%%% Copyright 2013-2014 Kivra AB
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
-module(krc_resolver).

%%%_* Exports ==========================================================
%% API
-export([ compose/1
        ]).

-export_type([ resolution_procedure/0
             , resolver/0
             ]).

%%%_* Includes =========================================================
-include_lib("stdlib2/include/prelude.hrl").

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-type resolution_procedure() :: fun((A, A) -> 'maybe'(A, _)).
-type resolver()             :: module().

%%%_ * Behaviour -------------------------------------------------------
-callback resolve(A, A) -> 'maybe'(A, _).

%%%_ * API -------------------------------------------------------------
-spec compose([resolver()]) -> resolution_procedure().
compose([R]) ->
  fun(V1, V2) ->
    case resolve(R, V1, V2, []) of
      {ok, _} = Ok  -> Ok;
      {error, Rsns} -> {error, {conflict, V1, V2, Rsns}}
    end
  end;
compose([_, _|_] = Rs) ->
  fun(V1, V2) ->
    case (lists:foldl(s2_funs:flip(fun compose/2), hd(Rs), tl(Rs)))(V1, V2) of
      {ok, _} = Ok  -> Ok;
      {error, Rsns} -> {error, {conflict, V1, V2, Rsns}}
    end
  end.

-spec compose(resolver() | resolution_procedure(),
              resolver() | resolution_procedure()) -> resolution_procedure().
compose(R1, R2) ->
  fun(V1, V2) ->
    case resolve(R1, V1, V2, []) of
      {ok, _} = Ok -> Ok;
      {error, Rsn} -> resolve(R2, V1, V2, Rsn)
    end
  end.

resolve(X, V1, V2, Rsns) ->
  case ?lift(do_resolve(X, V1, V2)) of
    {ok, _} = Ok -> Ok;
    {error, Rsn} -> {error, lists:flatten(Rsns ++ [Rsn])}
  end.

do_resolve(R, V1, V2) when is_atom(R)     -> R:resolve(V1, V2);
do_resolve(F, V1, V2) when is_function(F) -> F(V1, V2).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

r1() -> fun(foo, bar) -> bar;
           (_, _)     -> throw({error, r1})
        end.
r2() -> fun(foo, foo) -> foo;
           (_, _)     -> throw({error, r2})
        end.
r3() -> fun(foo, foo) -> bar;
           (_, _)     -> throw({error, r3})
        end.

compose_test() ->
  R = compose([r1(), r2(), r3()]),
  {ok, foo} = R(foo, foo),
  {error, {conflict, bar, bar, [r1,r2,r3]}} = R(bar, bar),
  ok.

policy_test() ->
  Policy = [krc_resolver_eq, krc_resolver_tombstone] ++
    krc_policy_default:lookup(foo, bar),
  R = compose(Policy),
  {ok, 42} = R(42, 42),
  {error, {conflict, 42, 43, [nomatch, no_tombstone, defaulty]}} = R(42, 43).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
