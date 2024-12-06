-module(krc_resolver_tombstone).
-behaviour(krc_resolver).
-export([resolve/2]).
-include_lib("krc/include/krc.hrl").

resolve(V1, ?TOMBSTONE) -> {ok, V1};
resolve(?TOMBSTONE, V2) -> {ok, V2};
resolve(_, _) -> {error, no_tombstone}.
