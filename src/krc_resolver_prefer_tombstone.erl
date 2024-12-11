-module(krc_resolver_prefer_tombstone).
-behaviour(krc_resolver).
-export([resolve/2]).
-include_lib("krc/include/krc.hrl").

resolve(_, ?TOMBSTONE) -> {ok, ?TOMBSTONE};
resolve(?TOMBSTONE, _) -> {ok, ?TOMBSTONE};
resolve(_, _)          -> {error, no_tombstone}.
