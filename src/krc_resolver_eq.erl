-module(krc_resolver_eq).
-behaviour(krc_resolver).
-export([resolve/2]).

resolve(V, V) -> {ok, V};
resolve(_, _) -> {error, nomatch}.
