-module(krc_resolver_defaulty).
-behaviour(krc_resolver).
-export([resolve/2]).

resolve(_, _) -> {error, defaulty}.
