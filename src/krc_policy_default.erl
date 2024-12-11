-module(krc_policy_default).
-behaviour(krc_policy).
-export([lookup/2]).

lookup(_Bucket, _Key) ->
  [ krc_resolver_defaulty
  ].
