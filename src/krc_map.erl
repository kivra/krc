%%%_* Module declaration ===============================================
-module(krc_map).
-compile({no_auto_import, [get/1, put/2]}).

%%%_* Exports ==========================================================
%% API
-export([ new/0
        , from_list/1
        , to_list/1
        , get/3
        , put/3
        , put/4
        , bucket_type/0
        ]).

%%%_* Includes =========================================================
-include("krc.hrl").

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-define(ATOM, <<"at">>).
-define(BINARY, <<"bin">>).
-define(INTEGER, <<"int">>).

%%%_ * API -------------------------------------------------------------
new() ->
    riakc_map:new().

from_list(List) ->
    from_list(new(), List).

to_list(Map) ->
    Value = riakc_map:value(Map),
    to_list(Value, []).

get(S, B, K) ->
    krc_server:get_map(S, B, K).

put(S, B, K, D) ->
    M = from_list(D),
    O = krc_obj:new(B, K, M, map),
    put(S, O, []).

put(S, O, Opts) ->
    krc_server:put(S, O, Opts).

bucket_type() ->
    application:get_env(?APP, map_bucket_type, <<"maps">>).

%%%_ * Internals -------------------------------------------------------
from_list(Map, []) ->
    Map;
from_list(Map0, [{Key, Atom} | List]) when is_atom(Atom) ->
    Map1 = set_field(Map0, Key, atom_to_binary(Atom), ?ATOM),
    from_list(Map1, List);
from_list(Map0, [{Key, Num} | List]) when is_integer(Num)->
    Map1 = set_field(Map0, Key, integer_to_binary(Num), ?INTEGER),
    from_list(Map1, List);
from_list(Map0, [{Key, Bin} | List]) when is_binary(Bin) ->
    Map1 = set_field(Map0, Key, Bin, ?BINARY),
    from_list(Map1, List);
from_list(Map0, [{Key, L} | List]) when is_list(L) ->
    Map1 = riakc_map:update({Key, map}, fun(M) -> from_list(M, L) end, Map0),
    from_list(Map1, List).

to_list([], List) ->
    List;
to_list([{{Key, map}, [{{<<"type">>, register}, Type}, {{<<"value">>, register}, Value}]} | Rest], List) ->
    to_list(Rest, [{Key, decode_value(Type, Value)} | List]);
to_list([{{Key, map}, L} | Rest], List) ->
    to_list(Rest, [{Key, to_list(L, [])} | List]).

set_field(Map, Key, Value, Type) ->
    riakc_map:update(
      {Key, map},
      fun(M0) ->
              M1 = riakc_map:update({<<"type">>, register}, fun(R) -> riakc_register:set(Type, R) end, M0),
              riakc_map:update({<<"value">>, register}, fun(R) -> riakc_register:set(Value, R) end, M1)
      end,
      Map).

decode_value(?ATOM, Value) -> binary_to_atom(Value);
decode_value(?INTEGER, Value) -> binary_to_integer(Value);
decode_value(_, Value) -> Value.
