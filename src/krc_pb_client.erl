%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Protobuf client.
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
-module(krc_pb_client).
-behaviour(krc_riak_client).

%%%_* Exports ==========================================================
-export([ delete/4
        , delete/5
        , get/5
        , get_bucket/3
        , get_index/5
        , get_index/6
        , get_map/5
        , list_keys/3
        , put/4
        , putwo/4
        , set_bucket/4
        , start_link/3
        ]).

%%%_* Includes =========================================================
-include_lib("stdlib2/include/prelude.hrl").
-include_lib("riakc/include/riakc.hrl").

%%%_* Code =============================================================
delete(Pid, Obj, Options, Timeout) ->
  case
    riakc_pb_socket:delete_obj(Pid, krc_obj:to_riakc_obj(Obj), Options, Timeout)
  of
    ok               -> ok;
    {error, _} = Err -> Err
  end.

delete(Pid, Bucket, Key, Options, Timeout) ->
  case
    riakc_pb_socket:delete(
      Pid, krc_obj:encode(Bucket), krc_obj:encode(Key), Options, Timeout)
  of
    ok               -> ok;
    {error, _} = Err -> Err
  end.

get(Pid, Bucket, Key, Options, Timeout) ->
  case
    riakc_pb_socket:get(
      Pid, krc_obj:encode(Bucket), krc_obj:encode(Key), Options, Timeout)
  of
    {ok, Obj}        -> {ok, krc_obj:from_riakc_obj(Obj)};
    {error, _} = Err -> Err
  end.

get_bucket(Pid, Bucket, Timeout) ->
  case
    riakc_pb_socket:get_bucket(Pid,
                   krc_obj:encode(Bucket),
                   Timeout) of
    {ok, Props}      -> {ok, krc_bucket_properties:decode(Props)};
    {error, _} = Err -> Err
  end.

% Similar to https://github.com/kivra/riak-erlang-client/blob/master/src/riakc_utils.erl#L29
wait_for_list(ReqId) ->
  wait_for_list(ReqId, []).
wait_for_list(ReqId, Acc) ->
  receive
    {ReqId, {done, undefined}}   -> {ok, lists:flatten(Acc)};
    {ReqId, {error, Reason}}     -> {error, Reason};
    {ReqId, Res} -> wait_for_list(ReqId, [Res?INDEX_STREAM_RESULT.keys | Acc])
  end.

handle_stream(ReqId) ->
  case wait_for_list(ReqId) of
    {ok, Res}        ->  {ok, [krc_obj:decode(K) || K <- Res]};
    {error, _} = Err -> Err
  end.

get_index(Pid, Bucket, Index, Key, Timeout) ->
  {Idx, IdxKey} = krc_obj:encode_index({Index, Key}),
  case
    riakc_pb_socket:get_index_eq(Pid,
                                 krc_obj:encode(Bucket),
                                 Idx,
                                 IdxKey,
                                 [{timeout, Timeout}, {stream, true}])
  of
    {ok, ReqId}      -> handle_stream(ReqId);
    {error, _} = Err -> Err
  end.

get_index(Pid, Bucket, Index, Lower, Upper, Timeout) ->
  {Idx, LowerKey} = krc_obj:encode_index({Index, Lower}),
  {Idx, UpperKey} = krc_obj:encode_index({Index, Upper}),
  case
    riakc_pb_socket:get_index_range(Pid,
                                    krc_obj:encode(Bucket),
                                    Idx,
                                    LowerKey,
                                    UpperKey,
                                    [{timeout, Timeout}, {stream, true}])
  of
    {ok, ReqId}        ->  handle_stream(ReqId);
    {error, _} = Err -> Err
  end.

get_map(Pid, Bucket, Key, Options, _Timeout) ->
  case
    riakc_pb_socket:fetch_type(Pid,
                               {krc_map:bucket_type(), krc_obj:encode(Bucket)},
                               krc_obj:encode(Key),
                               Options)
  of
    {ok, Map}        -> {ok, krc_obj:from_map(Bucket, Key, Map)};
    {error, _} = Err -> Err
  end.


list_keys(Pid, Bucket, Timeout) ->
    riakc_pb_socket:list_keys(Pid, krc_obj:encode(Bucket), Timeout).

putwo(Pid, Obj, Options, Timeout) -> put(Pid, Obj, Options, Timeout).
put(Pid, Obj, Options, Timeout)   ->
  ObjType = krc_obj:type(Obj),
  put(Pid, Obj, Options, Timeout, ObjType).

put(Pid, Obj, Options, Timeout, object) ->
  case
    riakc_pb_socket:put(Pid, krc_obj:to_riakc_obj(Obj), Options, Timeout)
  of
    ok               -> ok;
    {ok, Res}        -> {ok, krc_obj:from_riakc_obj(Res)};
    {error, _} = Err -> Err
  end;
put(Pid, Obj, Options, _Timeout, map) ->
  Bucket = krc_obj:bucket(Obj),
  Map = krc_obj:val(Obj),
  Key = krc_obj:key(Obj),
  riakc_pb_socket:update_type(
    Pid,
    {krc_map:bucket_type(), krc_obj:encode(Bucket)},
    Key,
    riakc_map:to_op(Map),
    Options
  ).

set_bucket(Pid, Bucket, Props, Timeout) ->
  case
    riakc_pb_socket:set_bucket(Pid,
                   krc_obj:encode(Bucket),
                   Props,
                   Timeout) of
    ok               -> ok;
    {error, _} = Err -> Err
  end.

start_link(IP, Port, Options) ->
  {ok, Pid} = riakc_pb_socket:start_link(IP, Port, Options),
  pong      = riakc_pb_socket:ping(Pid), %ensure server actually reachable
  {ok, Pid}.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
