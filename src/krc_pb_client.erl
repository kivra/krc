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
-export([
    delete/4,
    delete/5,
    get/5,
    get_bucket/3,
    get_index/5,
    get_index/6,
    list_keys/3,
    ping/2,
    put/4,
    putwo/4,
    set_bucket/4,
    start_link/3
]).

%%%_* Includes =========================================================
-include_lib("krc/include/krc.hrl").
-include_lib("stdlib2/include/prelude.hrl").
-include_lib("riakc/include/riakc.hrl").

%%%_* Code =============================================================
start_link(IP, Port, Options) ->
    {ok, Pid} = riakc_pb_socket:start_link(IP, Port, Options),
    %ensure server actually reachable
    pong = riakc_pb_socket:ping(Pid),
    {ok, Pid}.

delete(Pid, Obj, Options, Timeout) ->
    obj_exec(Pid, delete_obj, Obj, Options, Timeout).

delete(Pid, Bucket, Key, Options, Timeout) ->
    obj_exec(Pid, delete, Bucket, Key, Options, Timeout).

get(Pid, Bucket, Key, Options, Timeout) ->
    obj_exec(Pid, get, Bucket, Key, Options, Timeout).

ping(Pid, Timeout) ->
    try
        pong = riakc_pb_socket:ping(Pid, Timeout),
        ok
    catch
        _:Reason -> {error, Reason}
    end.

putwo(Pid, Obj, Options, Timeout) -> put(Pid, Obj, Options, Timeout).
put(Pid, Obj, Options, Timeout) ->
    obj_exec(Pid, put, Obj, Options, Timeout).

get_index(Pid, Bucket, Index, Key, Timeout) ->
    {Idx, IdxKey} = krc_obj:encode_index({Index, Key}),
    ExtraMetadata = #{index => Index, idx => Idx, idxkey => IdxKey},
    StartMetadata = metadata(Pid, get_index_eq, Bucket, Key, [], Timeout, ExtraMetadata),
    telemetry_span(
        StartMetadata,
        fun() ->
            Resp = riakc_pb_socket:get_index_eq(
                Pid,
                krc_obj:encode(Bucket),
                Idx,
                IdxKey,
                [{timeout, Timeout}, {stream, true}]
            ),
            handle_resp(Resp, StartMetadata, fun handle_stream/1)
        end
    ).

get_index(Pid, Bucket, Index, Lower, Upper, Timeout) ->
    {Idx, LowerKey} = krc_obj:encode_index({Index, Lower}),
    {Idx, UpperKey} = krc_obj:encode_index({Index, Upper}),
    ExtraMetadata = #{index => Index, idx => Idx, lower_key => LowerKey, upper_key => UpperKey},
    StartMetadata = metadata(Pid, get_index_range, Bucket, undefined, [], Timeout, ExtraMetadata),
    telemetry_span(
        StartMetadata,
        fun() ->
            Resp =
                riakc_pb_socket:get_index_range(
                    Pid,
                    krc_obj:encode(Bucket),
                    Idx,
                    LowerKey,
                    UpperKey,
                    [{timeout, Timeout}, {stream, true}]
                ),
            handle_resp(Resp, StartMetadata, fun handle_stream/1)
        end
    ).

list_keys(Pid, Bucket, Timeout) ->
    StartMetadata = metadata(Pid, list_keys, Bucket, undefined, [], Timeout),
    telemetry_span(
        StartMetadata,
        fun() ->
            Resp = riakc_pb_socket:list_keys(Pid, krc_obj:encode(Bucket), Timeout),
            handle_resp(Resp, StartMetadata, fun(Keys) -> {ok, Keys} end)
        end
    ).

get_bucket(Pid, Bucket, Timeout) ->
    StartMetadata = metadata(Pid, get_bucket, Bucket, undefined, [], Timeout),
    telemetry_span(
        StartMetadata,
        fun() ->
            Resp = riakc_pb_socket:get_bucket(Pid, krc_obj:encode(Bucket), Timeout),
            handle_resp(
                Resp,
                StartMetadata,
                fun(Props) -> {ok, krc_bucket_properties:decode(Props)} end
            )
        end
    ).

set_bucket(Pid, Bucket, Props, Timeout) ->
    ExtraMetadata = #{props => Props},
    StartMetadata = metadata(Pid, set_bucket, Bucket, undefined, [], Timeout, ExtraMetadata),
    telemetry_span(
        StartMetadata,
        fun() ->
            Resp = riakc_pb_socket:set_bucket(Pid, krc_obj:encode(Bucket), Props, Timeout),
            handle_resp(Resp, StartMetadata)
        end
    ).

%%%_* Private ==========================================================
handle_stream(ReqId) ->
    case wait_for_list(ReqId) of
        {ok, Res} -> {ok, [krc_obj:decode(K) || K <- Res]};
        {error, _} = Err -> Err
    end.

% Similar to https://github.com/kivra/riak-erlang-client/blob/master/src/riakc_utils.erl#L29
wait_for_list(ReqId) ->
    wait_for_list(ReqId, []).
wait_for_list(ReqId, Acc) ->
    receive
        {ReqId, {done, undefined}} -> {ok, lists:flatten(Acc)};
        {ReqId, {error, Reason}} -> {error, Reason};
        {ReqId, Res} -> wait_for_list(ReqId, [Res?INDEX_STREAM_RESULT.keys | Acc])
    end.

obj_exec(Pid, Op, Obj, Options, Timeout) ->
    StartMetadata = metadata(Pid, Op, Obj, Options, Timeout),
    telemetry_span(
        StartMetadata,
        fun() ->
            Resp =
                riakc_pb_socket:Op(Pid, krc_obj:to_riakc_obj(Obj), Options, Timeout),
            handle_resp(Resp, StartMetadata)
        end
    ).

obj_exec(Pid, Op, Bucket, Key, Options, Timeout) ->
    StartMetadata = metadata(Pid, Op, Bucket, Key, Options, Timeout),
    telemetry_span(
        StartMetadata,
        fun() ->
            Resp =
                riakc_pb_socket:Op(
                    Pid,
                    krc_obj:encode(Bucket),
                    krc_obj:encode(Key),
                    Options,
                    Timeout
                ),
            handle_resp(Resp, StartMetadata)
        end
    ).

handle_resp(Resp, StartMetadata) ->
    handle_resp(Resp, StartMetadata, fun(Obj) -> {ok, krc_obj:from_riakc_obj(Obj)} end).

handle_resp(ok, StartMetadata, _DecodeRespFun) ->
    RespMetadata = #{response => #{result => ok}},
    {ok, maps:merge(StartMetadata, RespMetadata)};
handle_resp({ok, RespObj}, StartMetadata, DecodeRespFun) ->
    Resp = DecodeRespFun(RespObj),
    RespMetadata =
        case Resp of
            {ok, Data} -> #{response => #{result => ok, size => size_in_bytes(Data)}};
            {error, Reason} -> #{response => #{result => error, error => Reason}}
        end,
    {Resp, maps:merge(StartMetadata, RespMetadata)};
handle_resp({error, Reason}, StartMetadata, _DecodeRespFun) ->
    RespMetadata = #{response => #{result => error, error => Reason}},
    {{error, Reason}, maps:merge(StartMetadata, RespMetadata)}.

telemetry_span(StartMetadata, Fun) ->
    telemetry:span([?MODULE, request], StartMetadata, Fun).

metadata(Pid, Op, Obj, Options, Timeout) ->
    ExtraMetadata = #{size => size_in_bytes(Obj)},
    metadata(Pid, Op, krc_obj:bucket(Obj), krc_obj:key(Obj), Options, Timeout, ExtraMetadata).

metadata(Pid, Op, Bucket, Key, Options, Timeout) ->
    metadata(Pid, Op, Bucket, Key, Options, Timeout, #{}).

metadata(Pid, Op, Bucket, Key, Options, Timeout, ExtraMetadata) ->
    BaseMetadata =
        #{
            pid => Pid,
            method => Op,
            bucket => Bucket,
            key => Key,
            opts => Options,
            timeout => Timeout
        },

    #{request => maps:merge(BaseMetadata, ExtraMetadata)}.

size_in_bytes(Data) ->
    case krc_obj:is_obj(Data) of
        true -> krc_obj_size(Data);
        false -> term_size(Data)
    end.

krc_obj_size(Obj) ->
    case krc_obj:val(Obj) of
        [?TOMBSTONE] -> 0;
        Value -> term_size(Value)
    end.

term_size(Data) when is_binary(Data) ->
    erlang:iolist_size(Data);
term_size(Data) ->
    %% From the docs: Calculates, without doing the encoding, the maximum
    %% byte size for a term encoded in the Erlang external term format
    %% TODO: Is there a better way to do this?
    erlang:external_size(Data).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
