%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Prometheus metric definitions for KRC
%%%
%%% This module requires that the top level application already set ups
%%% prometheus metrics using `prometheus` library
%%%
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(krc_prometheus).

%%%_* Exports ==========================================================
-export([declare_metrics/0]).
-export([conflict_count/2]).
-export([process_error_count/0]).
-export([retry_count/2]).
-export([request_count/2]).
-export([request_duration/4]).
-export([request_size/4]).
-export([response_size/4]).

%%%_* Code =============================================================
-spec declare_metrics() -> ok.
declare_metrics() ->
  %% The names are chosen according to these guides:
  %% https://github.com/deadtrickster/prometheus.erl/blob/master/doc/prometheus_time.md#description
  %% https://prometheus.io/docs/practices/naming/#metric-names
  prometheus_counter:declare([
    {name, krc_conflict_total},
    {labels, [result, bucket]},
    {help, "Client conflict count"}
  ]),
  prometheus_counter:declare([
    {name, krc_retries_total},
    {labels, [operation, bucket]},
    {help, "Client retry count per request"}
  ]),
  prometheus_counter:declare([
    {name, krc_request_total},
    {labels, [result, error]},
    {help, "Client request count"}
  ]),
  prometheus_counter:declare([
    {name, krc_process_error_total},
    {labels, []},
    {help, "Client process error count"}
  ]),
  prometheus_histogram:declare([
    {name, krc_request_duration_seconds},
    {labels, [result, operation, bucket]},
    %% Beware bucket resolution, but let's just use the defaults until we make an informed decision:
    %% https://medium.com/mercari-engineering/have-you-been-using-histogram-metrics-correctly-730c9547a7a9
    %% The default buckets are floats representing seconds.
    {buckets, default},
    {help, "Client request duration"}
  ]),
  prometheus_histogram:declare([
    {name, krc_request_size_bytes},
    {labels, [result, operation, bucket]},
    %% Beware bucket resolution, but let's just use the defaults until we make an informed decision:
    %% https://medium.com/mercari-engineering/have-you-been-using-histogram-metrics-correctly-730c9547a7a9
    %% The default buckets are floats representing seconds.
    {buckets, default},
    {help, "Riak client request size"}
  ]),
  prometheus_histogram:declare([
    {name, krc_response_size_bytes},
    {labels, [result, operation, bucket]},
    %% Beware bucket resolution, but let's just use the defaults until we make an informed decision:
    %% https://medium.com/mercari-engineering/have-you-been-using-histogram-metrics-correctly-730c9547a7a9
    %% The default buckets are floats representing seconds.
    {buckets, default},
    {help, "Riak client response size"}
  ]),
  ok.

-spec conflict_count(atom(), binary()) -> any().
conflict_count(Result, Bucket) ->
    prometheus_counter:inc(krc_conflict_total, [Result, Bucket], 1).

-spec retry_count(atom(), binary()) -> any().
retry_count(Op, Bucket) ->
    prometheus_counter:inc(krc_retries_total, [Op, Bucket], 1).

-spec request_count(atom(), atom()) -> any().
request_count(Result, Error) ->
    prometheus_counter:inc(krc_request_total, [Result, Error], 1).

-spec process_error_count() -> any().
process_error_count() ->
    prometheus_counter:inc(krc_process_error_total, [], 1).

-spec request_duration(pos_integer(), atom(), binary(), binary()) -> any().
request_duration(DurationNative, Result, Op, Bucket) ->
    prometheus_histogram:observe(
      krc_request_duration_seconds,
      [Result, Op, Bucket], DurationNative
     ).

-spec request_size(pos_integer(), atom(), binary(), binary()) -> any().
request_size(SizeInBytes, Result, Op, Bucket) ->
    prometheus_histogram:observe(
      krc_request_size_bytes,
      [Result, Op, Bucket], SizeInBytes
     ).

-spec response_size(pos_integer(), atom(), binary(), binary()) -> any().
response_size(SizeInBytes, Result, Op, Bucket) ->
    prometheus_histogram:observe(
      krc_response_size_bytes,
      [Result, Op, Bucket], SizeInBytes
     ).

%%%_* Private ==========================================================
