                                  The
           _           _       _           _ _            _
          | | __  _ __(_) __ _| | __   ___| (_) ___ _ __ | |_
          | |/ / | '__| |/ _` | |/ /  / __| | |/ _ \ '_ \| __|
          |   <  | |  | | (_| |   <  | (__| | |  __/ | | | |_
          |_|\_\ |_|  |_|\__,_|_|\_\  \___|_|_|\___|_| |_|\__|

## Overview
A simple wrapper around the official Riak clients for Erlang.

Costs an extra copy, yields nicer API.

## Installation
jakob@moody.primat.es:~/git/erlang/krc$ gmake

jakob@moody.primat.es:~/git/erlang/krc$ gmake test

## Manifest
* krc.erl             -- API
* krc.hrl             -- internal header
* krc_app.erl         -- application
* krc_mock_client.erl -- mock backend client
* krc_obj.erl         -- like riak_obj
* krc_pb_client.erl   -- protobuffs backend client
* krc_riak_client.erl -- backend client interface
* krc_server.erl      -- worker pool
* krc_sup.erl         -- supervisor
* krc_test.erl        -- test support

## Telemetry Events


The application emits the following `telemetry` events:

- `[krc, get, conflict]` emitted when a conflict has been encountered during read.
  - Measurement: `#{monotonic_time => integer(), system_time => integer()}`
  - Metadata: `#{result => ok | error, error => term(), bucket => B, key => K}`

- `[krc, 'operation', retry]` (`operation` can be `get` or `put`) emitted when a get retry is made
  - Measurement: `#{monotonic_time => integer(), system_time => integer()}`
  - Metadata: `#{retry => positive_integer(), retry_limit => positive_integer(), bucket => binary(), key => binary(), error => term()}`

- `[krc_server, process, error]` emitted when a process terminates
  - Measurement: `#{monotonic_time => integer(), system_time => integer()}`
  - Metadata: `#{pid => pid(), reason => term(), failure_count => non_neg_integer()}`

- `[krc_server, request, stop]` emitted when a request terminates
  - Measurement: `#{monotonic_time => integer(), system_time => integer()}`
  - Metadata: `#{pid => pid(), client => atom(), daddy => pid(), result => ok | error, error => term()}`

- `[krc_pb_client, request, stop]` emitted at the end of a request to riak
  - Measurement: `#{duration => integer(), monotonic_time => integer()}`
  - Metadata: `#{request => map(), response => map()}`
