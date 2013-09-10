%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Conversion from/to riak-erlang-client objects.
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
-module(krc_obj).

%%%_* Exports ==========================================================
%% Operations
-export([ resolve/2
        ]).

%% Representation
-export([ decode/1
        , decode_index/1
        , encode/1
        , encode_index/1
        , from_riakc_obj/1
        , to_riakc_obj/1
        ]).

%% ADT
-export([ new/3
        , bucket/1
        , key/1
        , val/1
        , indices/1
        , vclock/1
        , siblings/1
        , set_bucket/2
        , set_val/2
        , set_vclock/2
        , set_indices/2
        ]).

-export_type([ bucket/0
             , ect/0
             , idx/0
             , idx_key/0
             , indices/0
             , key/0
             ]).

%%%_* Includes =========================================================
-include("krc.hrl").
-include_lib("krc/include/krc.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include_lib("stdlib2/include/prelude.hrl").

%%%_* Code =============================================================
%%%_ * ADT -------------------------------------------------------------
-type bucket()               :: _.
-type key()                  :: _.
-type val()                  :: _.
-type idx()                  :: _.
-type idx_key()              :: _.
-type indices()              :: [{idx(), idx_key()}].

%% #krc_obj.vclock is set to undefined; this is compatible with
%% riakc_obj.erl (by inspection in late 2011).
-record(krc_obj,
        { bucket             :: bucket()  %\
        , key                :: key()     % } Application specific
        , val                :: val()     %/
        , indices=[]         :: indices() %KRC specific
        , vclock=undefined   :: _         %Riak internal
        }).

-opaque ect()                :: #krc_obj{}.

-spec new(_, _, _)           -> ect().
new(B, K, V)                 -> #krc_obj{bucket=B, key=K, val=V}.

bucket(#krc_obj{bucket=B})   -> B.
key(#krc_obj{key=K})         -> K.
val(#krc_obj{val=V})         -> V.
indices(#krc_obj{indices=I}) -> I.
vclock(#krc_obj{vclock=C})   -> C.
siblings(Obj)                -> length(val(Obj)) =/= 1.

set_bucket(Obj, B)           -> Obj#krc_obj{bucket=B}.
set_val(Obj, V)              -> Obj#krc_obj{val=V}.
set_vclock(Obj, C)           -> Obj#krc_obj{vclock=C}.
set_indices(Obj, I)          -> ?hence(is_indices(I)), Obj#krc_obj{indices=I}.

is_indices(I)                -> lists:all(fun is_idx/1, I).
is_idx({_, _})               -> true;
is_idx(_)                    -> false.

%%%_ * KRC<->riakc -----------------------------------------------------
%% Since the Riak PB client doesn't have an API for adding indices, we
%% add index entries to the metadata dictionary manually.
%% To verify that this works:
%%
%% {ok, Pid}   = riakc_pb_socket:start_link("127.0.0.1", 8081),
%% Bucket      = <<"bucket">>,
%% Key         = <<"key">>,
%% Val         = <<"val">>,
%% Idx         = <<"idx_bin">>,
%% IdxKey      = <<"key2">>,
%% Obj0        = riakc_obj:new(Bucket, Key, Val),
%% MetaData    = dict:store(?MD_INDEX, [{Idx, IdxKey}], dict:new()),
%% Obj         = riakc_obj:update_metadata(Obj0, MetaData),
%% ok          = riakc_pb_socket:put(Pid, Obj),
%% {ok, [Key]} = riakc_pb_socket:get_index(Pid, Bucket, Idx, IdxKey).
%%
-type riakc_obj() :: _. %#riakc_obj{}

-spec to_riakc_obj(ect()) -> riakc_obj().
to_riakc_obj(#krc_obj{bucket=B, key=K, indices=I, val=V, vclock=C}) ->
  riakc_obj:new_obj(encode(B), encode(K), C, [{encode_indices(I), encode(V)}]).


-spec from_riakc_obj(riakc_obj()) -> ect() | no_return().
%% Siblings need to be resolved separately.
from_riakc_obj(Obj) ->
  {riakc_obj, _, _, _, _, undefined, undefined} = Obj, %assert
  Contents = riakc_obj:get_contents(Obj),
  #krc_obj{ bucket  = decode(riakc_obj:bucket(Obj))
          , key     = decode(riakc_obj:key(Obj))
          , val     = [decode(V) || {_, V} <- Contents]
          , indices = [decode_indices(MD) || {MD, _} <- Contents]
          , vclock  = riakc_obj:vclock(Obj) %opaque
          }.


%% We're only interested in index-metadata.
-spec encode_indices(indices()) -> dict:dict().
encode_indices([_|_] = I) ->
  dict:store(
    ?MD_INDEX,
    lists:map(fun encode_index/1, I),
    dict:new());
encode_indices([]) -> dict:new().

-spec decode_indices(dict:dict()) -> indices().
decode_indices(MD) ->
  case dict:find(?MD_INDEX, MD) of
    {ok, I} -> lists:map(fun decode_index/1, I);
    error   -> []
  end.


%% @doc Resolve conflicts by taking the union of all indices and
%% computing the LUB of all values under F.
resolve(#krc_obj{val=Vs, indices=Is} = Obj, F) ->
  ?lift(Obj#krc_obj{ val     = ?unlift(s2_maybe:reduce(F, Vs))
                   , indices = lists:usort(lists:flatten(Is))
                   }).

%%%_ * Representation --------------------------------------------------
%% On-disk (we allow arbitrary terms, Riak stores binaries; these are
%% used for bucket names, keys, and values).
-spec encode(binary)           -> binary().
encode(X)                      -> X.

-spec decode(binary())         -> _.
decode(<<>>)                   -> ?TOMBSTONE;
decode(X)                      -> X.

%% Index names (Riak uses strings with a type suffix; we determine the
%% index type based on the type of the index key, and allow any Erlang
%% term as the name).
-spec encode_index({_, _})     -> {binary(), binary()}.
encode_index({'$bucket',
              '$bucket'})      -> {<<"$bucket">>, <<"$bucket">>};
encode_index({Idx, Key})
  when is_integer(Key)         -> {encode_idx(Idx,<<"int">>), Key};
encode_index({Idx, Key})       -> {encode_idx(Idx,<<"bin">>), Key}.

-spec decode_index({binary(), binary()})
                               -> {_, _}.
decode_index({Idx, _Key})       -> decode_idx(Idx).

-spec encode_idx(_, string())  -> binary().
encode_idx(Name, Suffix)       -> add_suffix(Name, Suffix).

-spec decode_idx(binary())     -> _.
decode_idx(Name)               -> drop_suffix(Name).

add_suffix(Str, Suffix)        -> <<Str/binary, $_, Suffix/binary>>.
drop_suffix(Str)               -> { binary:part(Str, {byte_size(Str), -3})
                                  , <<Str:(byte_size(Str)-4)/binary>> }.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Presumably, this is how objects come out of Riak...
new_riakc_obj(Bucket, Key, Val) ->
  riakc_obj:new_obj(Bucket, Key, undefined, [{dict:new(), Val}]).

resolver() -> fun(_, _) -> {error, conflict} end.

basic_test() ->
  KrcObj1       = new(foo, bar, baz),
  RiakcObj1     = to_riakc_obj(KrcObj1),
  {ok, KrcObj1} = resolve(from_riakc_obj(RiakcObj1), resolver()),
  ?assertEqual(encode(foo), riakc_obj:bucket(RiakcObj1)),
  ?assertEqual(encode(bar), riakc_obj:key(RiakcObj1)),
  ?assertEqual([{dict:new(), encode(baz)}], riakc_obj:get_contents(RiakcObj1)),

  RiakcObj2     = new_riakc_obj(encode(foo), encode(bar), encode(baz)),
  {ok, KrcObj2} = resolve(from_riakc_obj(RiakcObj2), resolver()),
  RiakcObj2     = to_riakc_obj(KrcObj2),
  foo           = bucket(KrcObj2),
  bar           = key(KrcObj2),
  baz           = val(KrcObj2).

indices_test() ->
  KrcObj0      = new(foo, bar, baz),
  I            = [{i1, k1}, {i2, k2}],
  KrcObj       = set_indices(KrcObj0, I),

  RiakcObj     = to_riakc_obj(KrcObj),
  Meta         = riakc_obj:get_metadata(RiakcObj),
  I            = decode_indices(Meta),

  {ok, KrcObj} = resolve(from_riakc_obj(RiakcObj), resolver()),
  I            = indices(KrcObj).

coverage_test() ->
  Obj        = new(foo, bar, baz),
  _          = set_bucket(Obj, blarg),
  undefined  = vclock(Obj),
  {error, _} = ?lift(set_indices(Obj, [foo, bar])),
  42         = val(set_val(Obj, 42)),
  ?TOMBSTONE = decode(<<>>),
  ok.

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
