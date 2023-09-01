%%%------------------------------------------------------------------------
%% Copyright 2020, OpenTelemetry Authors
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% @doc All measurements are associated with an instrument.
%% @end
%%%-------------------------------------------------------------------------
-module(otel_instrument).

-export([new/6,
         new/8,
         is_monotonic/1,
         temporality/1]).

-include("otel_metrics.hrl").

-type name() :: atom().
-type description() :: unicode:unicode_binary().
-type kind() :: ?KIND_COUNTER | ?KIND_OBSERVABLE_COUNTER | ?KIND_HISTOGRAM |
                ?KIND_OBSERVABLE_GAUGE | ?KIND_UPDOWN_COUNTER | ?KIND_OBSERVABLE_UPDOWNCOUNTER.
-type unit() :: atom(). %% latin1, maximum length of 63 characters
-type observation() :: {number(), opentelemetry:attributes_map()}.
-type named_observations() :: {name(), [observation()]}.
-type callback_args() :: term().
-type callback_result() :: [observation()] |
                           [named_observations()].
-type callback() :: fun((callback_args()) -> callback_result()).

-type temporality() :: ?TEMPORALITY_DELTA | ?TEMPORALITY_CUMULATIVE.

-type t() :: #instrument{}.

-export_type([t/0,
              name/0,
              description/0,
              kind/0,
              unit/0,
              temporality/0,
              callback/0,
              callback_args/0,
              callback_result/0]).

-spec new(module(), otel_meter:t(), kind(), name(), description() | undefined, unit() | undefined) -> t().
new(Module, Meter, Kind, Name, Description, Unit) ->
    #instrument{module      = Module,
                meter       = Meter,
                name        = Name,
                description = Description,
                temporality = ?TEMPORALITY_DELTA,
                kind        = Kind,
                unit        = Unit}.

-spec new(module(), otel_meter:t(), kind(), name(), description() | undefined, unit() | undefined, callback(), callback_args()) -> t().
new(Module, Meter, Kind, Name, Description, Unit, Callback, CallbackArgs) ->
    #instrument{module        = Module,
                meter         = Meter,
                name          = Name,
                description   = Description,
                kind          = Kind,
                unit          = Unit,
                temporality   = ?TEMPORALITY_CUMULATIVE,
                callback      = Callback,
                callback_args = CallbackArgs}.

is_monotonic(#instrument{kind=?KIND_COUNTER}) ->
    true;
is_monotonic(#instrument{kind=?KIND_OBSERVABLE_COUNTER}) ->
    true;
is_monotonic(#instrument{kind=?KIND_HISTOGRAM}) ->
    true;
is_monotonic(_) ->
    false.

temporality(#instrument{kind=?KIND_COUNTER}) ->
    ?TEMPORALITY_DELTA;
temporality(#instrument{kind=?KIND_OBSERVABLE_COUNTER}) ->
    ?TEMPORALITY_CUMULATIVE;
temporality(#instrument{kind=?KIND_UPDOWN_COUNTER}) ->
    ?TEMPORALITY_DELTA;
temporality(#instrument{kind=?KIND_OBSERVABLE_UPDOWNCOUNTER}) ->
    ?TEMPORALITY_CUMULATIVE;
temporality(#instrument{kind=?KIND_HISTOGRAM}) ->
    ?TEMPORALITY_DELTA;
temporality(#instrument{kind=?KIND_OBSERVABLE_GAUGE}) ->
    ?TEMPORALITY_CUMULATIVE.