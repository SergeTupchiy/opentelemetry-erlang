%%%------------------------------------------------------------------------
%% Copyright 2024, OpenTelemetry Authors
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
%% @doc
%% @end
%%%-------------------------------------------------------------------------
-module(otel_metric_exemplar).

-export([new/5]).

-include("otel_metric_exemplar.hrl").

-type exemplar() :: #exemplar{}.

-export_type([exemplar/0]).

-spec new(number(), opentelemetry:timestamp(), opentelemetry:attributes_map(), opentelemetry:trace_id() | undefined, opentelemetry:span_id() | undefined) -> exemplar().
new(Value, Time, FilteredAttributes, TraceId, SpanId) ->
    #exemplar{value=Value,
              time=Time,
              filtered_attributes=FilteredAttributes,
              span_id=SpanId,
              trace_id=TraceId}.
