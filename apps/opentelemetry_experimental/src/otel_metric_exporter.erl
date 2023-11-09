%%%------------------------------------------------------------------------
%% Copyright 2022, OpenTelemetry Authors
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
%% @doc MetricExporter defines the interface that protocol-specific
%% exporters MUST implement so that they can be plugged into OpenTelemetry
%% SDK and support sending of telemetry data.
%% @end
%%%-------------------------------------------------------------------------

-module(otel_metric_exporter).

-export([init/3,
         export/2,
         force_flush/0,
         shutdown/0]).

init(_OtelSignal, _ExporterId, _) ->
    {ok, []}.

export(_Batch, _Config) ->
    ok.

force_flush() ->
    ok.

shutdown() ->
    ok.
