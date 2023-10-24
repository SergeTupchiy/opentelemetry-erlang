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
%% @doc
%% @end
%%%-------------------------------------------------------------------------
-module(otel_aggregation_last_value).

-behaviour(otel_aggregation).

-export([init/2,
         aggregate/4,
         checkpoint/3,
         collect/3]).

-include_lib("opentelemetry_api_experimental/include/otel_metrics.hrl").
-include("otel_metrics.hrl").

-type t() :: #last_value_aggregation{}.

-export_type([t/0]).

-include_lib("opentelemetry_api/include/gradualizer.hrl").
-include("otel_view.hrl").

init(#view_aggregation{name=Name,
                       reader=ReaderId,
                       aggregation_options=_Options}, Attributes) ->
    Key = {Name, Attributes, ReaderId},
    #last_value_aggregation{key=Key,
                            start_time=opentelemetry:timestamp(),
                            value=undefined}.

aggregate(Tab, ViewAggregation=#view_aggregation{name=Name,
                                                 reader=ReaderId}, Value, Attributes) ->
    Key = {Name, Attributes, ReaderId},
    case ets:update_element(Tab, Key, {#last_value_aggregation.value, Value}) of
        true ->
            true;
        false ->
            Metric = init(ViewAggregation, Attributes),
            ets:insert(Tab, ?assert_type((?assert_type(Metric, #last_value_aggregation{}))#last_value_aggregation{value=Value}, tuple()))
    end.

-dialyzer({nowarn_function, checkpoint/3}).
checkpoint(Tab, #view_aggregation{name=Name,
                                  reader=ReaderId,
                                  temporality=?TEMPORALITY_DELTA}, CollectionStartTime) ->
    MS = [{#last_value_aggregation{key='$1',
                                   start_time='$3',
                                   last_start_time='_',
                                   checkpoint='_',
                                   value='$2'},
           [{'=:=', {element, 1, '$1'}, {const, Name}},
            {'=:=', {element, 3, '$1'}, {const, ReaderId}}],
           [{#last_value_aggregation{key='$1',
                                     start_time={const, CollectionStartTime},
                                     last_start_time='$3',
                                     checkpoint='$2',
                                     value='$2'}}]}],
    _ = ets:select_replace(Tab, MS),

    ok;
checkpoint(Tab, #view_aggregation{name=Name,
                                  reader=ReaderId}, _CollectionStartTime) ->
    MS = [{#last_value_aggregation{key='$1',
                                   start_time='$3',
                                   last_start_time='_',
                                   checkpoint='_',
                                   value='$2'},
           [{'=:=', {element, 1, '$1'}, {const, Name}},
            {'=:=', {element, 3, '$1'}, {const, ReaderId}}],
           [{#last_value_aggregation{key='$1',
                                     start_time='$3',
                                     last_start_time='$3',
                                     checkpoint='$2',
                                     value='$2'}}]}],
    _ = ets:select_replace(Tab, MS),

    ok.


collect(Tab, #view_aggregation{name=Name,
                               reader=ReaderPid}, CollectionStartTime) ->
    Select = [{'$1',
               [{'=:=', Name, {element, 1, {element, 2, '$1'}}},
                {'=:=', ReaderPid, {element, 3, {element, 2, '$1'}}}],
               ['$1']}],
    AttributesAggregation = ets:select(Tab, Select),
    #gauge{datapoints=[datapoint(CollectionStartTime, LastValueAgg) ||
                          LastValueAgg <- AttributesAggregation]}.

%%

datapoint(CollectionStartTime, #last_value_aggregation{key={_, Attributes, _},
                                                       last_start_time=StartTime,
                                                       checkpoint=Checkpoint}) ->
    #datapoint{attributes=Attributes,
               %% `start_time' being set to `last_start_time' causes complaints
               %% because `last_start_time' has matchspec values in its typespec
               %% eqwalizer:ignore see above
               start_time=StartTime,
               time=CollectionStartTime,
               %% eqwalizer:ignore more matchspec fun
               value=Checkpoint,
               exemplars=[],
               flags=0}.
