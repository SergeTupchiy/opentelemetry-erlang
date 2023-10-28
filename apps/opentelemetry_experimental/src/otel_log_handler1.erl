%%%------------------------------------------------------------------------
%% Copyright 2022-2023, OpenTelemetry Authors
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
%% @doc Specification: https://opentelemetry.io/docs/specs/otel/logs/sdk
%% @end
%%%-------------------------------------------------------------------------
-module(otel_log_handler1).

-behaviour(gen_statem).

-include_lib("kernel/include/logger.hrl").
-include_lib("opentelemetry_api/include/opentelemetry.hrl").

-export([start_link/1]).

-export([log/2,
         adding_handler/1,
         removing_handler/1,
         changing_config/3,
         report_cb/1]).

-export([init/1,
         callback_mode/0,
         idle/3,
         exporting/3,
         terminate/3]).

-type config() :: #{id => logger:handler_id(),
                    reg_name := atom(),
                    atomic_ref := reference(),
                    tables := {ets:table(), ets:table()},
                    config => otel_log_handler_config(),
                    level => logger:level() | all | none,
                    module => module(),
                    filter_default => log | stop,
                    filters => [{logger:filter_id(), logger:filter()}],
                    formatter => {module(), logger:formatter_config()}}.

-type otel_log_handler_config() ::
        #{max_queue_size => max_queue_size(),
          max_export_batch_size => max_export_batch_size(),
          exporting_timeout_ms => exporting_timeout_ms(),
          scheduled_delay_ms => scheduled_delay_ms(),
          check_table_size_ms => check_table_size_ms(),
          exporter => exporter_config()}.

-type max_queue_size() :: non_neg_integer() | infinity.
-type exporter_config() :: module() | {module(), Config :: term()} | undefined.
-type max_export_batch_size() :: non_neg_integer().
-type exporting_timeout_ms() :: non_neg_integer().
-type scheduled_delay_ms() :: non_neg_integer().
-type check_table_size_ms() :: non_neg_integer().

-define(SUP, opentelemetry_experimental_sup).

%% simplify max batch size (e.g. like setting it to 1?)

%% if grpc client is very fast we don't need to wait for a timeout
%% postpone timeouts accumulation (if grpc client is slow)?


-define(name_to_reg_name(Module, Id),
        list_to_atom(lists:concat([Module, "_", Id]))).
-define(table_name(_RegName_, _TabName_), list_to_atom(lists:concat([_RegName_, "_", _TabName_]))).
-define(table_1(_RegName_), ?table_name(_RegName_, table1)).
-define(table_2(_RegName_), ?table_name(_RegName_, table2)).

%% extra 1 is substrcated, because we add 1 to max_queue_size
-define(MAX_SIGNED_INT, (1 bsl 63)-1-1).
-define(TAB_1_IX, 1).
-define(TAB_2_IX, 2).
%% signifies which table is currently enabled (1 - table_1, 2 - table_2)
-define(CURRENT_TAB_IX, 3).

-define(current_tab(_AtomicRef_), atomics:get(_AtomicRef_, ?CURRENT_TAB_IX)).
-define(tab_name(_TabIx_, _Tabs_), element(_TabIx_, _Tabs_)).
-define(next_tab(_CurrentTab_), case _CurrentTab_ of
                                    ?TAB_1_IX -> ?TAB_2_IX;
                                    ?TAB_2_IX -> ?TAB_1_IX
                                end).

-define(set_current_tab(_AtomicRef_, _TabIx_), atomics:put(_AtomicRef_, ?CURRENT_TAB_IX, _TabIx_)).
-define(set_available(_AtomicRef_, _TabIx_, _Size_), atomics:put(_AtomicRef_, _TabIx_, _Size_+1)).
-define(get_available(_AtomicRef_, _TabIx_), atomics:get(_AtomicRef_, _TabIx_)).
-define(is_available(_AtomicRef_, _TabIx_), atomics:sub_get(_AtomicRef_, _TabIx_, 1) > 0).

-define(DEFAULT_MAX_QUEUE_SIZE, 2048).
-define(DEFAULT_MAX_EXPORT_BATCH_SIZE, 512).
-define(DEFAULT_SCHEDULED_DELAY_MS, timer:seconds(1)).
-define(DEFAULT_EXPORTER_TIMEOUT_MS, timer:seconds(30)).
-define(DEFAULT_CHECK_TABLE_SIZE_MS, 200).
-define(DEFAULT_EXPORTER,
        {opentelemetry_exporter, #{protocol => grpc, endpoints => ["http://172.18.0.2:4317"]}}).

-define(SUP_SHUTDOWN_MS, 5500).
%% Slightly lower than SUP_SHUTDOWN_MS
-define(GRACE_SHUTDOWN_MS, 5000).
-define(time_ms, erlang:monotonic_time(millisecond)).
-define(rem_time(_Timeout_, _T0_, _T1_), max(0, _Timeout_ - (_T1_ - _T0_))).

-define(check_tab_timeout(_TimeoutMs_), {{timeout, check_table_size}, _TimeoutMs_, check_table_size}).

-record(data, {exporter              :: {module(), State :: term()} | undefined,
               exporter_config       :: exporter_config(),
               resource              :: otel_resource:t(),
               handed_off_table      :: atom() | undefined,
               runner_pid            :: pid() | undefined,
               tables                :: {ets:table(), ets:table()},
               reg_name              :: atom(),
               config                :: config(),
               max_queue_size        = ?DEFAULT_MAX_QUEUE_SIZE        :: non_neg_integer(),
               max_export_batch_size = ?DEFAULT_MAX_EXPORT_BATCH_SIZE :: max_export_batch_size(),
               exporting_timeout_ms  = ?DEFAULT_EXPORTER_TIMEOUT_MS   :: exporting_timeout_ms(),
               scheduled_delay_ms    = ?DEFAULT_SCHEDULED_DELAY_MS    :: scheduled_delay_ms(),
               check_table_size_ms   = ?DEFAULT_CHECK_TABLE_SIZE_MS   :: check_table_size_ms(),
               atomic_ref            :: reference()
              }).

start_link(#{reg_name := RegName} = Config) ->
    gen_statem:start_link({local, RegName}, ?MODULE, Config, []).

%% TODO:
%% - implement force flush: https://opentelemetry.io/docs/specs/otel/logs/sdk/#forceflush
%% - implement max_batch_size fully

%%--------------------------------------------------------------------
%% Logger handler callbacks
%%--------------------------------------------------------------------

-spec adding_handler(Config) -> {ok, Config} | {error, Reason} when
      Config :: config(),
      Reason :: term().
adding_handler(#{id := Id}=Config) ->
    RegName = ?name_to_reg_name(?MODULE, Id),
    AtomicRef = atomics:new(3, [{signed, true}]),
    Config1 = Config#{reg_name => RegName,
                      tables => {?table_1(RegName), ?table_2(RegName)},
                      atomic_ref => AtomicRef},
    OtelConfig = maps:get(config, Config, #{}),
    case validate_config(OtelConfig) of
        ok ->
            OtelConfig1 = maps:merge(default_config(), OtelConfig),
            start(Id, Config1#{config => OtelConfig1});
        Err ->
            Err
    end.

-spec changing_config(SetOrUpdate, OldConfig, NewConfig) ->
          {ok, Config} | {error, Reason} when
      SetOrUpdate :: set | update,
      OldConfig :: config(),
      NewConfig :: config(),
      Config :: config(),
      Reason :: term().
changing_config(_, #{reg_name := RegName}, #{reg_name := RegName1}) when RegName =/= RegName1 ->
    {error, reg_name_change_not_allowed};
changing_config(_, #{config := #{exporter := Exporter}},
                #{config := #{exporter := Exporter1}}) when Exporter =/= Exporter1 ->
    {error, exporter_change_not_allowed};
changing_config(SetOrUpdate, #{reg_name := RegName, config := OldOtelConfig}, NewConfig) ->
    NewOtelConfig = maps:get(config, NewConfig, #{}),
    case validate_config(NewOtelConfig) of
        ok ->
            NewOtelConfig1 = case SetOrUpdate of
                                 update -> maps:merge(OldOtelConfig, NewOtelConfig);
                                 set -> maps:merge(default_config(), NewOtelConfig)
                             end,
            NewConfig1 = NewConfig#{config => NewOtelConfig1, reg_name => RegName},
            gen_statem:call(RegName, {changing_config, NewConfig1});
        Err ->
            Err
    end.

-spec removing_handler(Config) -> ok | {error, Reason} when
      Config :: config(), Reason :: term().
removing_handler(_Config=#{id := Id}) ->
    Res = supervisor:terminate_child(?SUP, Id),
    _ = supervisor:delete_child(?SUP, Id),
    Res.

-spec log(LogEvent, Config) -> true | dropped | {error, term()} when
      LogEvent :: logger:log_event(),
      Config :: config().
log(LogEvent, _Config=#{atomic_ref := AtomicRef, tables := Tabs}) ->
    Scope = case LogEvent of
                #{meta := #{otel_scope := Scope0=#instrumentation_scope{}}} ->
                    Scope0;
                #{meta := #{mfa := {Module, _, _}}} ->
                    opentelemetry:get_application_scope(Module);
                _ ->
                    opentelemetry:instrumentation_scope(<<>>, <<>>, <<>>)
            end,
    do_insert(AtomicRef, Tabs, Scope, LogEvent).

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

init(Config) ->
    #{config := OtelConfig,
      atomic_ref := AtomicRef,
      reg_name := RegName,
      tables := {Tab1, Tab2}} = Config,
    process_flag(trap_exit, true),
    Resource = otel_resource_detector:get_resource(),
    ExporterConfig = maps:get(exporter, OtelConfig, ?DEFAULT_EXPORTER),
    %% assert table names match
    Tab1 = ?table_1(RegName),
    Tab2 = ?table_2(RegName),
    _Tid1 = new_export_table(Tab1),
    _Tid2 = new_export_table(Tab2),

    Data = #data{atomic_ref=AtomicRef,
                 exporter=undefined,
                 exporter_config=ExporterConfig,
                 resource=Resource,
                 tables={Tab1, Tab2},
                 reg_name=RegName,
                 config = Config},
    Data1 = add_config_to_data(Config, Data),

    ?set_current_tab(AtomicRef, ?TAB_1_IX),

    %% TODO: it's enabled to start log events writes to ETS table,
    %% but soon the handler can fail to init exporter
    %% Disabling it if exporter config is none/undefined is not perfect either,
    %% since the exporter may default to localhost:<default port>.
    %% It may be better to init exporter synchronously in `init/1` cb
    %% and make choice to enable/disable

    ?set_available(AtomicRef, ?TAB_1_IX, Data1#data.max_queue_size),
    ?set_available(AtomicRef, ?TAB_2_IX, Data1#data.max_queue_size),

    {ok, idle, Data1, [?check_tab_timeout(Data1#data.check_table_size_ms)]}.

callback_mode() ->
    [state_functions, state_enter].

idle(enter, _OldState, Data=#data{exporter=undefined,
                                  exporter_config=ExporterConfig,
                                  scheduled_delay_ms=SendInterval,
                                  atomic_ref=AtomicRef,
                                  tables=Tabs}) ->
    Exporter = init_exporter(AtomicRef, Tabs, ExporterConfig),
    {keep_state, Data#data{exporter=Exporter},
     [{{timeout, export_logs}, SendInterval, export_logs}]};
idle(enter, _OldState, #data{scheduled_delay_ms=SendInterval}) ->
    {keep_state_and_data,
     [{{timeout, export_logs}, SendInterval, export_logs}]};
idle(_, export_logs, Data=#data{exporter=undefined,
                                exporter_config=ExporterConfig,
                                atomic_ref=AtomicRef,
                                tables=Tabs}) ->
    Exporter = init_exporter(AtomicRef, Tabs, ExporterConfig),
    {next_state, exporting, Data#data{exporter=Exporter}};

%% TODO: check it
idle(_, export_logs, Data) ->
    {next_state, exporting, Data};
idle(EventType, EventContent, Data) ->
    handle_event_(idle, EventType, EventContent, Data).

exporting({timeout, export_logs}, export_logs, _) ->
    %% TODO: check growing of postpone events
    {keep_state_and_data, [postpone]};
exporting(enter, _OldState, #data{exporter=undefined,
                                  atomic_ref=AtomicRef,
                                  tables=Tabs}) ->
    %% exporter still undefined, go back to idle.
    %% First empty the table and disable the processor so no more log events are added.
    %% We wait until the attempt to export to disable so we don't lose log events
    %% on startup but disable once it is clear the exporter isn't being set
    clear_table_and_disable(AtomicRef, Tabs),

    %% use state timeout to transition to `idle' since we can't set a
    %% new state in an `enter' handler
    {keep_state_and_data, [{state_timeout, 0, no_exporter}]};
exporting(enter, _OldState, Data=#data{atomic_ref=AtomicRef,
                                       max_queue_size=MaxSize,
                                       exporting_timeout_ms=ExportingTimeout,
                                       scheduled_delay_ms=SendInterval}) ->
    CurrentTab = ?current_tab(AtomicRef),
    case ?get_available(AtomicRef, CurrentTab) of
        %% No event yet, maximum available capacity, nothing to export
        MaxSize ->
            %% The other table may contain residual (late) writes not exported during
            %% the previous run.
            %% If current table is not empty,
            %% we don't need to check the size of the previous (currently disabled) table,
            %% since we will switch to it after this exporter run.
            %% However, of current table remains empty for the long time, no export and table switch
            %% will be triggered, and any residual late log events in the previous table will be left dangling.
            maybe_export_other_table(CurrentTab, Data);
        _ ->
            RunnerPid = export_logs(CurrentTab, Data),
            {keep_state,
             Data#data{runner_pid=RunnerPid, handed_off_table=CurrentTab},
             [{state_timeout, ExportingTimeout, exporting_timeout},
              {{timeout, export_logs}, SendInterval, export_logs}]}
    end;

%% TODO: we need to just check if `exporter=undefined' right?
%% two hacks since we can't transition to a new state or send an action from `enter'
exporting(state_timeout, no_exporter, Data) ->
    {next_state, idle, Data};
exporting(state_timeout, empty_table, Data) ->
    {next_state, idle, Data};

exporting(state_timeout, exporting_timeout, Data) ->
    %% kill current exporting process because it is taking too long
    Data1 = kill_runner(Data),
    {next_state, idle, Data1};
%% important to verify runner_pid and FromPid are the same in case it was sent
%% after kill_runner was called but before it had done the unlink
exporting(info, {'EXIT', FromPid, _}, Data=#data{runner_pid=FromPid}) ->
    complete_exporting(Data);
%% important to verify runner_pid and FromPid are the same in case it was sent
%% after kill_runner was called but before it had done the unlink
exporting(info, {completed, FromPid}, Data=#data{runner_pid=FromPid}) ->
    complete_exporting(Data);
exporting(EventType, Event, Data) ->
    handle_event_(exporting, EventType, Event, Data).

terminate(_Reason, State, Data=#data{exporter=Exporter,
                                     resource=Resource,
                                     config=Config,
                                     atomic_ref=AtomicRef,
                                     tables=Tabs
                                    }) ->
    CurrentTab = ?current_tab(AtomicRef),
    ?set_available(AtomicRef, CurrentTab, 0),
    T0 = ?time_ms,
    _ = maybe_wait_for_current_runner(State, Data, ?GRACE_SHUTDOWN_MS),
    T1 = ?time_ms,

    OtherTabName = ?tab_name(?next_tab(CurrentTab), Tabs),
    CurrentTabName = ?tab_name(CurrentTab, Tabs),

    %% Check both tables as each one may have some late unexported log events.
    %% NOTE: exports are attempted sequentially to follow the specification restriction:
    %% "Export will never be called concurrently for the same exporter instance"
    %% (see: https://opentelemetry.io/docs/specs/otel/logs/sdk/#export).
    RemTime = ?rem_time(?GRACE_SHUTDOWN_MS, T0, T1),
    ets:info(CurrentTabName, size) > 0
        andalso export_and_wait(CurrentTabName, Resource, Exporter, Config, RemTime),
    T2 = ?time_ms,
    RemTime1 = ?rem_time(RemTime, T1, T2),
    ets:info(OtherTabName, size) > 0
        andalso export_and_wait(OtherTabName, Resource, Exporter, Config, RemTime1),

    _ = otel_exporter:shutdown(Exporter),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

start(Id, Config) ->
    ChildSpec =
        #{id       => Id,
          start    => {?MODULE, start_link, [Config]},
          restart  => transient,
          shutdown => ?SUP_SHUTDOWN_MS,
          type     => worker,
          modules  => [?MODULE]},
    case supervisor:start_child(?SUP, ChildSpec) of
        {ok, _Pid} ->
            {ok, Config};
        {error, {Reason, Ch}} when is_tuple(Ch), element(1, Ch) == child ->
            {error, Reason};
        {error, _Reason}=Error ->
            Error
    end.

handle_event_(State, {timeout, check_table_size}, check_table_size,
              Data=#data{atomic_ref=AtomicRef,
                         max_queue_size=MaxQueueSize,
                         max_export_batch_size=MaxBatchSize,
                         check_table_size_ms=CheckInterval}) ->
%% This is not needed, insertions are disabled automatically,
%%    as soon as available atomic counter drops to 0
%%    Size = ets:info(?CURRENT_TABLE(RegName), size),
%%    case Size >= MaxQueueSize of
%%        true -> disable(RegName);
%%        false -> enable(RegName)
%%    end,
    CurrentTab = ?current_tab(AtomicRef),
    Available = ?get_available(AtomicRef, CurrentTab),

    case Available < 0 of
        true ->
            %% this is just to make sure the counter is not decremented forever and risks
            %% to overflow (though it seems infeasible),
            %% any value below 0 is treated as 0, so we don't care..
            ?set_available(AtomicRef, CurrentTab, 0);
        false -> ok
    end,
    %% TODO: probably keep using ets:info(TabName, size) as before?
    case (MaxQueueSize + 1 - Available) >= MaxBatchSize of
        true when State =:= exporting ->
            %% TODO: must immediately export next batch after the previous finished
            {keep_state_and_data, [?check_tab_timeout(CheckInterval)]};
        true ->
            %% By no means it can ensure that max_export_batch_size is never exceeded.
            %% However, it should work fine to trigger export as soon as the table
            %% reaches the max size instead of waiting for the scheduled export timeout,
            %% that can be a quite large value.
            {next_state, exporting, Data, [?check_tab_timeout(CheckInterval)]};
        false ->
            {keep_state_and_data, [?check_tab_timeout(CheckInterval)]}
    end;
handle_event_(_State, internal, init_exporter, Data=#data{exporter=undefined,
                                                          exporter_config=ExporterConfig,
                                                          atomic_ref=AtomicRef,
                                                          tables=Tabs}) ->
    Exporter = init_exporter(AtomicRef, Tabs, ExporterConfig),
    {keep_state, Data#data{exporter=Exporter}};

handle_event_(_State, {call, From}, {changing_config, NewConfig}, Data) ->
    {keep_state, add_config_to_data(NewConfig, Data), [{reply, From, {ok, NewConfig}}]};
handle_event_(_State, _, _, _) ->
    keep_state_and_data.

init_exporter(AtomicRef, Tabs, ExporterConfig) ->
    case otel_exporter:init(ExporterConfig) of
        Exporter when Exporter =/= undefined andalso Exporter =/= none ->
            Exporter;
        _ ->
            %% exporter is undefined/none
            %% disable the insertion of new log events and delete the current table
            clear_table_and_disable(AtomicRef, Tabs),
            undefined
    end.

maybe_export_other_table(CurrentTab, Data=#data{tables=Tabs,
                                                exporting_timeout_ms=ExportingTimeout,
                                                scheduled_delay_ms=SendInterval}) ->
    NextTab = ?next_tab(CurrentTab),
    %% TODO: this is more complicated, have to check ETS size anyway, as late writes are not reflected
    %% in the counter?
    %% TODO: probably always keep using ETS size for simplicty and atomic counter for OLP?
    case ets:info(?tab_name(NextTab, Tabs), size) of
        0 ->
            %% in an `enter' handler we can't return a `next_state' or `next_event'
            %% so we rely on a timeout to trigger the transition to `idle'
            {keep_state, Data#data{runner_pid=undefined}, [{state_timeout, 0, empty_table}]};
        _ ->
            RunnerPid = export_logs(NextTab, Data),
            {keep_state,
             Data#data{runner_pid=RunnerPid, handed_off_table=CurrentTab},
             [{state_timeout, ExportingTimeout, exporting_timeout},
              {{timeout, export_spans}, SendInterval, export_spans}]}
    end.

export_logs(CurrentTab, #data{exporter=Exporter,
                              max_queue_size=MaxSize,
                              resource=Resource,
                              atomic_ref=AtomicRef,
                              tables=Tabs,
                              config=Config}) ->

    NewCurrentTab = ?next_tab(CurrentTab),
    ?set_current_tab(AtomicRef, NewCurrentTab),
    %% TODO: double check it, but looks like it should work,
    %% we just renew available size for the previous table,
    %% assuming that we export all the data or fail to export all the data
    %% but the table will be empty except some possible late writes...
    ?set_available(AtomicRef, CurrentTab, MaxSize),
    export_async(?tab_name(CurrentTab, Tabs), Resource, Exporter, Config).

export_async(CurrentTab, Resource, Exporter, Config) ->
    From = self(),
    erlang:spawn_link(fun() -> send_logs(From, CurrentTab, Resource, Exporter, Config) end).

send_logs(FromPid, Tab, Resource, Exporter, Config) ->
    export(Exporter, Resource, Tab, Config),
    completed(FromPid).

completed(FromPid) ->
    FromPid ! {completed, self()}.

export(undefined, _, _, _) ->
    true;
export({ExporterModule, ExporterConfig}, Resource, Tab, Config) ->
    %% TODO: better error handling, `
    %% failed_not_retryable` is actually never returned from
    try
        otel_exporter:export_logs(ExporterModule, {Tab, Config}, Resource, ExporterConfig)
            =:= failed_not_retryable
    catch
        Kind:Reason:StackTrace ->
            %% Other logger handler(s) (e.g. default) should be enabled, so that
            %% log events produced by otel_log_handler are not lost in case otel_log_handler
            %% is not functioning properly.
            ?LOG_WARNING(#{source => exporter,
                           during => export,
                           kind => Kind,
                           reason => Reason,
                           exporter => ExporterModule,
                           stacktrace => StackTrace}, #{report_cb => fun ?MODULE:report_cb/1}),
            true
    end.

%% logger format functions
report_cb(#{source := exporter,
            during := export,
            kind := Kind,
            reason := Reason,
            exporter := ExporterModule,
            stacktrace := StackTrace}) ->
    {"log exporter threw exception: exporter=~p ~ts",
     [ExporterModule, otel_utils:format_exception(Kind, Reason, StackTrace)]}.

new_export_table(Name) ->
     ets:new(Name, [public,
                    named_table,
                    {write_concurrency, true},
                    duplicate_bag]).

do_insert(AtomicRef, Tabs, Scope, LogEvent) ->
    CurrentTab = ?current_tab(AtomicRef),
    try
        case ?is_available(AtomicRef, CurrentTab) of
            true ->
                ets:insert(?tab_name(CurrentTab, Tabs), {Scope, LogEvent});
            _ ->
                dropped
        end
    catch
        error:badarg ->
            {error, no_otel_log_handler};
        _:_ ->
            {error, other}
    end.

clear_table_and_disable(AtomicRef, Tabs) ->
    CurrentTab = ?current_tab(AtomicRef),
    ?set_available(AtomicRef, CurrentTab, 0),
    CurrentTabName = ?tab_name(CurrentTab, Tabs),
    ets:delete(CurrentTabName),
    new_export_table(CurrentTabName).

complete_exporting(Data) ->
    {next_state, idle, Data#data{runner_pid=undefined,
                                 handed_off_table=undefined}}.

kill_runner(Data=#data{runner_pid=RunnerPid}) when RunnerPid =/= undefined ->
    Mon = erlang:monitor(process, RunnerPid),
    erlang:unlink(RunnerPid),
    erlang:exit(RunnerPid, kill),
    %% TODO: this is not required, as we don't delete/recreate tables
    receive
        {'DOWN', Mon, process, RunnerPid, _} ->
            Data#data{runner_pid=undefined, handed_off_table=undefined}
    end.

%% terminate/3 helpers

export_and_wait(Tab, Resource, Exporter, Config, Timeout) ->
    RunnerPid = export_async(Tab, Resource, Exporter, Config),
    wait_for_runner(RunnerPid, Timeout).

wait_for_runner(RunnerPid, Timeout) ->
    receive
        {completed, RunnerPid} -> ok;
        {'EXIT', RunnerPid, _} -> ok
    after Timeout ->
            erlang:exit(RunnerPid, kill),
            ok
    end.

maybe_wait_for_current_runner(exporting, #data{runner_pid=RunnerPid}, Timeout) when is_pid(RunnerPid) ->
    wait_for_runner(RunnerPid, Timeout);
maybe_wait_for_current_runner(_State, _Date, _Timeout) -> ok.

%% Config helpers

default_config() ->
    %% exporter is set separately because it's not allowed to be changed for now (requires handler restart)
    #{max_queue_size => ?DEFAULT_MAX_QUEUE_SIZE,
      max_export_batch_size => ?DEFAULT_MAX_EXPORT_BATCH_SIZE,
      exporting_timeout_ms => ?DEFAULT_EXPORTER_TIMEOUT_MS,
      scheduled_delay_ms => ?DEFAULT_SCHEDULED_DELAY_MS,
      check_table_size_ms => ?DEFAULT_CHECK_TABLE_SIZE_MS}.

validate_config(Config) ->
    Errs = maps:fold(fun(K, Val, Acc) ->
                             case validate_opt(K, Val, Config) of
                                 ok -> Acc;
                                 Err -> [Err | Acc]
                             end
              end,
                     [], Config),
    case Errs of
        [] -> ok;
        _ -> {error, Errs}
    end.

validate_opt(max_export_batch_size, Val, Config) ->
    MaxQueueSize = maps:get(max_queue_size, Config, ?DEFAULT_MAX_QUEUE_SIZE),
    IsValid = is_integer(Val) andalso Val > 0 andalso Val =< MaxQueueSize,
    case IsValid of
        true ->
            ok;
        false ->
            {invalid_config,
             "max_export_batch_size must be =< max_queue_size",
             #{max_export_batch_size => Val, max_queue_size => MaxQueueSize}}
    end;
validate_opt(max_queue_size, infinity, _Config) ->
    ok;
validate_opt(K, Val, _Config) when is_integer(Val), Val > 0,
                          K =:= max_queue_size;
                          K =:= exporting_timeout_ms;
                          K =:= scheduled_delay_ms;
                          K =:= check_table_size_ms ->
    ok;
validate_opt(exporter, {Module, _}, _Config) when is_atom(Module) ->
    ok;
validate_opt(exporter, Module, _Config) when is_atom(Module) ->
    ok;
validate_opt(K, Val, _Config) ->
    {invalid_config, K, Val}.

add_config_to_data(#{config := OtelConfig} = Config, Data) ->
    #{max_queue_size:=SizeLimit,
      max_export_batch_size:=MaxExportBatchSize,
      exporting_timeout_ms:=ExportingTimeout,
      scheduled_delay_ms:=ScheduledDelay,
      check_table_size_ms:=CheckTableSize
     } = OtelConfig,
    SizeLimit1 = case SizeLimit of
                     %% high enough, must be infeasible to reach
                     infinity -> ?MAX_SIGNED_INT;
                     Int  -> Int
                 end,
    Data#data{config=Config,
              max_queue_size=SizeLimit1,
              max_export_batch_size=MaxExportBatchSize,
              exporting_timeout_ms=ExportingTimeout,
              scheduled_delay_ms=ScheduledDelay,
              check_table_size_ms=CheckTableSize}.
