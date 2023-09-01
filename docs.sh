#!/bin/bash
set -e

# Setup:
#
#     # 1. install OTP 24+
#     # 2. install ExDoc:
#     $ mix escript.install github elixir-lang/ex_doc

rebar3 compile
rebar3 edoc
sdk_version=1.3.1
api_version=1.2.2
otlp_version=1.6.0
zipkin_version=1.1.0
semconv_version=0.2.0

ex_doc "opentelemetry" $sdk_version "_build/default/lib/opentelemetry/ebin" \
  --source-ref v${sdk_version} \
  --config apps/opentelemetry/docs.config $@ \
  --output "apps/opentelemetry/doc"

ex_doc "opentelemetry_exporter" $otlp_version "_build/default/lib/opentelemetry_exporter/ebin" \
  --source-ref v${otlp_version} \
  --config apps/opentelemetry_exporter/docs.config $@ \
  --output "apps/opentelemetry_exporter/doc"

ex_doc "opentelemetry_zipkin" $zipkin_version "_build/default/lib/opentelemetry_zipkin/ebin" \
  --source-ref v${zipkin_version} \
  --config apps/opentelemetry_zipkin/docs.config $@ \
  --output "apps/opentelemetry_zipkin/doc"

pushd apps/opentelemetry_api
mix deps.get
mix compile
popd
ex_doc "opentelemetry_api" $api_version "apps/opentelemetry_api/_build/dev/lib/opentelemetry_api/ebin" \
  --source-ref v${api_version} \
  --config apps/opentelemetry_api/docs.config $@ \
  --output "apps/opentelemetry_api/doc"

pushd apps/opentelemetry_semantic_conventions/
mix deps.get
mix compile
popd
ex_doc "opentelemetry_semantic_conventions" $semconv_version "apps/opentelemetry_semantic_conventions/_build/dev/lib/opentelemetry_semantic_conventions/ebin" \
  --source-ref v${semconv_version} \
  --config apps/opentelemetry_semantic_conventions/docs.config $@ \
  --output "apps/opentelemetry_semantic_conventions/doc"
