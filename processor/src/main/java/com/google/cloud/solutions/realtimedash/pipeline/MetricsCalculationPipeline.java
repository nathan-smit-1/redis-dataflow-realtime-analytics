/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.solutions.realtimedash.pipeline;

import com.google.common.flogger.FluentLogger;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.io.redis.RedisIO.Write.Method;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Realtime Dataflow pipeline to extract experiment metrics from Log Events published on Pub/Sub.
 */
public final class MetricsCalculationPipeline {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final long DEFAULT_WINDOW_DURATION = 1L; // 1 - second

  /**
   * Parses the command line arguments and runs the pipeline.
   */
  public static void main(String[] args) {
    MetricsPipelineOptions options = extractPipelineOptions(args);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<LogEvent> parsedLoggedEvents =
        pipeline
            .apply("Read PubSub Events",
                PubsubIO.readStrings().fromTopic(options.getInputTopic()))
            .apply("Parse Message JSON",
                ParDo.of(new ParseMessageAsLogElement()));

    RedisIO.Write redisWriter =
        RedisIO.write()
            .withEndpoint(
                options.getRedisHost(), options.getRedisPort());

    //visit counter
    parsedLoggedEvents
        .apply("Get stock movement from message", ParDo.of(
            new DoFn<LogEvent, KV<String, String>>() {
              @ProcessElement
              public void getStockMovement(ProcessContext context) {
                LogEvent event = context.element();
                context.output(
                    KV.of("example_key", "1"));
              }
            }
        ))
        .apply("Update stock movement counter", redisWriter.withMethod(Method.INCRBY));

    pipeline.run();
  }

  private static SingleOutput<Pair<String, String>, KV<String, String>>
  hllKeyGenerator(String name) {
    return
        ParDo.of(new DoFn<Pair<String, String>, KV<String, String>>() {
          @ProcessElement
          public void buildHllKey(ProcessContext context) {
            Pair<String, String> elem = context.element();
            context.output(
                KV.of("hll_" + buildPrefix(name) + elem.getKey(), elem.getValue()));
          }
        });
  }

  private static SingleOutput<Pair<String, String>, KV<String, String>> setKeyGenerator(
      String name) {
    return ParDo.of(
        new DoFn<Pair<String, String>, KV<String, String>>() {
          @ProcessElement
          public void buildSetKey(ProcessContext context) {
            Pair<String, String> elem = context.element();
            context.output(
                KV.of("set_" + buildPrefix(name) + elem.getKey(), elem.getValue()));
          }
        });
  }

  private static SingleOutput<LogEvent, Pair<String, String>> extractUsersForDateTime() {
    return ParDo.of(
        new DoFn<LogEvent, Pair<String, String>>() {
          @ProcessElement
          public void extractDateTimeForUser(ProcessContext context) {
            LogEvent elem = context.element();
            context.output(
                Pair.of(elem.getTimestamp().toString(timeBasedKeyBuilder(null)),
                    elem.getUid()));
          }
        });
  }

  private static String timeBasedKeyBuilder(String prefix) {
    return (prefix == null ? "" : ("'" + buildPrefix(prefix) + "'")) + "yyyy_MM_dd'T'HH_mm";
  }

  private static String buildPrefix(String prefix) {
    return (prefix == null || prefix.equals("")) ? "" : prefix + "_";
  }

  /**
   * Parse Pipeline options from command line arguments.
   */
  private static MetricsPipelineOptions extractPipelineOptions(String[] args) {
    MetricsPipelineOptions options = PipelineOptionsFactory
        .fromArgs(args)
        .withValidation()
        .as(MetricsPipelineOptions.class);

    return options;
  }
}
