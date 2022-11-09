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

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.flogger.FluentLogger;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DoubleCoder;
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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.transforms.MapElements;
import com.google.cloud.solutions.realtimedash.pipeline.BranchCompanySkuValue;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;

/**
 * Realtime Dataflow pipeline to extract experiment metrics from Log Events
 * published on Pub/Sub.
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

        String project = "sg-apps-store-fulfillment-dev";
        String dataset = "testing";
        String table = "central_branch_vw";

        String query = String.format("SELECT branch_id, cpy_id FROM `%s.%s.%s`;",
                project, dataset, table);

        final PCollectionView<Map<String, String>> branches = pipeline
                // Emitted long data trigger this batch read BigQuery client job.
                .apply(String.format("Updating every %s hours", 10),
                 GenerateSequence.from(0).withRate(1, Duration.standardHours(10)))
                
                .apply("Assign to Fixed Window", Window
                        .<Long>into(FixedWindows.of(Duration.standardHours(10)))
                )
                
                .apply(new ReadSlowChangingTable("Read BigQuery Table", query, "branch_id", "cpy_id"))
                // Caching results as Map.
                .apply("View As Map", View.<String, String>asMap());

        PCollection<BranchCompanySkuValue> parsedLoggedEvents
                = pipeline
                        .apply("Read PubSub Events",
                                PubsubIO.readStrings().fromTopic(options.getInputTopic()))
                        .apply("Parse Message JSON",
                                ParDo.of(new ParseMessageAsLogElement()));
     
        PCollection<BranchCompanySkuValue> output = parsedLoggedEvents.apply(
         ParDo
           .of(new AddCompanyDataToMessage(branches))
           // the side input is provided here and above
           // now the city map is available for the transform to use
           .withSideInput("branches", branches)
       );
        
        //create partitions based on company
        
        PCollectionList<BranchCompanySkuValue> msgs = parsedLoggedEvents.apply(Partition.of(2, new PartitionFn<BranchCompanySkuValue>() {
            public int partitionFor(BranchCompanySkuValue msg, int numPartitions) {
                // TODO: determine how to partition messages
                if (msg.getCompany() == "1") {
                    return 0;
                } else {
                    return 1;
                }
            }
        }));

PCollection<BranchCompanySkuValue> partition1 = msgs.get(0);

RedisIO.Write redisWriter = RedisIO.write().withEndpoint(options.getRedisHost(), 
                                                         options.getRedisPort());
        
        //stock movement updater
        partition1
                .apply("Get stock movement from message", ParDo.of(
                        new DoFn<BranchCompanySkuValue, KV<String, String>>() {
                    @ProcessElement
                    public void getStockMovement(ProcessContext context) {
                        BranchCompanySkuValue updateValue = context.element();
                        context.output(
                                KV.of(updateValue.getBranch() 
                                        + "|" + updateValue.getCompany()
                                        + "|" + updateValue.getSku()
                                                , updateValue.getValue()));
                    }
                }
                ))
                .apply("Update stock movement counter", redisWriter.withMethod(Method.INCRBY));

        pipeline.run();
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
