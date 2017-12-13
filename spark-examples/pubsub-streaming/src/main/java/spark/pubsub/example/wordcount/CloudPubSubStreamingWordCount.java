/*
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package spark.pubsub.example.wordcount;

import com.google.cloud.hadoop.util.EntriesCredentialConfiguration;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.pubsub.PubsubUtils;
import org.apache.spark.streaming.pubsub.SparkGCPCredentials;
import org.apache.spark.streaming.pubsub.SparkPubsubMessage;

import scala.Tuple2;

/**
 * A simple Spark Streaming example that gets words from a Cloud PubSub topic, computes the word
 * counts, and writes the result of each batch to a file on GCS.
 *
 * <p> This example uses the GCS connector's Hadoop configuration property {@code
 * google.cloud.auth.service.account.json.keyfile} for specifying the GCP service account key file
 * path, even though {@link SparkGCPCredentials} also works with Application Default Credentials
 * that uses the environment variable GOOGLE_APPLICATION_CREDENTIALS. </p>
 */
public class CloudPubSubStreamingWordCount {

  public static void main(String[] args) throws InterruptedException {
    if (args.length != 4) {
      System.err.println("Usage: CloudPubSubStreamingWordCount <GCP project ID> " +
          "<Cloud PubSub subscription> <GCS output dir path> <job duration in seconds>");
      System.exit(1);
    }

    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(args[0]), "GCP project ID must not be null or empty");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(args[1]), "Cloud PubSub topic name must not be empty");

    JavaStreamingContext jsc = new JavaStreamingContext(
        new SparkConf().setAppName("Cloud PubSub Spark Streaming Word Count"),
        Seconds.apply(30) // Batch duration
    );

    Configuration hadoopConf = jsc.sparkContext().hadoopConfiguration();
    // Use service account for authentication. The service account key file is located at the path
    // specified by the configuration property google.cloud.auth.service.account.json.keyfile.
    hadoopConf.set(
        EntriesCredentialConfiguration.BASE_KEY_PREFIX +
            EntriesCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX,
        "true");
    // Use the service account Json key file shared with the GCS connector.
    String serviceAccountJsonKeyFilePath = hadoopConf.get(
        EntriesCredentialConfiguration.BASE_KEY_PREFIX +
            EntriesCredentialConfiguration.JSON_KEYFILE_SUFFIX);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(serviceAccountJsonKeyFilePath),
        "Service account Json key file path must be specified");

    // This will create a subscription to the given topic.
    JavaReceiverInputDStream<SparkPubsubMessage> pubSubStream = PubsubUtils.createStream(
        jsc,
        args[0], // GCP project ID
        args[1], // Cloud PubSub subscription
        new SparkGCPCredentials.Builder()
            .jsonServiceAccount(serviceAccountJsonKeyFilePath)
            .build(),
        StorageLevel.MEMORY_AND_DISK_SER());

    JavaPairDStream<String, Long> wordCounts = pubSubStream
        .mapToPair(message -> new Tuple2<>(new String(message.getData()), 1L))
        .reduceByKey((count1, count2) -> count1 + count2);

    final String gcsFilePathTemplate = args[2] + "/batch-%d";
    wordCounts
        .mapToPair(tuple -> new Tuple2<>(new Text(tuple._1), new LongWritable(tuple._2)))
        .foreachRDD(rdd -> rdd
            .saveAsNewAPIHadoopFile(String.format(gcsFilePathTemplate, rdd.id()),
                Text.class,
                LongWritable.class,
                TextOutputFormat.class));

    try {
      jsc.start();
      // Let the job run for the given duration and then terminate it.
      jsc.awaitTerminationOrTimeout(TimeUnit.SECONDS.toMillis(Long.parseLong(args[3])));
    } finally {
      jsc.stop(true, true);
    }
  }
}
