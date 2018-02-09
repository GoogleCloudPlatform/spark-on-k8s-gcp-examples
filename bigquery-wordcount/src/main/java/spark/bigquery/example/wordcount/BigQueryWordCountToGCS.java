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
 
package spark.bigquery.example.wordcount;

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat;
import com.google.cloud.hadoop.util.EntriesCredentialConfiguration;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.JsonObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.io.IOException;

/**
 * A simple work count example that both reads its input from a BigQuery table, e.g.,
 * publicdata:samples.shakespeare and writes output to a user-specified GCS bucket at path
 * /spark/output/wordcount. GCS is also used as temporary storage for both data exported from the
 * input BigQuery table.
 */
public class BigQueryWordCountToGCS {

  private static final String FS_GS_PROJECT_ID = "fs.gs.project.id";
  private static final String FS_GS_SYSTEM_BUCKET = "fs.gs.system.bucket";
  private static final String FS_GS_WORKING_DIR = "fs.gs.working.dir";

  private static final String WORD_COLUMN = "word";
  private static final String WORD_COUNT_COLUMN = "word_count";

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: BigQueryWordCountToGCS <fully-qualified input table id>");
      System.exit(1);
    }

    Configuration conf = null;
    Path tmpDirPath = null;
    try (JavaSparkContext javaSparkContext = JavaSparkContext
        .fromSparkContext(SparkContext.getOrCreate())) {
      conf = configure(javaSparkContext.hadoopConfiguration(), args);
      tmpDirPath = new Path(conf.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY));
      deleteTmpDir(tmpDirPath, conf);
      Path gcsOutputPath =
          new Path(String.format("gs://%s/spark/output/wordcount", conf.get(FS_GS_SYSTEM_BUCKET)));
      FileSystem gcsFs = gcsOutputPath.getFileSystem(conf);
      if (gcsFs.exists(gcsOutputPath) && !gcsFs.delete(gcsOutputPath, true)) {
        System.err.println("Failed to delete the output directory: " + gcsOutputPath);
      }
      compute(javaSparkContext, conf, gcsOutputPath);
    } finally {
      if (conf != null && tmpDirPath != null) {
        deleteTmpDir(tmpDirPath, conf);
      }
    }
  }

  private static Configuration configure(Configuration conf, String[] args) throws IOException {
    String inputTableId = args[0];
    Preconditions.checkArgument(!Strings.isNullOrEmpty(inputTableId),
        "Input BigQuery table (fully-qualified) ID must not be null or empty");
    String projectId = conf.get(FS_GS_PROJECT_ID);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId),
        "GCP project ID must not be null or empty");
    String systemBucket = conf.get(FS_GS_SYSTEM_BUCKET);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(systemBucket),
        "GCS system bucket must not be null or empty");

    // Basic BigQuery connector configuration.
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId);
    conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, systemBucket);
    conf.set(FS_GS_WORKING_DIR, "/");

    // Use service account for authentication. The service account key file is located at the path
    // specified by the configuration property google.cloud.auth.service.account.json.keyfile.
    conf.set(EntriesCredentialConfiguration.BASE_KEY_PREFIX +
            EntriesCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX,
        "true");

    // Input configuration.
    BigQueryConfiguration.configureBigQueryInput(conf, inputTableId);
    String inputTmpDir = String.format("gs://%s/spark/tmp/bigquery/wordcount", systemBucket);
    conf.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, inputTmpDir);

    return conf;
  }

  private static void compute(JavaSparkContext javaSparkContext, Configuration conf,
      Path gcsOutputPath) {
    JavaPairRDD<LongWritable, JsonObject> tableData = javaSparkContext.newAPIHadoopRDD(
        conf,
        GsonBigQueryInputFormat.class,
        LongWritable.class,
        JsonObject.class);
    JavaPairRDD<String, Long> wordCount = tableData
        .map(entry -> toTuple(entry._2))
        .keyBy(tuple -> tuple._1)
        .mapValues(tuple -> tuple._2)
        .reduceByKey((count1, count2) -> count1 + count2)
        .cache();

    // First write to GCS.
    wordCount.
        mapToPair(tuple -> new Tuple2<>(new Text(tuple._1), new LongWritable(tuple._2))).
        saveAsNewAPIHadoopFile(
            gcsOutputPath.toString(), Text.class, LongWritable.class, TextOutputFormat.class);
  }

  private static void deleteTmpDir(Path tmpDirPath, Configuration conf) throws IOException {
    FileSystem fs = tmpDirPath.getFileSystem(conf);
    if (fs.exists(tmpDirPath) && !fs.delete(tmpDirPath, true)) {
      System.err.println("Failed to delete temporary directory: " + tmpDirPath);
    }
  }

  private static Tuple2<String, Long> toTuple(JsonObject jsonObject) {
    String word = jsonObject.get(WORD_COLUMN).getAsString().toLowerCase();
    long count = jsonObject.get(WORD_COUNT_COLUMN).getAsLong();
    return new Tuple2<>(word, count);
  }
}
