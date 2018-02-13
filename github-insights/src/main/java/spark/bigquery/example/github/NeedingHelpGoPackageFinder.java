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
 
package spark.bigquery.example.github;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.spotify.spark.bigquery.BigQueryDataFrame;
import com.spotify.spark.bigquery.BigQuerySQLContext;
import com.spotify.spark.bigquery.CreateDisposition;
import com.spotify.spark.bigquery.WriteDisposition;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

/**
 * This is an example Spark pipeline that finds popular Golang projects that need help from
 * contributors for bug fixes, feature implementations, or documentation improvements, etc.
 */
public class NeedingHelpGoPackageFinder {

  private static final String GO_FILES_QUERY_TEMPLATE =
      "SELECT id\n"
          + "FROM [bigquery-public-data:github_repos.%s]\n"
          + "WHERE RIGHT(path, 3) = '.go'";

  private static final String GO_FILES_TABLE = "go_files";

  private static final String GO_CONTENTS_QUERY_TEMPLATE =
      "SELECT sample_repo_name as repo_name, content\n"
          + "FROM [bigquery-public-data:github_repos.%s]\n"
          + "WHERE id IN (SELECT id FROM %s.%s)";

  private static final String POPULAR_PACKAGES_QUERY_TEMPLATE =
      "SELECT\n"
          + "  h.repo_name as repo_name,\n"
          + "  h.package as package,\n"
          + "  help_count,\n"
          + "  import_count,\n"
          + "  ceil(log(help_count*2)*log(import_count*2)) as popularity\n"
          + "FROM\n"
          + "  [%s:%s.%s] AS h\n"
          + "INNER JOIN\n"
          + "  [%s:%s.%s] AS i\n"
          + "ON\n"
          + "  h.repo_name = i.repo_name\n"
          + "  AND h.package = i.package\n"
          + "ORDER BY\n"
          + "  popularity DESC";

  private static final String GO_PACKAGE_KEYWORD = "package";

  private static final Pattern GO_SINGLE_IMPORT_PATTERN = Pattern
      .compile(".*import\\s+\"(.*)\".*");

  private static final Pattern GO_BLOCK_IMPORT_PATTERN = Pattern
      .compile("(?s).*import\\s+\\(([^)]*)\\).*");

  private static final String GO_PACKAGE_HELPS_TABLE = "go_package_helps";

  private static final String GO_PACKAGE_IMPORTS_TABLE = "go_package_imports";

  private static final String POPULAR_GO_PACKAGES_TABLE = "popular_go_packages";

  private static final StructType GO_PACKAGES_HELPS_TABLE_SCHEMA = new StructType(
      new StructField[]{
          new StructField("repo_name", DataTypes.StringType, false, Metadata.empty()),
          new StructField("package", DataTypes.StringType, false, Metadata.empty()),
          new StructField("help_count", DataTypes.IntegerType, false, Metadata.empty()),
      }
  );

  private static final StructType GO_PACKAGE_IMPORTS_TABLE_SCHEMA = new StructType(
      new StructField[]{
          new StructField("repo_name", DataTypes.StringType, true, Metadata.empty()),
          new StructField("package", DataTypes.StringType, false, Metadata.empty()),
          new StructField("import_count", DataTypes.IntegerType, false, Metadata.empty()),
      }
  );

  private final String projectId;
  private final String bigQueryDataset;

  private final SQLContext sqlContext;
  private final BigQuerySQLContext bigQuerySQLContext;

  private final boolean useSampleTables;

  private NeedingHelpGoPackageFinder(
      String projectId,
      String bigQueryDataset,
      String gcsBucket,
      boolean useSampleTables) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId),
        "GCP project ID must not be empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(bigQueryDataset),
        "BigQuery dataset name must not be empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(gcsBucket),
        "GCS bucket must not be empty");

    this.projectId = projectId;
    this.bigQueryDataset = bigQueryDataset;

    this.sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate());
    this.bigQuerySQLContext = new BigQuerySQLContext(this.sqlContext);
    this.bigQuerySQLContext.setBigQueryProjectId(projectId);
    this.bigQuerySQLContext.setBigQueryGcsBucket(gcsBucket);

    this.useSampleTables = useSampleTables;
  }

  public static void main(String[] args) throws Exception {
    Options opts = new Options();
    opts.addOption("usesample", false, "Use the sample tables if present.");
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(opts, args);
    args = cmd.getArgs();

    if (args.length != 3) {
      System.err.println("Usage: NeedingHelpGoPackageFinder "
          + "<GCP project ID for billing> "
          + "<BigQuery dataset for output tables> "
          + "<GCS bucket used by BigQuery for temporary data>");
      System.exit(1);
    }

    String projectId = args[0];
    String bigQueryDataset = args[1];
    String gcsBucket = args[2];
    NeedingHelpGoPackageFinder finder = new NeedingHelpGoPackageFinder(projectId, bigQueryDataset,
        gcsBucket, cmd.hasOption("usesample"));
    finder.run();
  }

  private void run() {
    String goFilesTableId = String
        .format("%s:%s.%s", this.projectId, this.bigQueryDataset, GO_FILES_TABLE);
    selectAndOutputGoFilesTable(goFilesTableId);

    Dataset<Row> goContentsDataset = queryAndCacheGoContents();

    String goPackageHelpsTableId = String
        .format("%s:%s.%s", this.projectId, this.bigQueryDataset, GO_PACKAGE_HELPS_TABLE);
    outputGoPackageHelpsTable(getNeedHelpPackages(goContentsDataset),
        goPackageHelpsTableId);

    String goPackageImportsTableId = String
        .format("%s:%s.%s", this.projectId, this.bigQueryDataset, GO_PACKAGE_IMPORTS_TABLE);
    outputGoPackageImportsTable(getImportedPackages(goContentsDataset), goPackageImportsTableId);

    String popularGoPackagesTableId = String
        .format("%s:%s.%s", this.projectId, this.bigQueryDataset, POPULAR_GO_PACKAGES_TABLE);
    queryAndOutputPopularPackagesTable(popularGoPackagesTableId);
  }

  /**
   * This method selects out Go files from github_repos.sample_files and output the result to the
   * given BigQuery table.
   */
  private void selectAndOutputGoFilesTable(String outputTableId) {
    Dataset<Row> dataset = this.bigQuerySQLContext.bigQuerySelect(
        String.format(GO_FILES_QUERY_TEMPLATE, this.useSampleTables ? "sample_files" : "files"));
    BigQueryDataFrame bigQueryDataFrame = new BigQueryDataFrame(dataset);
    bigQueryDataFrame.saveAsBigQueryTable(outputTableId, CreateDisposition.CREATE_IF_NEEDED(),
        WriteDisposition.WRITE_EMPTY());
  }

  /**
   * This method selects out Go source file contents from github_repos.sample_contents for files
   * in the table written by {@link #selectAndOutputGoFilesTable(String)} and output the result to
   * the given BigQuery table.
   */
  private Dataset<Row> queryAndCacheGoContents() {
    Dataset<Row> dataset = this.bigQuerySQLContext
        .bigQuerySelect(
            String.format(
              GO_CONTENTS_QUERY_TEMPLATE,
              this.useSampleTables ? "sample_contents" : "contents",
              this.bigQueryDataset,
              GO_FILES_TABLE));
    return dataset.persist(StorageLevel.MEMORY_AND_DISK());
  }

  private static JavaPairRDD<String, String> getContentsByRepoNames(
      Dataset<Row> goContentsDataset) {
    return goContentsDataset.select("repo_name", "content")
        .javaRDD()
        .mapToPair(row -> new Tuple2<>(row.getString(0), row.getString(1)))
        .filter(tuple -> tuple._2() != null);
  }

  /**
   * This method outputs the result of the number of places helps are needed for each
   * repository/package combination to the given BigQuery table.
   */
  private void outputGoPackageHelpsTable(
      JavaPairRDD<Tuple2<String, String>, Integer> packagesNeedingHelp, String outputTableId) {
    Dataset<Row> dataset = this.sqlContext.createDataFrame(packagesNeedingHelp
            .map(tuple -> RowFactory.create(tuple._1()._1(), tuple._1()._2(), tuple._2()))
            .rdd(),
        GO_PACKAGES_HELPS_TABLE_SCHEMA);
    BigQueryDataFrame bigQueryDataFrame = new BigQueryDataFrame(dataset);
    bigQueryDataFrame.saveAsBigQueryTable(outputTableId, CreateDisposition.CREATE_IF_NEEDED(),
        WriteDisposition.WRITE_EMPTY());
  }

  /**
   * This method outputs the result of the number of times a package is imported in other
   * repositories to the given BigQuery table.
   */
  private void outputGoPackageImportsTable(
      JavaPairRDD<Tuple2<String, String>, Integer> packageImports, String outputTableId) {
    Dataset<Row> dataset = this.sqlContext.createDataFrame(
        packageImports
            .map(tuple -> RowFactory.create(tuple._1()._1(), tuple._1()._2(), tuple._2()))
            .rdd(),
        GO_PACKAGE_IMPORTS_TABLE_SCHEMA);
    BigQueryDataFrame bigQueryDataFrame = new BigQueryDataFrame(dataset);
    bigQueryDataFrame
        .saveAsBigQueryTable(outputTableId, CreateDisposition.CREATE_IF_NEEDED(),
            WriteDisposition.WRITE_EMPTY());
  }

  /**
   * This method outputs the final result of popular Go packages that need help sorted by a
   * popularity factor that is calculated as {@code log(num_helps_needed)*log(num_imports)}.
   */
  private void queryAndOutputPopularPackagesTable(String outputTableId) {
    Dataset<Row> dataset = this.bigQuerySQLContext
        .bigQuerySelect(String
            .format(POPULAR_PACKAGES_QUERY_TEMPLATE,
                this.projectId, this.bigQueryDataset, GO_PACKAGE_HELPS_TABLE,
                this.projectId, this.bigQueryDataset, GO_PACKAGE_IMPORTS_TABLE));
    BigQueryDataFrame bigQueryDataFrame = new BigQueryDataFrame(dataset);
    bigQueryDataFrame
        .saveAsBigQueryTable(outputTableId, CreateDisposition.CREATE_IF_NEEDED(),
            WriteDisposition.WRITE_EMPTY());
  }

  /**
   * This method takes the sample contents dataset and produces a JavaPairRDD with keys in the
   * form of (sample_repo_name, package) and values being a count a particular repo_name/package
   * combination needs help. The result is sorted in descending order by the counts.
   */
  private static JavaPairRDD<Tuple2<String, String>, Integer> getNeedHelpPackages(
      Dataset<Row> goContentsDataset) {
    return getContentsByRepoNames(goContentsDataset)
        .filter(tuple -> tuple._2().contains("TODO") || tuple._2().contains("FIXME"))
        .flatMapValues(content -> Splitter.on('\n').omitEmptyStrings().trimResults().split(content))
        .filter(tuple -> tuple._2().startsWith(GO_PACKAGE_KEYWORD))
        .mapValues(line -> line.substring(GO_PACKAGE_KEYWORD.length() + 1))
        .mapToPair(tuple -> new Tuple2<>(new Tuple2<>(tuple._1(), tuple._2()), 1))
        .reduceByKey((left, right) -> left + right)
        .mapToPair(Tuple2::swap)
        .sortByKey(false)
        .mapToPair(Tuple2::swap);
  }

  /**
   * This method takes the sample contents dataset and produces a JavaPairRDD with keys in the
   * form of (sample_repo_name, package) and values being a count a particular repo_name/package
   * combination is imported by other projects. The result is sorted in descending order by the
   * counts.
   */
  private static JavaPairRDD<Tuple2<String, String>, Integer> getImportedPackages(
      Dataset<Row> goContentsDataset) {
    return getContentsByRepoNames(goContentsDataset)
        .map(tuple -> getImportedPackages(tuple._2()))
        .flatMap(
            importPaths -> Splitter.on('\n').omitEmptyStrings().trimResults().split(importPaths)
                .iterator())
        .filter(path -> !path.startsWith("//")) // Ignore comments.
        .mapToPair(path -> new Tuple2<>(getRepoNameAndPackage(path), 1))
        .reduceByKey((left, right) -> left + right)
        .mapToPair(Tuple2::swap)
        .sortByKey(false)
        .mapToPair(Tuple2::swap);
  }

  private static String getImportedPackages(String importContent) {
    Matcher matcher = GO_BLOCK_IMPORT_PATTERN.matcher(importContent);
    if (matcher.matches()) {
      return matcher.group(1).replaceAll("\"", ""); // Get rid of the double quotes.
    }
    matcher = GO_SINGLE_IMPORT_PATTERN.matcher(importContent);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    return "";
  }

  /**
   * This method converts a Go import path into a combination of a repository name and a package
   * name on a best-effort basis with some special handling for popular Go repositories.
   */
  private static Tuple2<String, String> getRepoNameAndPackage(String importPath) {
    // Get rid of the alias.
    if (importPath.contains(" ")) {
      importPath = importPath.substring(importPath.indexOf(' ') + 1);
    }
    List<String> items = Splitter.on('/').splitToList(importPath);

    // Standard top-level Go packages.
    if (items.size() < 2) {
      return new Tuple2<>("golang/go", importPath);
    }

    // Special handling for import paths starting with "k8s.io".
    if (items.get(0).equalsIgnoreCase("k8s.io")) {
      return new Tuple2<>("kubernetes/" + items.get(1), Iterables.getLast(items));
    }

    if (items.size() == 2) {
      return new Tuple2<>(importPath, Iterables.getLast(items));
    }

    // Special handling for import paths starting with "golang.org/x".
    if (importPath.startsWith("golang.org/x")) {
      return new Tuple2<>("golang/" + items.get(2), Iterables.getLast(items));
    }

    // Special handling for import paths starting with "github.com".
    if (items.get(0).equalsIgnoreCase("github.com")) {
      return new Tuple2<>(items.get(1) + "/" + items.get(2), Iterables.getLast(items));
    }

    // All other cases.
    return new Tuple2<>(items.get(1) + "/" + items.get(2), Iterables.getLast(items));
  }
}
