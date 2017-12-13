# Querying BigQuery Using Spark SQL and DataFrames API

This package contains an example that shows how to query BigQuery using the Spark SQL and DataFrames API with the [spark-bigquery](https://github.com/spotify/spark-bigquery) connector. 

## Build

To build the example, run the following command:

```
mvn clean package
```

This will create a single jar under `target/` named `bigquery-sparksql-<version>-jar-with-dependencies.jar` with the necessary dependencies. This is the jar to be used as the `<application-jar>` in `spark-submit` and must be accessible locally by the driver and executors at runtime. There are two ways of making the jar available locally to the driver and executors as introduced below.

## Running the Example

The example takes the following arguments:
* GCP project ID for billing.
* Cloud Storage bucket for temporary data.
* A file storing the SQL query to run.
* Fully qualified ID of a BigQuery table where the query result will be written to.


There are two ways of running this example on [Spark on Kubernetes](https://github.com/apache-spark-on-k8s/spark), depending on how the example jar and the file storing the SQL query to run are shipped.

### Staging The Resources using the Resource Staging Server

[Spark on Kubernetes](https://github.com/apache-spark-on-k8s/spark) ships with a [Resource Staging Server](https://apache-spark-on-k8s.github.io/userdocs/running-on-kubernetes.html#dependency-management) that can be used to stage resources such as jars and files local to the submission machine. The Spark submission client uploads the resources to the Resource Staging Server, from where they are downloaded by the init-container into the Spark driver and executor Pods so they can be used by the driver and executors. To use it, the Resource Staging Server needs to be deployed to the Kubernetes cluster and the Spark configuration property `spark.kubernetes.resourceStagingServer.uri` needs to be set accordingly. Please refer to the [documentation](https://apache-spark-on-k8s.github.io/userdocs/running-on-kubernetes.html#dependency-management) for more details on how to deploy and use the Resource Staging Server. An example `spark-submit` command when using this option looks like the following:

```
bin/spark-submit \
  --deploy-mode cluster \
  --master k8s://https://192.168.99.100:8443 \
  --kubernetes-namespace default \
  --class spark.bigquery.example.sparksql.BigQuerySparkSQL \
  --conf spark.executor.instances=1 \
  --conf spark.executor.memory=512m \
  --conf spark.driver.cores=0.1 \
  --conf spark.app.name=bigquery-sparksql \
  --conf spark.kubernetes.driver.docker.image=<driver image> \
  --conf spark.kubernetes.executor.docker.image=<executor image> \
  --conf spark.kubernetes.initcontainer.docker.image=<init-container image> \
  --conf spark.kubernetes.driver.secrets.<GCP service account secret name>=<mount path> \
  --conf spark.kubernetes.executor.secrets.<GCP service account secret name>=<mount path> \
  --conf spark.kubernetes.driverEnv.GOOGLE_APPLICATION_CREDENTIALS=<service account JON key file path> \
  --conf spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS=<service account JON key file path> \
  --conf spark.kubernetes.resourceStagingServer.uri=<resource staging server URI> \ 
  examples/jars/bigquery-sparksql-1.0-SNAPSHOT-jar-with-dependencies.jar \
  <GCP project ID> \
  <GCS bucket for temporary data> \
  path/to/sql/query/file.sql \
  <output BigQuery table ID> 
```

### Putting The Resources into Custom Spark Driver and Executor Images

For users who prefer not using the Resource Staging Server, an alternative way is to put the resources into custom built Spark driver and executor Docker images. Typically the jar gets copy into the `examples/jars` directory of a unzipped Spark distribution, from where the Docker images are to be built. The entire `examples/jars` directory get copied into the driver and executor images. In this case, users may also choose to put the file storing the SQL query into the same directory as the application jar. When using this option, the `<application-jar>` is in the form of `local:///opt/spark/examples/jars/bigquery-sparksql-<version>-jar-with-dependencies.jar`, where the `local://` scheme is needed and it means the jar is locally in the driver and executor containers. An example `spark-submit` command when using this option looks like the following:

```
bin/spark-submit \
  --deploy-mode cluster \
  --master k8s://https://192.168.99.100:8443 \
  --kubernetes-namespace default \
  --class spark.bigquery.example.sparksql.BigQuerySparkSQL \
  --conf spark.executor.instances=3 \
  --conf spark.executor.memory=512m \
  --conf spark.driver.cores=0.1 \
  --conf spark.app.name=bigquery-sparksql \
  --conf spark.kubernetes.driver.docker.image=<driver image> \
  --conf spark.kubernetes.executor.docker.image=<executor image> \
  --conf spark.kubernetes.driver.secrets.<GCP service account secret name>=<mount path> \
  --conf spark.kubernetes.executor.secrets.<GCP service account secret name>=<mount path> \
  --conf spark.kubernetes.driverEnv.GOOGLE_APPLICATION_CREDENTIALS=<service account JON key file path> \
  --conf spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS=<service account JON key file path> \
  local:///opt/spark/examples/jars/bigquery-sparksql-1.0-SNAPSHOT-jar-with-dependencies.jar \
  <GCP project ID> \
  <GCS bucket for temporary data> \
  local:///path/to/sql/query/file.sql \
  <output BigQuery table ID> 
```   

### Getting a GCP Service Account JSON Key File

This example, like other examples that access GCS or BigQuery, needs a GCP service account to authenticate with the BigQuery service. To create a new or use an existing GCP service account, furnish a private key, and download a JSON key file, please following instructions in [Authenticating to Cloud Platform with Service Accounts](https://cloud.google.com/kubernetes-engine/docs/tutorials/authenticating-to-cloud-platform). Once the JSON key file is downloaded locally, a Kubernetes secret can be created from the file using the following command:
      
```
kubectl create secret generic <secret name> --from-file=/path/to/key/file
```

This new secret can then be mounted into the driver and executor containers using the following Saprk configuration options:

```
--conf spark.kubernetes.driver.secrets.<GCP service account secret name>=<mount path> \
--conf spark.kubernetes.executor.secrets.<GCP service account secret name>=<mount path> \
```

The example uses the [spark-bigquery](https://github.com/spotify/spark-bigquery) connector to read and write BigQuery tables. This connector supports [Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials). So to make the connector be aware of the service account JSON key file, the environment variable `GOOGLE_APPLICATION_CREDENTIALS` needs to be set in the driver and executor containers using the following configuration options:

```
--conf spark.kubernetes.driverEnv.GOOGLE_APPLICATION_CREDENTIALS=<service account JON key file path> \
--conf spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS=<service account JON key file path> \
```

## Monitoring and Checking Logs

[Spark on Kubernetes](https://github.com/apache-spark-on-k8s/spark) jobs create a driver Pod and one or more executor Pods named after the Spark application name specified by `spark.app.name`, with a suffix `-driver` for the driver Pod and `-exec-<executor ID>` for the executor Pods. The logs of a driver or executor Pod can be checked using `kubectl logs <pod name>`.

## Known Issues

### Using `spark-avro` with Spark 2.2.0

The latest version of the spark-bigquery connector uses version `3.2.0` of `spark-avro`, which does not work with Spark `2.2.0` according to this Github [issue](https://github.com/databricks/spark-avro/issues/240), due to missing implementation of the `org.apache.spark.sql.execution.datasources.OutputWriter.write` method added in version `2.2.0` of `spark-sql`. There are two workarounds for this problem. One is to download the release jar of version [`4.0.0`](https://mvnrepository.com/artifact/com.databricks/spark-avro_2.11/4.0.0) of `spark-avro` and add it to `jars/` of the latest [release](https://github.com/apache-spark-on-k8s/spark/releases/tag/v2.2.0-kubernetes-0.5.0) of Spark on Kubernetes. The second solutions is to add the following option to the `spark-submit` script when submitting a Spark application:

```
--packages com.databricks:spark-avro_2.11:4.0.0
```         
