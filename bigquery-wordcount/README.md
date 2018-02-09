# Spark Word Count using BigQuery and Google Cloud Storage

This package contains two variants of a Spark word count example, namely, `BigQueryWordCountToGCS` and `BigQueryWordCountToBigQuery`. Both variants use the [BigQuery](https://cloud.google.com/dataproc/docs/connectors/bigquery) and [GCS](https://cloud.google.com/dataproc/docs/connectors/cloud-storage) connectors. Both variants reads input data from a BigQuery table such as `publicdata:samples.shakespeare`. The two variants differ in where they write the output data to. One version writes its output to a user-specified GCS bucket at path `/spark/output/wordcount`, and the other writes to a user-specified BigQuery table. Both variants of the example requires a GCP service account with the appropriate IAM roles to read and write GCS buckets and objects and to create, read, and write BigQuery datasets and tables.

## Build

To build the example, run the following command:

```
mvn clean package
```

This will create a single jar under `target/` named `bigquery-wordcount-<version>-jar-with-dependencies.jar` with the necessary dependencies. This is the jar to be used as the `<application-jar>` in `spark-submit` and must be accessible locally by the driver and executors at runtime. There are two ways of making the jar available locally to the driver and executors.

## Making the Jar Available to the Driver and Executors

There are two ways of running this example on [Spark on Kubernetes](https://github.com/apache-spark-on-k8s/spark), depending on how the example jar is shipped.

### Staging The Example Jar using the Resource Staging Server

[Spark on Kubernetes](https://github.com/apache-spark-on-k8s/spark) ships with a [Resource Staging Server](https://apache-spark-on-k8s.github.io/userdocs/running-on-kubernetes.html#dependency-management) that can be used to stage resources such as jars and files local to the submission machine. The Spark submission client uploads the resources to the Resource Staging Server, from where they are downloaded by the init-container into the Spark driver and executor Pods so they can be used by the driver and executors. To use it, the Resource Staging Server needs to be deployed to the Kubernetes cluster and the Spark configuration property `spark.kubernetes.resourceStagingServer.uri` needs to be set accordingly. Please refer to the [documentation](https://apache-spark-on-k8s.github.io/userdocs/running-on-kubernetes.html#dependency-management) for more details on how to deploy and use the Resource Staging Server. An example `spark-submit` command when using this option looks like the following:

```
bin/spark-submit \
  --deploy-mode cluster \
  --master k8s://https://192.168.99.100:8443 \
  --kubernetes-namespace default \
  --class spark.bigquery.example.wordcount.BigQueryWordCount \
  --conf spark.executor.instances=1 \
  --conf spark.app.name=bigquery-wordcount \
  --conf spark.kubernetes.driver.docker.image=<driver image> \
  --conf spark.kubernetes.executor.docker.image=<executor image> \
  --conf spark.kubernetes.initcontainer.docker.image=<init-container image> \
  --conf spark.kubernetes.driver.secrets.<GCP service account secret name>=<mount path> \
  --conf spark.kubernetes.executor.secrets.<GCP service account secret name>=<mount path> \
  --conf spark.hadoop.fs.gs.project.id=<GCP project ID> \
  --conf spark.hadoop.fs.gs.system.bucket=<Root GCS bucket to use for temporary working and output directories>  \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=<Path to GCS service account Json key file> \
  --conf spark.kubernetes.resourceStagingServer.uri=<resource staging server URI> \ 
  local:///opt/spark/examples/jars/bigquery-wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar \
  publicdata:samples.shakespeare
```

### Putting The Example Jar into Custom Spark Driver and Executor Images

For users who prefer not using the Resource Staging Server, an alternative way is to put the example jar into custom built Spark driver and executor Docker images. Typically the jar gets copy into the `examples/jars` directory of a unzipped Spark distribution, from where the Docker images are to be built. The entire `examples/jars` directory get copied into the driver and executor images. When using this option, the `<application-jar>` is in the form of `local:///opt/spark/examples/jars/bigquery-wordcount-<version>-jar-with-dependencies.jar`, where the `local://` scheme is needed and it means the jar is locally in the driver and executor containers. An example `spark-submit` command when using this option looks like the following:

```
bin/spark-submit \
  --deploy-mode cluster \
  --master k8s://https://192.168.99.100:8443 \
  --kubernetes-namespace default \
  --class spark.bigquery.example.wordcount.BigQueryWordCount \
  --conf spark.executor.instances=1 \
  --conf spark.app.name=bigquery-wordcount \
  --conf spark.kubernetes.driver.docker.image=<driver image> \
  --conf spark.kubernetes.executor.docker.image=<executor image> \
  --conf spark.kubernetes.initcontainer.docker.image=<init-container image> \
  --conf spark.kubernetes.driver.secrets.<GCP service account secret name>=<mount path> \
  --conf spark.kubernetes.executor.secrets.<GCP service account secret name>=<mount path> \
  --conf spark.hadoop.fs.gs.project.id=<GCP project ID> \
  --conf spark.hadoop.fs.gs.system.bucket=<Root GCS bucket to use for temporary working and output directories>  \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=<Path to GCS service account Json key file> \
  local:///opt/spark/examples/jars/bigquery-wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar \
  publicdata:samples.shakespeare
```    

## Using the BigQuery/GCS Connectors

As mentioned above, this example requires a GCP service account Json key file mounted into the driver and executor containers as a Kubernetes secret volume. Please refer to [Authenticating to Cloud Platform with Service Accounts](https://cloud.google.com/kubernetes-engine/docs/tutorials/authenticating-to-cloud-platform) for detailed information on how to get a service account Json key file and how to create a secret out of it. The service account must have the appropriate roles and permissions to read and write GCS buckets and objects and to create, read, and write BigQuery datasets and tables. As the example commands above show, users can request the secret to be mounted into the driver and executor containers using the following Spark configuration properties:

```
--conf spark.kubernetes.driver.secrets.<GCP service account secret name>=<mount path> \
--conf spark.kubernetes.executor.secrets.<GCP service account secret name>=<mount path> \
``` 

The BigQuery and GCS connectors are special in how they use the service account Json key file to authenticate with the BigQuery and GCS services. Specifically, the following two Spark configuration properties must be set:

```
--conf spark.hadoop.google.cloud.auth.service.account.enable=true \
--conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=<Path to GCS service account Json key file>
``` 

Both connectors also require that the user specifies a GCP project ID for billing and a GCS bucket name for temporary input from data exported from the input BigQuery table and output, which can be done using the following Spark configuration properties.

```
--conf spark.hadoop.fs.gs.project.id=<GCP project ID>
--conf spark.hadoop.fs.gs.system.bucket=<Root GCS bucket to use for temporary working and output directories>
```

## Monitoring and Checking Logs

[Spark on Kubernetes](https://github.com/apache-spark-on-k8s/spark) jobs create a driver Pod and one or more executor Pods named after the Spark application name specified by `spark.app.name`, with a suffix `-driver` for the driver Pod and `-exec-<executor ID>` for the executor Pods. The logs of a driver or executor Pod can be checked using `kubectl logs <pod name>`.

## Known Issues

### Guava Version

The Saprk on Kubernetes distribution (e.g., of the latest release) comes with `guava-14.0.1.jar` under `jars`, which is older than the version used and needed by the GCS/BigQiery connectors. To fix this issue, replace `guava-14.0.1.jar` with one of a newer version, e.g., `19.0`. 
