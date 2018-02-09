# Spark Streaming Word Count using Cloud PubSub

This package contains a simple Spark Streaming example that computes word counts from data stream received from a
 [Google Cloud PubSub](https://cloud.google.com/pubsub/) topic and writes the result to a
 [Google Cloud Storage](https://cloud.google.com/storage/) (GCS) bucket. The example uses the [Apache Bahir](http://bahir.apache.org/)'s
 [Spark Streaming connector for Google Cloud Pub/Sub](https://github.com/apache/bahir/tree/master/streaming-pubsub) to pull
 data from a given Cloud PubSub topic and the [GCS connector](https://cloud.google.com/dataproc/docs/connectors/cloud-storage)
 to write the result to GCS. The example requires a GCP service account with the appropriate IAM roles to write to GCS, and subscribe to
 a Cloud PubSub topic.

## Usage

The example takes the following four input arguments:
* GCP project ID for Cloud PubSub and GCS usage billing.
* Name of the Cloud PubSub subscription to the topic where words are pulled from. Note that the subscription name here is the the last part
of a full subscription name, e.g., `<subscription name>` in `projects/<project id>/subscriptions/<subscription name>`.
* GCS path for output with a `gs://` scheme, e.g., `gs://spark-streaming/wordcount/output`.
* Job duration in seconds.

## Build

To build the example, run the following command:

```
mvn clean package
```

This will create a single jar under target/ named `pubsub-streaming-<version>-jar-with-dependencies.jar` with the necessary dependencies.
This is the jar to be used as the `<application-jar>` in spark-submit and must be accessible locally by the driver and executors at runtime.
There are two ways of making the jar available locally to the driver and executors.

## Making the Jar Available to the Driver and Executors

There are two ways of running this example on [Spark on Kubernetes](https://github.com/apache-spark-on-k8s/spark), depending on how the
example jar is shipped.

### Staging The Example Jar using the Resource Staging Server

For users who prefer not using the Resource Staging Server, an alternative way is to put the example jar into custom built Spark driver
and executor Docker images. Typically the jar gets copy into the examples/jars directory of a unzipped Spark distribution, from where the
Docker images are to be built. The entire examples/jars directory get copied into the driver and executor images. When using this option,
the `<application-jar>` is in the form of `local:///opt/spark/examples/jars/pubsub-streaming-<version>-jar-with-dependencies.jar`, where
the `local://` scheme is needed and it means the jar is locally in the driver and executor containers. An example spark-submit command when
using this option looks like the following:

```
bin/spark-submit \
  --deploy-mode cluster \
  --master k8s://https://192.168.99.100:8443 \
  --kubernetes-namespace default \
  --class spark.pubsub.example.wordcount.CloudPubSubStreamingWordCount \
  --conf spark.executor.instances=1 \
  --conf spark.app.name=pubsub-wordcount \
  --conf spark.kubernetes.driver.docker.image=<driver image> \
  --conf spark.kubernetes.executor.docker.image=<executor image> \
  --conf spark.kubernetes.initcontainer.docker.image=<init-container image> \
  --conf spark.kubernetes.driver.secrets.<GCP service account secret name>=<mount path> \
  --conf spark.kubernetes.executor.secrets.<GCP service account secret name>=<mount path> \
  --conf spark.hadoop.fs.gs.project.id=<GCP project ID> \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=<path to GCS service account Json key file> \
  --conf spark.kubernetes.resourceStagingServer.uri=<resource staging server URI> \
  path/to/pubsub-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar \
  <GCP project ID> <Cloud PubSub subscription name)> <gs://bucket/path/for/output> <job duration>
```


### Putting The Example Jar into Custom Spark Driver and Executor Images

For users who prefer not using the Resource Staging Server, an alternative way is to put the example jar into custom built Spark driver
and executor Docker images. Typically the jar gets copy into the `examples/jars` directory of a unzipped Spark distribution, from where
the Docker images are to be built. The entire `examples/jars` directory get copied into the driver and executor images. When using this
option, the `<application-jar>` is in the form of `local:///opt/spark/examples/jars/pubsub-streaming-<version>-jar-with-dependencies.jar`,
where the `local://` scheme is needed and it means the jar is locally in the driver and executor containers. An example `spark-submit`
command when using this option looks like the following:

```
bin/spark-submit \
  --deploy-mode cluster \
  --master k8s://https://192.168.99.100:8443 \
  --kubernetes-namespace default \
  --class spark.pubsub.example.wordcount.CloudPubSubStreamingWordCount \
  --conf spark.executor.instances=1 \
  --conf spark.app.name=pubsub-wordcount \
  --conf spark.kubernetes.driver.docker.image=<driver image> \
  --conf spark.kubernetes.executor.docker.image=<executor image> \
  --conf spark.kubernetes.driver.secrets.<GCP service account secret name>=<mount path> \
  --conf spark.kubernetes.executor.secrets.<GCP service account secret name>=<mount path> \
  --conf spark.hadoop.fs.gs.project.id=<GCP project ID> \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=<path to GCS service account Json key file> \
  local:///path/to/pubsub-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar \
  <GCP project ID> <Cloud PubSub subscription name)> <gs://bucket/path/for/output> <job duration>
```

## Monitoring and Checking Logs

[Spark on Kubernetes](https://github.com/apache-spark-on-k8s/spark) jobs create a driver Pod and one or more executor
Pods named after the Spark application name specified by `spark.app.name`, with a suffix `-driver` for the driver
Pod and `-exec-<executor ID>` for the executor Pods. The logs of a driver or executor Pod can be checked using
`kubectl logs <pod name>`.

## Known Issues

### Guava Version

The Saprk on Kubernetes distribution (e.g., of the latest release) comes with `guava-14.0.1.jar` under `jars`, which is older than the version used and needed by the GCS connector. To fix this issue, replace `guava-14.0.1.jar` with one of a newer version, e.g., `19.0`. 

