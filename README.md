flink-skyline-computation

In the context of a university course, I created this flink app in order to compute the skyline of multidimensional data in a streaming way.

Data are read from an input Kafka-topic. The app consumes another Kafka-topic which is used for triggering the skyline computation. So as more data are accumulated, they are pre-processed and when triggered the app produces the skyline and writes it to an output topic.

parameters:

—a: [“dim”, “grid”, “angle”] —w: parallelism —v: data maximum value per dimension (exclusive) —d: number of data dimensions —p: number of partitions —it: input topic —ot: output topic —tt: trigger topic
