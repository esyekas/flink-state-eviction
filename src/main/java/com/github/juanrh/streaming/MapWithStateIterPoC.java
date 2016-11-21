package com.github.juanrh.streaming;

import com.github.juanrh.streaming.source.ElementsWithGapsSource;
import lombok.Data;
import lombok.NonNull;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Created by juanrh on 11/13/2016.
 *
 * https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/datastream_api.html#iterations
 */
/*

"C:\Program Files\Java\jdk1.8.0_65\bin\java" -Didea.launcher.port=7537 "-Didea.launcher.bin.path=C:\Program Files (x86)\JetBrains\IntelliJ IDEA Community Edition 15.0.2\bin" -Dfile.encoding=UTF-8 -classpath "C:\Program Files\Java\jdk1.8.0_65\jre\lib\charsets.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\deploy.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\ext\access-bridge-64.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\ext\cldrdata.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\ext\dnsns.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\ext\jaccess.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\ext\jfxrt.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\ext\localedata.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\ext\nashorn.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\ext\sunec.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\ext\sunjce_provider.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\ext\sunmscapi.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\ext\sunpkcs11.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\ext\zipfs.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\javaws.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\jce.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\jfr.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\jfxswt.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\jsse.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\management-agent.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\plugin.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\resources.jar;C:\Program Files\Java\jdk1.8.0_65\jre\lib\rt.jar;C:\Users\juanrh\git\flink-state-eviction\target\classes;C:\Users\juanrh\.m2\repository\org\apache\flink\flink-java\1.1.3\flink-java-1.1.3.jar;C:\Users\juanrh\.m2\repository\org\apache\flink\flink-core\1.1.3\flink-core-1.1.3.jar;C:\Users\juanrh\.m2\repository\org\apache\flink\flink-annotations\1.1.3\flink-annotations-1.1.3.jar;C:\Users\juanrh\.m2\repository\org\apache\flink\flink-metrics-core\1.1.3\flink-metrics-core-1.1.3.jar;C:\Users\juanrh\.m2\repository\com\esotericsoftware\kryo\kryo\2.24.0\kryo-2.24.0.jar;C:\Users\juanrh\.m2\repository\com\esotericsoftware\minlog\minlog\1.2\minlog-1.2.jar;C:\Users\juanrh\.m2\repository\org\objenesis\objenesis\2.1\objenesis-2.1.jar;C:\Users\juanrh\.m2\repository\org\apache\avro\avro\1.7.6\avro-1.7.6.jar;C:\Users\juanrh\.m2\repository\org\apache\flink\flink-shaded-hadoop2\1.1.3\flink-shaded-hadoop2-1.1.3.jar;C:\Users\juanrh\.m2\repository\xmlenc\xmlenc\0.52\xmlenc-0.52.jar;C:\Users\juanrh\.m2\repository\commons-codec\commons-codec\1.4\commons-codec-1.4.jar;C:\Users\juanrh\.m2\repository\commons-io\commons-io\2.4\commons-io-2.4.jar;C:\Users\juanrh\.m2\repository\commons-net\commons-net\3.1\commons-net-3.1.jar;C:\Users\juanrh\.m2\repository\commons-collections\commons-collections\3.2.1\commons-collections-3.2.1.jar;C:\Users\juanrh\.m2\repository\javax\servlet\servlet-api\2.5\servlet-api-2.5.jar;C:\Users\juanrh\.m2\repository\org\mortbay\jetty\jetty-util\6.1.26\jetty-util-6.1.26.jar;C:\Users\juanrh\.m2\repository\com\sun\jersey\jersey-core\1.9\jersey-core-1.9.jar;C:\Users\juanrh\.m2\repository\commons-el\commons-el\1.0\commons-el-1.0.jar;C:\Users\juanrh\.m2\repository\commons-logging\commons-logging\1.1.3\commons-logging-1.1.3.jar;C:\Users\juanrh\.m2\repository\com\jamesmurty\utils\java-xmlbuilder\0.4\java-xmlbuilder-0.4.jar;C:\Users\juanrh\.m2\repository\commons-lang\commons-lang\2.6\commons-lang-2.6.jar;C:\Users\juanrh\.m2\repository\commons-configuration\commons-configuration\1.7\commons-configuration-1.7.jar;C:\Users\juanrh\.m2\repository\commons-digester\commons-digester\1.8.1\commons-digester-1.8.1.jar;C:\Users\juanrh\.m2\repository\org\codehaus\jackson\jackson-core-asl\1.8.8\jackson-core-asl-1.8.8.jar;C:\Users\juanrh\.m2\repository\org\codehaus\jackson\jackson-mapper-asl\1.8.8\jackson-mapper-asl-1.8.8.jar;C:\Users\juanrh\.m2\repository\com\thoughtworks\paranamer\paranamer\2.3\paranamer-2.3.jar;C:\Users\juanrh\.m2\repository\org\xerial\snappy\snappy-java\1.0.5\snappy-java-1.0.5.jar;C:\Users\juanrh\.m2\repository\com\jcraft\jsch\0.1.42\jsch-0.1.42.jar;C:\Users\juanrh\.m2\repository\org\apache\zookeeper\zookeeper\3.4.6\zookeeper-3.4.6.jar;C:\Users\juanrh\.m2\repository\io\netty\netty\3.7.0.Final\netty-3.7.0.Final.jar;C:\Users\juanrh\.m2\repository\org\apache\commons\commons-compress\1.4.1\commons-compress-1.4.1.jar;C:\Users\juanrh\.m2\repository\org\tukaani\xz\1.0\xz-1.0.jar;C:\Users\juanrh\.m2\repository\commons-beanutils\commons-beanutils-bean-collections\1.8.3\commons-beanutils-bean-collections-1.8.3.jar;C:\Users\juanrh\.m2\repository\commons-daemon\commons-daemon\1.0.13\commons-daemon-1.0.13.jar;C:\Users\juanrh\.m2\repository\javax\xml\bind\jaxb-api\2.2.2\jaxb-api-2.2.2.jar;C:\Users\juanrh\.m2\repository\javax\xml\stream\stax-api\1.0-2\stax-api-1.0-2.jar;C:\Users\juanrh\.m2\repository\javax\activation\activation\1.1\activation-1.1.jar;C:\Users\juanrh\.m2\repository\com\google\inject\guice\3.0\guice-3.0.jar;C:\Users\juanrh\.m2\repository\javax\inject\javax.inject\1\javax.inject-1.jar;C:\Users\juanrh\.m2\repository\aopalliance\aopalliance\1.0\aopalliance-1.0.jar;C:\Users\juanrh\.m2\repository\org\apache\commons\commons-math3\3.5\commons-math3-3.5.jar;C:\Users\juanrh\.m2\repository\com\google\code\findbugs\jsr305\1.3.9\jsr305-1.3.9.jar;C:\Users\juanrh\.m2\repository\org\apache\commons\commons-lang3\3.3.2\commons-lang3-3.3.2.jar;C:\Users\juanrh\.m2\repository\org\slf4j\slf4j-api\1.7.7\slf4j-api-1.7.7.jar;C:\Users\juanrh\.m2\repository\org\slf4j\slf4j-log4j12\1.7.7\slf4j-log4j12-1.7.7.jar;C:\Users\juanrh\.m2\repository\log4j\log4j\1.2.17\log4j-1.2.17.jar;C:\Users\juanrh\.m2\repository\org\apache\flink\force-shading\1.1.3\force-shading-1.1.3.jar;C:\Users\juanrh\.m2\repository\org\apache\flink\flink-streaming-java_2.10\1.1.3\flink-streaming-java_2.10-1.1.3.jar;C:\Users\juanrh\.m2\repository\org\apache\flink\flink-runtime_2.10\1.1.3\flink-runtime_2.10-1.1.3.jar;C:\Users\juanrh\.m2\repository\io\netty\netty-all\4.0.27.Final\netty-all-4.0.27.Final.jar;C:\Users\juanrh\.m2\repository\org\javassist\javassist\3.18.2-GA\javassist-3.18.2-GA.jar;C:\Users\juanrh\.m2\repository\org\scala-lang\scala-library\2.10.4\scala-library-2.10.4.jar;C:\Users\juanrh\.m2\repository\com\typesafe\akka\akka-actor_2.10\2.3.7\akka-actor_2.10-2.3.7.jar;C:\Users\juanrh\.m2\repository\com\typesafe\config\1.2.1\config-1.2.1.jar;C:\Users\juanrh\.m2\repository\com\typesafe\akka\akka-remote_2.10\2.3.7\akka-remote_2.10-2.3.7.jar;C:\Users\juanrh\.m2\repository\com\google\protobuf\protobuf-java\2.5.0\protobuf-java-2.5.0.jar;C:\Users\juanrh\.m2\repository\org\uncommons\maths\uncommons-maths\1.2.2a\uncommons-maths-1.2.2a.jar;C:\Users\juanrh\.m2\repository\com\typesafe\akka\akka-slf4j_2.10\2.3.7\akka-slf4j_2.10-2.3.7.jar;C:\Users\juanrh\.m2\repository\org\clapper\grizzled-slf4j_2.10\1.0.2\grizzled-slf4j_2.10-1.0.2.jar;C:\Users\juanrh\.m2\repository\com\github\scopt\scopt_2.10\3.2.0\scopt_2.10-3.2.0.jar;C:\Users\juanrh\.m2\repository\io\dropwizard\metrics\metrics-core\3.1.0\metrics-core-3.1.0.jar;C:\Users\juanrh\.m2\repository\io\dropwizard\metrics\metrics-jvm\3.1.0\metrics-jvm-3.1.0.jar;C:\Users\juanrh\.m2\repository\io\dropwizard\metrics\metrics-json\3.1.0\metrics-json-3.1.0.jar;C:\Users\juanrh\.m2\repository\com\fasterxml\jackson\core\jackson-databind\2.4.2\jackson-databind-2.4.2.jar;C:\Users\juanrh\.m2\repository\com\fasterxml\jackson\core\jackson-annotations\2.4.0\jackson-annotations-2.4.0.jar;C:\Users\juanrh\.m2\repository\com\fasterxml\jackson\core\jackson-core\2.4.2\jackson-core-2.4.2.jar;C:\Users\juanrh\.m2\repository\com\twitter\chill_2.10\0.7.4\chill_2.10-0.7.4.jar;C:\Users\juanrh\.m2\repository\com\twitter\chill-java\0.7.4\chill-java-0.7.4.jar;C:\Users\juanrh\.m2\repository\org\apache\sling\org.apache.sling.commons.json\2.0.6\org.apache.sling.commons.json-2.0.6.jar;C:\Users\juanrh\.m2\repository\org\apache\flink\flink-clients_2.10\1.1.3\flink-clients_2.10-1.1.3.jar;C:\Users\juanrh\.m2\repository\org\apache\flink\flink-optimizer_2.10\1.1.3\flink-optimizer_2.10-1.1.3.jar;C:\Users\juanrh\.m2\repository\commons-cli\commons-cli\1.3.1\commons-cli-1.3.1.jar;C:\Users\juanrh\.m2\repository\org\apache\flink\flink-streaming-contrib_2.10\1.1.3\flink-streaming-contrib_2.10-1.1.3.jar;C:\Users\juanrh\.m2\repository\org\apache\flink\flink-streaming-scala_2.10\1.1.3\flink-streaming-scala_2.10-1.1.3.jar;C:\Users\juanrh\.m2\repository\org\apache\flink\flink-scala_2.10\1.1.3\flink-scala_2.10-1.1.3.jar;C:\Users\juanrh\.m2\repository\org\scalamacros\quasiquotes_2.10\2.0.1\quasiquotes_2.10-2.0.1.jar;C:\Users\juanrh\.m2\repository\org\scala-lang\scala-reflect\2.10.4\scala-reflect-2.10.4.jar;C:\Users\juanrh\.m2\repository\org\scala-lang\scala-compiler\2.10.4\scala-compiler-2.10.4.jar;C:\Users\juanrh\.m2\repository\org\apache\flink\flink-test-utils_2.10\1.1.3\flink-test-utils_2.10-1.1.3.jar;C:\Users\juanrh\.m2\repository\org\apache\flink\flink-test-utils-junit\1.1.3\flink-test-utils-junit-1.1.3.jar;C:\Users\juanrh\.m2\repository\org\apache\curator\curator-test\2.8.0\curator-test-2.8.0.jar;C:\Users\juanrh\.m2\repository\org\apache\commons\commons-math\2.2\commons-math-2.2.jar;C:\Users\juanrh\.m2\repository\com\google\guava\guava\16.0.1\guava-16.0.1.jar;C:\Users\juanrh\.m2\repository\org\hamcrest\hamcrest-all\1.3\hamcrest-all-1.3.jar;C:\Program Files (x86)\JetBrains\IntelliJ IDEA Community Edition 15.0.2\lib\idea_rt.jar" com.intellij.rt.execution.application.AppMain com.github.juanrh.streaming.MapWithStateIterPoC
16/11/20 22:16:07 WARN InstanceConnectionInfo: No hostname could be resolved for the IP address 127.0.0.1, using IP address as host name. Local input split assignment (such as for HDFS files) may be impacted.
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsOut'. Metric will not be reported. (null)
16/11/20 22:16:07 WARN MetricGroup: Name collision: Group already contains a Metric with the name 'numRecordsIn'. Metric will not be reported. (null)
1> input: (a,2)
2> eitherInputOrTombstoneIter: Left((a,2))
16/11/20 22:16:07 WARN MapWithStateIterPoC: Scheduling the shipment of a tombstone for key a
2> out: (a,2)
2> trans: Left((a,2))
2> input: (a,3)
2> eitherInputOrTombstoneIter: Left((a,3))
2> out: (a,5)
2> trans: Left((a,5))
2> trans: Right(a)
16/11/20 22:16:08 WARN MapWithStateIterPoC: Sending a tombstone for key a
16/11/20 22:16:08 WARN MapWithStateIterPoC: Received a tombstone for key a
16/11/20 22:16:08 WARN MapWithStateIterPoC: Scheduling the shipment of a tombstone for key a
2> eitherInputOrTombstoneIter: Right(a)
3> input: (b,1)
7> eitherInputOrTombstoneIter: Left((b,1))
7> out: (b,1)
7> trans: Left((b,1))
16/11/20 22:16:08 WARN MapWithStateIterPoC: Scheduling the shipment of a tombstone for key b
16/11/20 22:16:08 WARN MapWithStateIterPoC: Sending a tombstone for key a
2> trans: Right(a)
16/11/20 22:16:09 WARN MapWithStateIterPoC: Received a tombstone for key a
16/11/20 22:16:09 WARN MapWithStateIterPoC: Scheduling the shipment of a tombstone for key a
2> eitherInputOrTombstoneIter: Right(a)
4> input: (c,5)
5> input: (d,2)
7> trans: Right(b)
16/11/20 22:16:09 WARN MapWithStateIterPoC: Sending a tombstone for key b
7> eitherInputOrTombstoneIter: Left((c,5))
7> out: (c,5)
7> trans: Left((c,5))
16/11/20 22:16:09 WARN MapWithStateIterPoC: Scheduling the shipment of a tombstone for key c
1> eitherInputOrTombstoneIter: Left((d,2))
16/11/20 22:16:09 WARN MapWithStateIterPoC: Scheduling the shipment of a tombstone for key d
1> out: (d,2)
1> trans: Left((d,2))
7> eitherInputOrTombstoneIter: Right(b)
16/11/20 22:16:09 WARN MapWithStateIterPoC: Received a tombstone for key b
16/11/20 22:16:09 WARN MapWithStateIterPoC: Scheduling the shipment of a tombstone for key b
2> trans: Right(a)
16/11/20 22:16:09 WARN MapWithStateIterPoC: Sending a tombstone for key a
16/11/20 22:16:09 WARN MapWithStateIterPoC: Received a tombstone for key a
16/11/20 22:16:09 WARN MapWithStateIterPoC: Evicted state for key a
2> eitherInputOrTombstoneIter: Right(a)
16/11/20 22:16:09 WARN MapWithStateIterPoC: Sending a tombstone for key c
7> trans: Right(c)
16/11/20 22:16:09 WARN MapWithStateIterPoC: Sending a tombstone for key d
1> trans: Right(d)
16/11/20 22:16:09 WARN MapWithStateIterPoC: Sending a tombstone for key b
7> trans: Right(b)
16/11/20 22:16:10 WARN MapWithStateIterPoC: Received a tombstone for key d
16/11/20 22:16:10 WARN MapWithStateIterPoC: Scheduling the shipment of a tombstone for key d
16/11/20 22:16:10 WARN MapWithStateIterPoC: Received a tombstone for key c
16/11/20 22:16:10 WARN MapWithStateIterPoC: Scheduling the shipment of a tombstone for key c
7> eitherInputOrTombstoneIter: Right(c)
1> eitherInputOrTombstoneIter: Right(d)
7> eitherInputOrTombstoneIter: Right(b)
16/11/20 22:16:10 WARN MapWithStateIterPoC: Received a tombstone for key b
16/11/20 22:16:10 WARN MapWithStateIterPoC: Evicted state for key b
16/11/20 22:16:10 WARN ElementsWithGapsSource: Source stopped
6> input: (a,3)
2> eitherInputOrTombstoneIter: Left((a,3))
2> out: (a,3)
2> trans: Left((a,3))
16/11/20 22:16:10 WARN MapWithStateIterPoC: Scheduling the shipment of a tombstone for key a
1> trans: Right(d)
16/11/20 22:16:10 WARN MapWithStateIterPoC: Sending a tombstone for key d
16/11/20 22:16:10 WARN MapWithStateIterPoC: Sending a tombstone for key c
7> trans: Right(c)
16/11/20 22:16:10 WARN MapWithStateIterPoC: Received a tombstone for key d
7> eitherInputOrTombstoneIter: Right(c)
16/11/20 22:16:10 WARN MapWithStateIterPoC: Received a tombstone for key c
16/11/20 22:16:10 WARN MapWithStateIterPoC: Evicted state for key d
1> eitherInputOrTombstoneIter: Right(d)
16/11/20 22:16:10 WARN MapWithStateIterPoC: Evicted state for key c
2> trans: Right(a)
16/11/20 22:16:10 WARN MapWithStateIterPoC: Sending a tombstone for key a
2> eitherInputOrTombstoneIter: Right(a)
16/11/20 22:16:10 WARN MapWithStateIterPoC: Received a tombstone for key a
16/11/20 22:16:10 WARN MapWithStateIterPoC: Scheduling the shipment of a tombstone for key a
16/11/20 22:16:11 WARN MapWithStateIterPoC: Sending a tombstone for key a
2> trans: Right(a)
16/11/20 22:16:11 WARN MapWithStateIterPoC: Received a tombstone for key a
2> eitherInputOrTombstoneIter: Right(a)
16/11/20 22:16:11 WARN MapWithStateIterPoC: Evicted state for key a

Had to manually stop this
* */
public class MapWithStateIterPoC {
    private static final Logger LOG = LoggerFactory.getLogger(MapWithStateIterPoC.class);

    @Data
    public static class TimeStampedValue<T> implements Serializable {
        private static final long serialVersionUID = 1L;

        @NonNull private T value;
        /** millis since UNIX epoch like in System.currentTimeMillis */
        private long lastAccessTimestamp;
        private boolean isTombstoneSent;

        public TimeStampedValue(@NonNull T value) {
            this.value = value;
            this.lastAccessTimestamp = 0;
            this.isTombstoneSent = false;
        }
    }

    public static void main(String [] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(8);
                        //.getExecutionEnvironment();
        env.setStateBackend(new MemoryStateBackend());
        env.getConfig().disableSysoutLogging();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        SourceFunction<Tuple2<String, Integer>> source =
                ElementsWithGapsSource
                        .addElem(Tuple2.of("a", 2)).addGap(Time.milliseconds(500))
                        .addElem(Tuple2.of("a", 3)).addGap(Time.milliseconds(500))
                        .addElem(Tuple2.of("b", 1)).addGap(Time.milliseconds(500))
                        .addElem(Tuple2.of("c", 5)).addElem(Tuple2.of("d", 2)).addGap(Time.seconds(1))
                        .addElem(Tuple2.of("a", 3)).build();

        DataStream<Tuple2<String, Integer>> input = env.addSource(source);

        final Time ttl = Time.milliseconds(1100); // how long a state key will last (approx)
        final Time ttlRefreshInterval = Time.milliseconds(450); // how often access to each state key will be checked (approx)
        // to avoid non serializable exception in main rich function
        final long ttlMillis = ttl.toMilliseconds();
        final long ttlRefreshIntervalMillis = ttlRefreshInterval.toMilliseconds();

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = input.keyBy(0);
        DataStream<Either<Tuple2<String, Integer>, String>> eitherInputOrTombstone =
                keyed.map(new MapFunction<Tuple2<String,Integer>, Either<Tuple2<String, Integer>, String>>() {
                    @Override
                    public Either<Tuple2<String, Integer>, String> map(Tuple2<String, Integer> stringInt) throws Exception {
                        return Either.Left(stringInt);
                    }
                });
        IterativeStream<Either<Tuple2<String,Integer>, String>> eitherInputOrTombstoneIter =
                eitherInputOrTombstone.iterate(Time.seconds(5).toMilliseconds());

        DataStream<Either<Tuple2<String,Integer>, String>> trans =
            eitherInputOrTombstoneIter
                    // required to avoid java.lang.RuntimeException: State key serializer has not been configured
                    // in the config. This operation cannot use partitioned state.
                    .keyBy(new KeySelector<Either<Tuple2<String,Integer>,String>, String>() {
                        @Override
                        public String getKey(Either<Tuple2<String, Integer>, String> either) throws Exception {
                            if (either.isLeft()) {
                                return either.left().f0;
                            }
                            return either.right();
                        }
                    })
                    .flatMap(new RichFlatMapFunction<Either<Tuple2<String,Integer>, String>, Either<Tuple2<String,Integer>, String>>() {
                        private final TimeStampedValue<Integer> defaultState = new TimeStampedValue(0);
                        private transient ScheduledExecutorService executor;
                        private transient ValueState<TimeStampedValue<Integer>> valueState;
                        @Override
                        public void open(Configuration config) {
                            ValueStateDescriptor<TimeStampedValue<Integer>> descriptor =
                                    new ValueStateDescriptor<>(
                                            "sumWithTimestamp", // the state name
                                            TypeInformation.of(new TypeHint<TimeStampedValue<Integer>>() {}),
                                            defaultState); // default value of the state
                            valueState = getRuntimeContext().getState(descriptor);
                            executor = Executors.newSingleThreadScheduledExecutor();
                        }

                        private void sendTombstone(final Collector<Either<Tuple2<String,Integer>, String>> collector, final String key) {
                            LOG.warn("Scheduling the shipment of a tombstone for key {}", key);
                            executor.schedule(new Runnable() {
                                @Override
                                public void run() {
                                    LOG.warn("Sending a tombstone for key {}", key);
                                    Either<Tuple2<String,Integer>, String> tombstone = Either.Right(key);
                                    collector.collect(tombstone);
                                }
                            }, ttlRefreshIntervalMillis, TimeUnit.MILLISECONDS);
                        }

                        @Override
                        public void flatMap(Either<Tuple2<String,Integer>, String> either, Collector<Either<Tuple2<String,Integer>, String>> collector) throws Exception {
                            final TimeStampedValue<Integer> state = valueState.value();
                            if (either.isLeft()) {
                                Tuple2<String, Integer> stringInt = either.left();
                                final int currentSum = stringInt.f1 + state.getValue();
                                state.setValue(currentSum); // state business logic
                                state.setLastAccessTimestamp(System.currentTimeMillis()); // update state for the current key was last touched now
                                if (! state.isTombstoneSent()) {
                                    // sent tombstone to the same key: this is only required to send the first tombstone
                                    // FIXME: this only works if we have access to the key!!!
                                    state.setTombstoneSent(true);
                                    sendTombstone(collector, stringInt.f0);
                                }
                                valueState.update(state);
                                Either<Tuple2<String,Integer>, String> result = Either.Left(Tuple2.of(stringInt.f0, currentSum));
                                collector.collect(result);
                            } else {
                                // we just received a tombstone
                                LOG.warn("Received a tombstone for key {}", either.right());
                                long currentTimeMillis = System.currentTimeMillis();
                                if (currentTimeMillis - state.getLastAccessTimestamp() >= ttlMillis) {
                                    // evict and stop sending tombstones for this key
                                    // next time this key appears and it uses the state, the default state
                                    // that makes isTombstoneSent = false will be used, and that will send
                                    // the first tombstone of the new eviction cycle
                                    valueState.clear();
                                    LOG.warn("Evicted state for key " + either.right());
                                } else {
                                    // send another tombstone: the current tombstone prevented sending
                                    // more tombstone for events after the one that sent the current tombstone
                                    sendTombstone(collector, either.right());
                                }
                            }
                        }
                    });

        eitherInputOrTombstoneIter.closeWith(trans.filter(new FilterFunction<Either<Tuple2<String, Integer>, String>>() {
            @Override
            public boolean filter(Either<Tuple2<String, Integer>, String> either) throws Exception {
                return either.isRight();
            }
        }));
        DataStream<Tuple2<String, Integer>> out = trans.flatMap(new FlatMapFunction<Either<Tuple2<String, Integer>, String>, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(Either<Tuple2<String, Integer>, String> tuple2StringEither, Collector<Tuple2<String, Integer>> collector) throws Exception {
                if (tuple2StringEither.isLeft()) {
                    collector.collect(tuple2StringEither.left());
                }
            }
        });
        StreamingUtils.printWithName(input, "input");
        StreamingUtils.printWithName(eitherInputOrTombstoneIter, "eitherInputOrTombstoneIter");
        StreamingUtils.printWithName(trans, "trans");
        StreamingUtils.printWithName(out, "out");

        env.execute("MapWithStateIterPoC");
    }
}

