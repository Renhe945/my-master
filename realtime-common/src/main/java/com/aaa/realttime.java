package com.aaa;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class realttime {
    @SneakyThrows
    public static void main(String[] args) {
//        StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.socketTextStream(  "hadoop102", 9999);
        streamSource.print();
        env.execute();
    }
}
/**
 * ./flink run-application -d -t yarn-application -yjm 900 -ynm flink-test -ytm 900 -yqu root.default -c com.aaa.realttime /root/lx/original-realtime-common-1.0-SNAPSHOT.jar
 * 2024-12-17 20:47:38,264 WARN  org.apache.flink.yarn.configuration.YarnLogConfigUtil        [] - The configuration directory ('/opt/module/flink/conf') already contains a LOG4J config file.If you want to use logback, then please delete or rename the log configuration file.
 * 2024-12-17 20:47:38,400 INFO  org.apache.hadoop.yarn.client.DefaultNoHARMFailoverProxyProvider [] - Connecting to ResourceManager at hadoop103/192.168.10.103:8032
 * 2024-12-17 20:47:39,027 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - No path for the flink jar passed. Using the location of class org.apache.flink.yarn.YarnClusterDescriptor to locate the jar
 * 2024-12-17 20:47:39,499 INFO  org.apache.hadoop.conf.Configuration                         [] - resource-types.xml not found
 * 2024-12-17 20:47:39,499 INFO  org.apache.hadoop.yarn.util.resource.ResourceUtils           [] - Unable to find 'resource-types.xml'.
 * 2024-12-17 20:47:39,801 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - The configured JobManager memory is 1600 MB. YARN will allocate 2048 MB to make up an integer multiple of its minimum allocation memory (512 MB, configured via 'yarn.scheduler.minimum-allocation-mb'). The extra 448 MB may not be used by Flink.
 * 2024-12-17 20:47:39,801 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - The configured TaskManager memory is 1728 MB. YARN will allocate 2048 MB to make up an integer multiple of its minimum allocation memory (512 MB, configured via 'yarn.scheduler.minimum-allocation-mb'). The extra 320 MB may not be used by Flink.
 * 2024-12-17 20:47:39,801 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - Cluster specification: ClusterSpecification{masterMemoryMB=1600, taskManagerMemoryMB=1728, slotsPerTaskManager=1}
 * 2024-12-17 20:47:48,179 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - Removing 'localhost' Key: 'jobmanager.bind-host' , default: null (fallback keys: []) setting from effective configuration; using '0.0.0.0' instead.
 * 2024-12-17 20:47:48,180 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - Removing 'localhost' Key: 'taskmanager.bind-host' , default: null (fallback keys: []) setting from effective configuration; using '0.0.0.0' instead.
 * 2024-12-17 20:47:48,239 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - Cannot use kerberos delegation token manager, no valid kerberos credentials provided.
 * 2024-12-17 20:47:48,244 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - Submitting application master application_1734401429095_0001
 * 2024-12-17 20:47:49,033 INFO  org.apache.hadoop.yarn.client.api.impl.YarnClientImpl        [] - Submitted application application_1734401429095_0001
 * 2024-12-17 20:47:49,033 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - Waiting for the cluster to be allocated
 * 2024-12-17 20:47:49,103 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - Deploying cluster, current state ACCEPTED
 * 2024-12-17 20:48:14,419 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - YARN application has been deployed successfully.
 * 2024-12-17 20:48:14,419 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - Found Web Interface hadoop103:37746 of application 'application_1734401429095_0001'.
 */