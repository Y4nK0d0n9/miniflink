/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class OperatorsDemo {
    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        final String hostname = "192.168.8.238";

        Configuration conf = new Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

//        env.socketTextStream(hostname,9000,"\n")
//                .map((value) -> {
//                    return Math.pow(Integer.parseInt(value), 2);
//                })
//                .print();

//        env.socketTextStream(hostname,9001,"\n")
//                .flatMap((String value, Collector<Integer> tmp) -> {
//                    for (String intStr : value.split("\\s")) {
//                        tmp.collect(Integer.parseInt(intStr) + 1);
//                    }
//                }).returns(Integer.class)
//                .print();

//        env.socketTextStream(hostname,9000,"\n")
//                .filter((value) -> {
//                    return Integer.parseInt(value) < 10;
//                }
//                .print();

//        env.socketTextStream(hostname,9000,"\n")
//                .map((value) -> {
//                    Feature col = new Feature();
//                    col.setFeatureName("x1");
//                    col.setValues(Integer.parseInt(value));
//                    return col;
//                })
//                .keyBy("featureName")
//                .print();

//        env.socketTextStream(hostname,9000,"\n")
//                .map((value) -> {
//                    Tuple2<String, String> features = new Tuple2();
//                    Integer pos = 0;
//                    for (String feature : value.split(",")) {
//                        features.setField(feature, pos++);
//                    }
//                    return features;
//                }).returns(new TypeHint<Tuple2<String, String>>(){})
//                .keyBy(0)
//                .print();

        env.socketTextStream(hostname,9000,"\n")
                .map((value) -> {
                    Tuple2<String, String> features = new Tuple2();
                    Integer pos = 0;
                    for (String feature : value.split(",")) {
                        features.setField(feature, pos++);
                    }
                    return features;
                }).returns(new TypeHint<Tuple2<String, String>>(){})
                .keyBy((value) -> {
                    return value.getField(0);
                })
                .print();

        env.execute("OperatorsDemo");
    }

//    public static class Feature {
//        public String getFeatureName() {
//            return featureName;
//        }
//
//        public void setFeatureName(String featureName) {
//            this.featureName = featureName;
//        }
//
//        String featureName;
//
//        public Integer getValues() {
//            return values;
//        }
//
//        public void setValues(Integer values) {
//            this.values = values;
//        }
//
//        Integer values;
//
//        public String toString() {
//            return values.toString();
//        }
//    }
}

