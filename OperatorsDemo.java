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

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.security.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class OperatorsDemo {

    public static class Feature {
        public String getFeatureName() {
            return featureName;
        }

        public void setFeatureName(String featureName) {
            this.featureName = featureName;
        }

        String featureName;

        public Integer getValues() {
            return values;
        }

        public void setValues(Integer values) {
            this.values = values;
        }

        Integer values;

        public String toString() {
            return values.toString();
        }
    }

    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        final String hostname = "192.168.7.196";

        Configuration conf = new Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.enableCheckpointing(1000);

        //        env.socketTextStream(hostname,9000,"\n")
        //                .map((value) -> {
        //                    return Math.pow(Integer.parseInt(value), 2);
        //                })
        //                .print();

        //        env.socketTextStream(hostname,9000,"\n")
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

        //        env.socketTextStream(hostname,9000,"\n")
        //                .map((value) -> {
        //                    Tuple2<String, String> features = new Tuple2();
        //                    Integer pos = 0;
        //                    for (String feature : value.split(",")) {
        //                        features.setField(feature, pos++);
        //                    }
        //                    return features;
        //                }).returns(Types.TUPLE(Types.STRING, Types.STRING))//.returns(new TypeHint<Tuple2<String, String>>(){})
        //                .keyBy((value) -> {
        //                    return value.getField(0);
        //                })
        //                .print();

        //        env.socketTextStream(hostname,9000,"\n")
        //                .map((value) -> {
        //                    Tuple2<Integer, Integer> features = new Tuple2();
        //                    Integer pos = 0;
        //                    for (String feature : value.split(",")) {
        //                        features.setField(Integer.parseInt(feature), pos++);
        //                    }
        //                    return features;
        //                }).returns(new TypeHint<Tuple2<Integer, Integer>>(){})
        //                .timeWindowAll(Time.seconds(5))
        //                .min(0)
        //                .print();

        //        env.socketTextStream(hostname,9000,"\n")
        //                .map((value) -> {
        //                    Tuple2<Integer, Integer> features = new Tuple2();
        //                    Integer pos = 0;
        //                    for (String feature : value.split(",")) {
        //                        features.setField(Integer.parseInt(feature), pos++);
        //                    }
        //                    return features;
        //                }).returns(new TypeHint<Tuple2<Integer, Integer>>(){})
        //                .timeWindowAll(Time.seconds(5))
        //                .apply(new AllWindowFunction<Tuple2<Integer,Integer>, Tuple2<Integer, Integer>, TimeWindow>() {
        //                    @Override
        //                    public void apply (TimeWindow window, Iterable<Tuple2<Integer, Integer>> values, Collector<Tuple2<Integer, Integer>> out) {
        //                        for (Tuple2<Integer, Integer> value : values) {
        //                            out.collect(value);
        //                        }
        //                    }
        //                })
        //                .print();

        //        env.socketTextStream(hostname,9000,"\n")
        //                .split((value) -> {
        //                    List<String> out = new ArrayList<>();
        //                    if (Integer.parseInt(value) % 2 == 0) {
        //                        out.add("even");
        //                    }
        //                    else {
        //                        out.add("odd");
        //                    }
        //                    return out;
        //                }).select("even").union(
        //                        env.socketTextStream(hostname,9001,"\n")
        //                            .split((value) -> {
        //                                List<String> out = new ArrayList<>();
        //                                if (Integer.parseInt(value) % 2 == 0) {
        //                                    out.add("even");
        //                                 } else {
        //                                    out.add("odd");
        //                                }
        //                                return out;
        //                         }).select("odd"))
        //                .print();

        //        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //        env.socketTextStream(hostname,9000,"\n")
        //                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
        //                    @Override
        //                    public long extractTimestamp(String t, long l) {
        //                        return System.currentTimeMillis();
        //                    }
        //                    @Override
        //                    public Watermark getCurrentWatermark() {
        //                        return new Watermark(System.currentTimeMillis());
        //                    }
        //                })
        //                .map((value) -> {
        //                    Tuple2<Integer, Integer> features = new Tuple2();
        //                    Integer pos = 0;
        //                    for (String feature : value.split(",")) {
        //                        features.setField(Integer.parseInt(feature), pos++);
        //                    }
        //                    return features;
        //                }).returns(new TypeHint<Tuple2<Integer, Integer>>(){})
        //                .keyBy(0)
        //                .intervalJoin(
        //                        env.socketTextStream(hostname,9001,"\n")
        //                                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
        //                                    @Override
        //                                    public long extractTimestamp(String t, long l) {
        //                                        return System.currentTimeMillis();
        //                                    }
        //                                    @Override
        //                                    public Watermark getCurrentWatermark() {
        //                                        return new Watermark(System.currentTimeMillis());
        //                                    }
        //                                })
        //                                .map((value) -> {
        //                                    Tuple2<Integer, Integer> features = new Tuple2();
        //                                    Integer pos = 0;
        //                                    for (String feature : value.split(",")) {
        //                                        features.setField(Integer.parseInt(feature), pos++);
        //                                    }
        //                                    return features;
        //                                }).returns(new TypeHint<Tuple2<Integer, Integer>>(){})
        //                                .keyBy(0)
        //                )
        //                .between(Time.seconds(-5), Time.seconds(5))
        //                .process(new ProcessJoinFunction< Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer> >() {
        //                    @Override
        //                    public void processElement(Tuple2<Integer, Integer> leftValue, Tuple2<Integer, Integer> rightValue, Context ctx, Collector<Tuple2<Integer, Integer>> out) {
        //                        out.collect(rightValue);
        //                    }
        //                })
        //                .print();

        //        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //        env.socketTextStream(hostname,9000,"\n")
        //                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
        //                    @Override
        //                    public long extractTimestamp(String t, long l) {
        //                        return System.currentTimeMillis();
        //                    }
        //                    @Override
        //                    public Watermark getCurrentWatermark() {
        //                        return new Watermark(System.currentTimeMillis());
        //                    }
        //                })
        //                .map((value) -> {
        //                    Tuple2<String, String> features = new Tuple2();
        //                    Integer pos = 0;
        //                    for (String feature : value.split(",")) {
        //                        features.setField(feature, pos++);
        //                    }
        //                    return features;
        //                }).returns(new TypeHint<Tuple2<String, String>>(){})
        //                .join(env.socketTextStream(hostname,9001,"\n")
        //                        .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
        //                            @Override
        //                            public long extractTimestamp(String t, long l) {
        //                                return System.currentTimeMillis();
        //                            }
        //                            @Override
        //                            public Watermark getCurrentWatermark() {
        //                                return new Watermark(System.currentTimeMillis());
        //                            }
        //                        })
        //                        .map((value) -> {
        //                            Tuple2<String, String> features = new Tuple2();
        //                            Integer pos = 0;
        //                            for (String feature : value.split(",")) {
        //                                features.setField(feature, pos++);
        //                            }
        //                            return features;
        //                        }).returns(new TypeHint<Tuple2<String, String>>(){}))
        //                .where(in -> {
        //                    return in.getField(0);
        //                }).equalTo(in -> {
        //                    return in.getField(0);
        //                })
        //                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
        //                .apply(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple3<String, String, String>>() {
        //                    @Override
        //                    public Tuple3<String, String, String> join(Tuple2<String, String> leftValue, Tuple2<String, String> rightValue) {
        //                        return new Tuple3(leftValue.getField(0), leftValue.getField(1), rightValue.getField(1));
        //                    }
        //                })
        //                .print();

        //        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //        env.socketTextStream(hostname,9000,"\n")
        //                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
        //                    @Override
        //                    public long extractTimestamp(String t, long l) {
        //                        return System.currentTimeMillis();
        //                    }
        //                    @Override
        //                    public Watermark getCurrentWatermark() {
        //                        return new Watermark(System.currentTimeMillis());
        //                    }
        //                })
        //                .map((value) -> {
        //                    Tuple2<String, String> features = new Tuple2();
        //                    Integer pos = 0;
        //                    for (String feature : value.split(",")) {
        //                        features.setField(feature, pos++);
        //                    }
        //                    return features;
        //                }).returns(new TypeHint<Tuple2<String, String>>(){})
        //                .coGroup(env.socketTextStream(hostname,9001,"\n")
        //                        .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
        //                            @Override
        //                            public long extractTimestamp(String t, long l) {
        //                                return System.currentTimeMillis();
        //                            }
        //                            @Override
        //                            public Watermark getCurrentWatermark() {
        //                                return new Watermark(System.currentTimeMillis());
        //                            }
        //                        })
        //                        .map((value) -> {
        //                            Tuple2<String, String> features = new Tuple2();
        //                            Integer pos = 0;
        //                            for (String feature : value.split(",")) {
        //                                features.setField(feature, pos++);
        //                            }
        //                            return features;
        //                        }).returns(new TypeHint<Tuple2<String, String>>(){}))
        //                .where(in -> {
        //                    return in.getField(0);
        //                }).equalTo(in -> {
        //                    return in.getField(0);
        //                })
        //                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
        //                .apply(new CoGroupFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>>() {
        //                    @Override
        //                    public void coGroup(Iterable<Tuple2<String, String>> leftValues, Iterable<Tuple2<String, String>> rightValues, Collector<Tuple2<String, String>> out) {
        //                        for (Tuple2<String, String> values: leftValues) {
        //                            out.collect(values);
        //                        }
        //                        for (Tuple2<String, String> values: rightValues) {
        //                            out.collect(values);
        //                        }
        //                    }
        //                })
        //                .print();

        env.socketTextStream(hostname,9000,"\n")
                .map(value -> Integer.parseInt(value))
                .connect(env.socketTextStream(hostname,9001,"\n"))
                .map(new CoMapFunction<Integer, String, String>() {
                        public String map1(Integer in1) {
                            return String.valueOf(in1+1);
                        }
                        public String map2(String in2) {
                            return in2;
                        }
                })
                .print();

        //        DataStream<Integer> inputStream = env.socketTextStream(hostname,9000,"\n").map(value -> Integer.parseInt(value));
        //        IterativeStream<Integer> iteration = inputStream.iterate();
        //        DataStream<Integer> iteratedStream = iteration.map(value -> value + 1);
        //        DataStream<Integer> feedbackStream = iteratedStream.filter(value -> {return value % 2 == 0;});
        //        iteration.closeWith(feedbackStream);
        //        iteration.print();

        env.execute("OperatorsDemo");
    }
}