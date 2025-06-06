/*
 * Copyright 2018 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * The "Hourly Tips" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class HourlyTipsExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		final String inputPath = params.get("input", ExerciseBase.pathToFareData);

		final int maxDelay = 60;       // events are out of order by max 60 seconds
		final int speedFactor = 600;   // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// create data source
		DataStream<TaxiFare> fares = env.addSource(
				fareSourceOrTest(new TaxiFareSource(inputPath, maxDelay, speedFactor)));

		// pipeline: sum tips per driver per hour, then find hourly max
		DataStream<Tuple3<Long, Long, Float>> hourlyMaxTips = fares
				.keyBy(fare -> fare.driverId)
				.timeWindow(Time.hours(1))
				.process(new ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow>() {
					@Override
					public void process(Long driverId, Context context, Iterable<TaxiFare> fares,
										Collector<Tuple3<Long, Long, Float>> out) {
						float totalTips = 0F;
						for (TaxiFare fare : fares) {
							totalTips += fare.tip;
						}
						out.collect(new Tuple3<>(context.window().getEnd(), driverId, totalTips));
					}
				})
				.timeWindowAll(Time.hours(1))
				.maxBy(2); // field 2 = total tips

		// output result
		printOrTest(hourlyMaxTips);

		// trigger execution
		env.execute("Hourly Tips (java)");
	}
}
