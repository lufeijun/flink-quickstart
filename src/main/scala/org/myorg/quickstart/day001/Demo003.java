package org.myorg.quickstart.day001;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Demo003 {

	/**
	 * 时间窗口
	 *
	 */

	public static void main(String[] args) throws Exception {

		// non-keyed 滚动窗口
		// timeWindow001();

		// keyed 滚动窗口
		// timeWindow002();

		// non-keyed 滑动窗口
		// timeWindow003();

		// keyed 滑动窗口
		 timeWindow004();


		// non-keyed 会话窗口
		// timeWindow005();

		// keyed 会话窗口
		// timeWindow006();

	}


	/**
	 * 滚动窗口
	 *
	 * 1、不分组 non-keyed 的窗口
	 *
	 * 不要调用 keyby，然后调用 windowall ，传入 windowAssigner
	 *
	 */
	public static void timeWindow001() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);


		SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);


		// 全局窗口
		AllWindowedStream<Integer, TimeWindow> integerTimeWindowAllWindowedStream = nums.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)));


		integerTimeWindowAllWindowedStream.sum(0).print();



		env.execute();

	}


	/**
	 * 滚动窗口
	 *
	 * keyby
	 *
	 * 先 keyby，然后调用 window ，传入 windowAssigner
	 */

	public static void timeWindow002() throws Exception {
		System.out.println(" timeWindow002 ");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);


		SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(String s) throws Exception {
				String[] fields = s.split(",");
				return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
			}
		});
		
		// 调用 keyBy
		KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndCount.keyBy(f -> f.f0);

		// 划分窗口
		WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowed = keyed.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

		// 直接求和
		//windowed.sum(1).print();

		// reduce 处理
		windowed.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
				System.out.println(" reduce 发生了 ");
				stringIntegerTuple2.f1 = stringIntegerTuple2.f1 + t1.f1;
				return stringIntegerTuple2;
			}
		}).print();


		env.execute();

	}


	/**
	 * 滑动窗口
	 *
	 * non-keyed
	 */

	public static void timeWindow003() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

		SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

		AllWindowedStream<Integer, TimeWindow> windowed = nums.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)));

		windowed.sum(0).print();

		env.execute();
	}


	/**
	 * 滑动窗口
	 * keyed
	 */
	public static void timeWindow004() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);


		SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(String s) throws Exception {
				String[] fields = s.split(",");
				return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
			}
		});

		// 调用 keyBy
		KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndCount.keyBy(f -> f.f0);

		// 划分窗口
		WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowed = keyed.window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)));

		// 直接求和
		//windowed.sum(1).print();

		// reduce 处理
		windowed.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
				System.out.println(" reduce 发生了 ");
				System.out.println( stringIntegerTuple2 );
				System.out.println( "=========" );
				System.out.println( t1 );
				stringIntegerTuple2.f1 = stringIntegerTuple2.f1 + t1.f1;
				return stringIntegerTuple2;
			}
		}).print();


		env.execute();
	}


	/**
	 * 会话窗口
	 *
	 */
	public static void timeWindow005() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

		SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

		// 会话，输入停止后，10 s ，进行输出
		nums.windowAll(ProcessingTimeSessionWindows.withGap( Time.seconds(10) )).sum(0).print();

		env.execute();
	}


	/**
	 * 会话窗口
	 */
	public static void timeWindow006() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);


		SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(String s) throws Exception {
				String[] fields = s.split(",");
				return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
			}
		});

		// 调用 keyBy
		KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndCount.keyBy(f -> f.f0);

		// 划分窗口
		WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowed = keyed.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));

		// 直接求和
		//windowed.sum(1).print();

		// reduce 处理
		windowed.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
				System.out.println(" reduce 发生了 ");
				stringIntegerTuple2.f1 = stringIntegerTuple2.f1 + t1.f1;
				return stringIntegerTuple2;
			}
		}).print();


		env.execute();
	}



}
