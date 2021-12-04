package org.myorg.quickstart.day001;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * flink 中的窗口
 *
 * 1、窗口分为：
 *     a、globalwindow ，按照指定的数据生成一个 Window ，与时间无关
 *     b、timewindow：按照时间生成一个 Window，具体细分为：滚动窗口、滑动窗口、会话窗口
 *
 * 2、non-keyed 和 keyed Window
 *
 * 3、时间类型：process、event 、
 */


public class Demo002 {


	public static void main(String[] args) throws Exception {

		// 数量窗口
		// windowsCount001();

		// reduce
		// windowsCount002();

		// apply 排序
		windowsCount003();

	}


	/**
	 *  计数窗口 countWindow
	 */
	public static void windowsCount001() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 并行度为 1
		DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);


		// 将字符串转换为数字，安全起见，应该有个转换异常
		// 并行度，与计算机核数相关，
		SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);


		// 划分 Windows
		// 并行度：1，只有一个分区，
		AllWindowedStream<Integer, GlobalWindow> windowed = nums.countWindowAll(5);


		SingleOutputStreamOperator<Integer> sum = windowed.sum(0);

		sum.print();

		env.execute("windows");


	}


	/**
	 * reduce 计算和
	 *
	 * 每次都会计算求和，使用中间值保存求和的结果，达到数量触发条件时，进行输出
	 *
	 * ex：本例中以 5 条数据为一个窗口
	 *
	 * 1、在输入 1，2，3，4 时，并不进行输出，只进行求和计算，使用中间变量保存计算结果
	 * 2、当输入 5 时，数量达到触发节点，求和并输出，并且重置中间值
	 *
	 */
	public static void windowsCount002() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 并行度为 1
		DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);


		// 将字符串转换为数字，安全起见，应该有个转换异常
		// 并行度，与计算机核数相关，
		SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);


		// 划分 Windows
		// 并行度：1，只有一个分区，
		AllWindowedStream<Integer, GlobalWindow> windowed = nums.countWindowAll(5);


		SingleOutputStreamOperator<Integer> reduced = windowed.reduce(new ReduceFunction<Integer>() {
			@Override
			public Integer reduce(Integer integer, Integer t1) throws Exception {
				// integer 是中间值，
				// t1 每次输入的值
				System.out.println( "中间值：integer=" + integer + "=== t1=" + t1 );
				return integer + t1;
			}
		});

		reduced.print();


		env.execute("windows");

	}

	/**
	 * Windows 全量聚合，排序
	 * 不分组，划分窗口，调用 apply 对窗口内的数据进行处理
	 *
	 * 将窗口内数据先存起来，存在 Window state 中，当满足条件后，在将状态中的数据取出来进行计算
	 *
	 */
	public static void windowsCount003() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 并行度为 1
		DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);


		// 将字符串转换为数字，安全起见，应该有个转换异常
		// 并行度，与计算机核数相关，
		SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);


		// 划分 Windows
		// 并行度：1，只有一个分区，
		AllWindowedStream<Integer, GlobalWindow> windowed = nums.countWindowAll(5);

		SingleOutputStreamOperator<Object> sorted = windowed.apply(new AllWindowFunction<Integer, Object, GlobalWindow>() {

			/**
			 * 当输入数据满足 5 条之后，会执行一次
			 * @param globalWindow 窗口类，一些窗口的数据
			 * @param iterable     所有输入的数据
			 * @param collector    输出
			 * @throws Exception
			 */
			@Override
			public void apply(GlobalWindow globalWindow, Iterable<Integer> iterable, Collector<Object> collector) throws Exception {

				ArrayList<Integer> list = new ArrayList<>();

				System.out.println("======开始循环了======");
				for (Integer value : iterable) {
					System.out.println("数据：" + value);
					list.add( value );
				}

				list.sort((o1,o2) -> { return o1 - o2; });

				// 输出
				System.out.println("本次输出：");
				for (Integer i : list) {
					System.out.println("数据：" + i);
					collector.collect(i);
				}

			}
		});

		sorted.setParallelism(1); // 怎么不管用
		sorted.print();

		env.execute("windows");
	}



}
