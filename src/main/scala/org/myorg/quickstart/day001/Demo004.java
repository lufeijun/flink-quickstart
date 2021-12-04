package org.myorg.quickstart.day001;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo004 {

	/**
	 * event 类型的时间窗口
	 */
	public static void main(String[] args) {
		System.out.println(111);
	}



	public static void eventTimeWindow() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

		SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);




		lines.print();

		env.execute();

	}

}
