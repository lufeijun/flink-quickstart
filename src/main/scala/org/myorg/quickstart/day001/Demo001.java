package org.myorg.quickstart.day001;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo001 {


	public static void main(String[] args) throws Exception {

		// map
		myMap();

	}


	/**
	 * map 函数
	 */
	public static void myMap() throws Exception{

		//
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> words = env.socketTextStream("localhost", 9999);

		// 转换处理
		SingleOutputStreamOperator<String> word = words.map(new MapFunction<String, String>() {

			@Override
			public String map(String s) throws Exception {
				return s.toUpperCase();
			}
		});


		word.print();

		env.execute("map");

	}


}
