package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * 从指定 socket 中读取数，并对单词进行计算
 *
 * 1、source 负责数据读取
 * 2、Transformation 负责对数据转换
 * 3、sink 负责最终计算好的结果数据输出
 */

public class StreamingWordCount {


	public static void main(String[] args) throws Exception {


		// 第一版
		StreamingWordCount.first();


		// 第二版
		// StreamingWordCount.second();


	}


	/**
	 * 初始版
	 */
	public static void first() throws Exception
	{
		// 创建 flink 流计算执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 创建 DataStream
		DataStream<String> lines = env.socketTextStream("127.0.0.1", 9999);


		// 调用 transformation 开始

		SingleOutputStreamOperator<String> wordsDataStream = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
				String[] words = line.split(" ");
				for (String word : words) {
					collector.collect(word);
				}

			}
		});

		// 将单词和 1 组合
		SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = wordsDataStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(String word) throws Exception {
				return Tuple2.of(word, 1);
			}
		});


		// 分组
		KeyedStream<Tuple2<String, Integer>, Object> keyed = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
			@Override
			public Object getKey(Tuple2<String, Integer> tp) throws Exception {
				return tp.f0;
			}
		});


		// 聚会
		SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = keyed.sum(1);


		// 调用 transformation 结束



		// 调用 sink
		sumed.print();


		// 启动执行
		env.execute("first");

	}

	/**
	 * 第二版
	 */
  public static void second() throws Exception
  {
	  // 创建执行环境
	  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


	  // 调用 source，指定 socket 地址和端口
	  DataStream<String> lines = env.socketTextStream("127.0.0.1", 9999);


	  //切分压平并将单词和一放入元组中
	  DataStream<Tuple2<String, Integer>> words = lines.
					  flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
						  @Override
						  public void flatMap(String line, Collector<Tuple2<String, Integer>> collector)
										  throws Exception {
							  String[] words = line.split(" ");
							  for (String word : words) {
								  collector.collect(Tuple2.of(word, 1));
							  }
						  }
					  });

	  // 按照key分组并聚合
	  DataStream<Tuple2<String, Integer>> result = words.keyBy(0).sum(1);
	  //将结果打印到控制台
	  result.print();
	  //执行
	  env.execute("second");
  }



}
