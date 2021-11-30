package org.myorg.quickstart.day001;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * 各种 sink ，
 * 1、文件 Hadoop
 * 2、kafka
 * 3、mysql、redis 等
 */

public class Index {


	public static void main(String[] args) throws Exception {


		// 1
		kafka();


	}





	// 写入到 kafka 中
	public static void kafka() throws Exception{

		// 创建 flink 流计算执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 创建 DataStream
		DataStream<String> lines = env.socketTextStream("127.0.0.1", 9999);


		// 不做任何转换


		// 输出到 kafka 中
		Properties props = new Properties();
		props.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.22:9092");

		FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
						"flink-1129",
						new SimpleStringSchema(),
						props
		);


		lines.addSink( kafkaProducer );


		lines.print();

		try {
			env.execute( "开始" );
		} catch (Exception e) {
			e.printStackTrace();
		}

	}




}
