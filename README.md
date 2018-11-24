# mapreduce








public class AverageTest {
		// 定义输入路径
		private static final String INPUT_PATH = "hdfs://liaozhongmin:9000/average_file";
		// 定义输出路径
		private static final String OUT_PATH = "hdfs://liaozhongmin:9000/out";
 
		public static void main(String[] args) {
 
			try {
				// 创建配置信息
				Configuration conf = new Configuration();
				
				// 创建文件系统
				FileSystem fileSystem = FileSystem.get(new URI(OUT_PATH), conf);
				// 如果输出目录存在，我们就删除
				if (fileSystem.exists(new Path(OUT_PATH))) {
					fileSystem.delete(new Path(OUT_PATH), true);
				}
 
				// 创建任务
				Job job = new Job(conf, AverageTest.class.getName());
 
				//1.1	设置输入目录和设置输入数据格式化的类
				FileInputFormat.setInputPaths(job, INPUT_PATH);
				job.setInputFormatClass(TextInputFormat.class);
 
				//1.2	设置自定义Mapper类和设置map函数输出数据的key和value的类型
				job.setMapperClass(AverageMapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
 
				//1.3	设置分区和reduce数量(reduce的数量，和分区的数量对应，因为分区为一个，所以reduce的数量也是一个)
				job.setPartitionerClass(HashPartitioner.class);
				job.setNumReduceTasks(1);
 
				//1.4	排序
				//1.5	归约
				//2.1	Shuffle把数据从Map端拷贝到Reduce端。
				//2.2	指定Reducer类和输出key和value的类型
				job.setReducerClass(AverageReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(FloatWritable.class);
 
				//2.3	指定输出的路径和设置输出的格式化类
				FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
				job.setOutputFormatClass(TextOutputFormat.class);
 
 
				// 提交作业 退出
				System.exit(job.waitForCompletion(true) ? 0 : 1);
			
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
 
	public static class AverageMapper extends Mapper<LongWritable, Text, Text, Text>{
		//设置输出的key和value
		private Text outKey = new Text();
		private Text outValue = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		
			//获取输入的行
			String line = value.toString();
			//取出无效记录
			if (line == null || line.equals("")){
				return ;
			}
			//对数据进行切分
			String[] splits = line.split("\t");
			
			//截取姓名和成绩
			String name = splits[0];
			String score = splits[2];
			//设置输出的Key和value
			outKey.set(name);
			outValue.set(score);
			//将结果写出去
			context.write(outKey, outValue);
			
		}
		
	}
	
	public static class AverageReducer extends Reducer<Text, Text, Text, FloatWritable>{
		//定义写出去的Key和value
		private Text name = new Text();
		private FloatWritable avg = new FloatWritable();
		@Override
		protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
			//定义科目数量
			int courseCount = 0;
			//定义中成绩
			int sum = 0;
			//定义平均分
			float average = 0;
			
			//遍历集合求总成绩
			for (Text val : value){
				sum += Integer.parseInt(val.toString());
				courseCount ++;
			}
			
			//求平均成绩
			average = sum / courseCount;
			
			//设置写出去的名字和成绩
			name.set(key);
			avg.set(average);
			
			//把结果写出去
			context.write(name, avg);
		}
	}
}



package com.hebut.mr;

import java.io.IOException;

import java.util.Iterator;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

public class Score {

    public static class Map extends

            Mapper<LongWritable, Text, Text, IntWritable> {

        // 实现map函数

        public void map(LongWritable key, Text value, Context context)

                throws IOException, InterruptedException {

            // 将输入的纯文本文件的数据转化成String

            String line = value.toString();

            // 将输入的数据首先按行进行分割

            StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");

            // 分别对每一行进行处理

            while (tokenizerArticle.hasMoreElements()) {

                // 每行按空格划分

                StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());

                String strName = tokenizerLine.nextToken();// 学生姓名部分

                String strScore = tokenizerLine.nextToken();// 成绩部分

                Text name = new Text(strName);

                int scoreInt = Integer.parseInt(strScore);

                // 输出姓名和成绩

                context.write(name, new IntWritable(scoreInt));

            }

        }



    }



    public static class Reduce extends

            Reducer<Text, IntWritable, Text, IntWritable> {

        // 实现reduce函数

        public void reduce(Text key, Iterable<IntWritable> values,

                Context context) throws IOException, InterruptedException {

            int sum = 0;

            int count = 0;

            Iterator<IntWritable> iterator = values.iterator();

            while (iterator.hasNext()) {

                sum += iterator.next().get();// 计算总分

                count++;// 统计总的科目数

            }

            int average = (int) sum / count;// 计算平均成绩

            context.write(key, new IntWritable(average));

        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        // 这句话很关键

        conf.set("mapred.job.tracker", "192.168.1.2:9001");

        String[] ioArgs = new String[] { "score_in", "score_out" };

        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();

        if (otherArgs.length != 2) {

            System.err.println("Usage: Score Average <in> <out>");

            System.exit(2);

        }



        Job job = new Job(conf, "Score Average");

        job.setJarByClass(Score.class);

        // 设置Map、Combine和Reduce处理类

        job.setMapperClass(Map.class);

        job.setCombinerClass(Reduce.class);

        job.setReducerClass(Reduce.class);

        // 设置输出类型

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        // 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现

        job.setInputFormatClass(TextInputFormat.class);

        // 提供一个RecordWriter的实现，负责数据输出

        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置输入和输出目录

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}

                a1.sources = r1
		a1.channels = c1
		a1.sinkgroups = g1
		a1.sinks = k1 k2

		a1.sources.r1.type=netcat
		a1.sources.r1.bind=localhost
		a1.sources.r1.port=8888

		a1.channels.c1.type = memory
		a1.channels.c1.capacity = 100000
		a1.channels.c1.transactionCapacity = 100
		a1.sinks.k1.type=file_roll
		a1.sinks.k1.sink.directory=/home/centos/flume/f1

		a1.sinks.k2.type=file_roll
		a1.sinks.k2.sink.directory=/home/centos/flume/f2

		a1.sinkgroups.g1.sinks = k1 k2
		a1.sinkgroups.g1.processor.type = failover
		a1.sinkgroups.g1.processor.priority.k1 = 1
		a1.sinkgroups.g1.processor.priority.k2 = 2
		a1.sinkgroups.g1.processor.maxpenalty = 10000

		a1.sources.r1.channels=c1
		a1.sinks.k1.channel=c1
		a1.sinks.k2.channel=c1
---------------------
a1.sources = r1
a1.sinks = k1 k2
a1.channels = c1
 
# Describe/configure the source
a1.sources.r1.type = exec
a1.sources.r1.channels=c1
a1.sources.r1.command=tail -F /root/dev/biz/logs/bizlogic.log 

#define sinkgroups
a1.sinkgroups=g1
a1.sinkgroups.g1.sinks=k1 k2
a1.sinkgroups.g1.processor.type=load_balance
a1.sinkgroups.g1.processor.backoff=true
a1.sinkgroups.g1.processor.selector=round_robin

#define the sink 1
a1.sinks.k1.type=avro
a1.sinks.k1.hostname=192.168.11.179
a1.sinks.k1.port=9876  

#define the sink 2
a1.sinks.k2.type=avro
a1.sinks.k2.hostname=192.168.11.178
a1.sinks.k2.port=9876


# Use a channel which buffers events in memory
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100
 
# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
a1.sinks.k2.channel=c1
--------------------- 
作者：chiweitree 
来源：CSDN 
原文：https://blog.csdn.net/simonchi/article/details/42495299 
版权声明：本文为博主原创文章，转载请附上博文链接！
