import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class HW_Proj {

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			context.write(new Text(value.toString()), new Text(value.toString()));
		}
	}

	public static class Combine extends Reducer<Text, Text, Text, Text>{

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			Text result;
			double min = Double.MAX_VALUE;
			double max = Double.MIN_VALUE;
			double sumSq = 0;
			double sum = 0;
			double count = 0;
			for(Text val: values){
				double d = Double.parseDouble(val.toString());
				sum += d;
				sumSq += Math.pow(d, 2);
				count += 1;
				min = Math.min(min, d);
				max = Math.max(max, d);
			}
			result = new Text(sum + " " + sumSq + " " + min + " " + max + " " + count);
			context.write(new Text("k"), result);
		}
	}


	public static class Reduce
	extends Reducer<Text,Text,Text,DoubleWritable> {

		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<Text> values, 
				Context context
				) throws IOException, InterruptedException {
			double sum = 0;
			double count = 0;
			double sumSq = 0;
			double min = Double.MAX_VALUE;
			double max = Double.MIN_VALUE;
			for (Text val : values) {
				String []str = val.toString().split(" ");
				sum += Double.parseDouble(str[0]);
				sumSq += Double.parseDouble(str[1]);
				min = Math.min(min, Double.parseDouble(str[2]));
				max = Math.max(max, Double.parseDouble(str[3]));
				count += Double.parseDouble(str[4]);
			}
			double avg = sum/count;
			double stdDiv = Math.sqrt((sumSq/count) - Math.pow((sum/count),2));
			context.write(new Text("Avg"), new DoubleWritable(avg));
			context.write(new Text("Min"), new DoubleWritable(min));
			context.write(new Text("Max"), new DoubleWritable(max));
			context.write(new Text("StdDev"), new DoubleWritable(stdDiv));
			context.write(new Text("Count"), new DoubleWritable(count));
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); 
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}

		// create a job with name "wordcount"
		Job job = new Job(conf, "wordcount");
		job.setJarByClass(HW_Proj.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);

		// Add a combiner here, not required to successfully run the wordcount program  

		// set output key type   
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
