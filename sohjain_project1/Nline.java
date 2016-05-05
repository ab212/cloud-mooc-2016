import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Nline extends Configured implements Tool{
    static Double sum = 0.0;
    static Integer count = 0;
    public static class NLineMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
            sum += Double.parseDouble(value.toString());
            count++;
            Text t = new Text(sum + " " + count);
            if(count==50) {
            	context.write(new Text("k"), t);
            } 
        }
    }

    public static class NLineReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
            float sum = 0, count = 0;
        	for (Text values : value){
            	String []arr = values.toString().split(" ");
            	sum += Float.parseFloat(arr[0]);
            	count += Integer.parseInt(arr[1]);
            }
        	Float avg = sum/count;
        	System.out.println(avg);
        	context.write(key, new Text(avg.toString()));
        }
    }
    
/*    public static class NLineInputFormat extends FileInputFormat<LongWritable,Text>{
        public static final String  LINES_PER_MAP = "mapreduce.input.lineinputformat.linespermap";
        
        public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, TaskAtttContext context) throws IOException{
            context.setStatus(split);
            return new LineRecordReader(); 
        }
    } */
    
    public int run(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.setInt(NLineInputFormat.LINES_PER_MAP, 50);
        
        Job job = new Job(conf,"NLine Input Format");
        job.setJarByClass(Nline.class);

        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);
        job.setInputFormatClass(NLineInputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        return job.waitForCompletion(true) ? 0:1;
    }

    public static void main(String[] args) throws Exception{
        int exitcode = ToolRunner.run(new Nline(), args);
        System.exit(exitcode);
    }
}