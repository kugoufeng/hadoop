package cn.jeremy.study.hadoop.mr;

import java.io.IOException;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCount
{

    static class FlowCountMapper extends Mapper<LongWritable, Text, Text, Flow>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String[] split = value.toString().split("\t");
            String phone = split[1];
            int length = split.length;
            long upFlow = NumberUtils.toLong(split[length - 3], 0);
            long downFlow = NumberUtils.toLong(split[length - 2], 0);
            context.write(new Text(phone), new Flow(upFlow, downFlow));
        }
    }

    static class FlowCountReduce extends Reducer<Text, Flow, Text, Flow>
    {
        @Override
        protected void reduce(Text key, Iterable<Flow> values, Context context)
            throws IOException, InterruptedException
        {
            long sum_upFlow = 0;
            long sum_downFlow = 0;
            for (Flow value : values)
            {
                sum_upFlow += value.getUpFlow();
                sum_downFlow += value.getDownFlow();
            }
            context.write(new Text(key), new Flow(sum_upFlow, sum_downFlow));
        }
    }

    public static void main(String[] args)
        throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FlowCount.class);
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Flow.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flow.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
