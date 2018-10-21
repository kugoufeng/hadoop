package cn.jeremy.study.hadoop.mr;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver
{
    public static void main(String[] args)
        throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountDriver.class);

        //指定本业务job要使用的mapper业务类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定最终输出的数据的kv数据
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //指定job的输入原始文件的所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //job.submit();
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
