package cn.jeremy.study.hadoop.mr;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * KEYIN:默认情况下，是mr框架索读到的一行文本的起始偏移量，Long
 * VALURIN:默认情况下，是mr框架所读到的一行文本的内容，String
 *
 * KEYOUT:是用户自定义逻辑完成之后输出数据中的key，在此处是单词，String
 * VALUEOUT:是用户自定义逻辑处理完成之后输出数据中的value，在此处是单词次数，Integer
 *
 * @author fengjiangtao
 * @date 2018/10/21 21:28
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    /**
     * map阶段的业务逻辑写在自定义的map（）方法中
     * maptask会对每一行输入数据调用一次我们自定义的map（）方法
     *
     * @param key
     * @param value
     * @param context
     * @author fengjiangtao
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException
    {
        //将maptask传给我们的文本内容转换成String
        String line = value.toString();
        //根据空格将这一行划分成单词
        String[] words = line.split(" ");
        //将单词输出为<单词，1>
        for (String word : words)
        {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
