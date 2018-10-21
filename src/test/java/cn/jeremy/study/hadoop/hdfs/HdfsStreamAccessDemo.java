package cn.jeremy.study.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class HdfsStreamAccessDemo
{
    FileSystem fs = null;

    Configuration conf = null;

    @Before
    public void init()
        throws Exception
    {
        conf = new Configuration();
        fs = FileSystem.get(conf);
        fs = FileSystem.get(new URI("hdfs://kugoufeng00:9000"), conf, "kugoufeng");
    }

    /**
     * 通过流的方式上传文件
     *
     * @author fengjiangtao
     */
    @Test
    public void testUpload()
        throws IOException
    {
        FSDataOutputStream outputStream = fs.create(new Path("/test.demo"), true);
        FileInputStream inputStream = new FileInputStream("D:/study/test.demo");
        IOUtils.copy(inputStream, outputStream);
    }

    @Test
    public void testDownload()
        throws IOException
    {
        FSDataInputStream inputStream = fs.open(new Path("/test.demo"));
        FileOutputStream fileOutputStream = new FileOutputStream("d:/study/test.demo.123");
        IOUtils.copy(inputStream, fileOutputStream);

    }
}
