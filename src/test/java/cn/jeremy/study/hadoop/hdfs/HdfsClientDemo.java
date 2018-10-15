package cn.jeremy.study.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class HdfsClientDemo
{
    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void init()
        throws IOException, URISyntaxException, InterruptedException
    {
        conf = new Configuration();
        fs = FileSystem.get(conf);
        fs = FileSystem.get(new URI("hdfs://kugoufeng00:9000"), conf, "kugoufeng");
    }

    @Test
    public void testMkdir()
        throws IOException
    {
        boolean testMkdir = fs.mkdirs(new Path("/testMkdir"));
        System.out.println(testMkdir);
    }

    @Test
    public void testDelete()
        throws IOException
    {
        boolean delete = fs.delete(new Path("/testMkdir"), true);
        System.out.println(delete);
    }
}
