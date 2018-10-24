package cn.jeremy.study.hadoop.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class Flow implements Writable
{
    private Long upFlow;

    private Long downFlow;

    private Long sunFlow;

    public Flow()
    {
    }

    public Flow(Long upFlow, Long downFlow)
    {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sunFlow = upFlow + downFlow;
    }

    public Long getUpFlow()
    {
        return upFlow;
    }

    public void setUpFlow(Long upFlow)
    {
        this.upFlow = upFlow;
    }

    public Long getDownFlow()
    {
        return downFlow;
    }

    public void setDownFlow(Long downFlow)
    {
        this.downFlow = downFlow;
    }

    public Long getSunFlow()
    {
        return sunFlow;
    }

    public void setSunFlow(Long sunFlow)
    {
        this.sunFlow = sunFlow;
    }

    @Override
    public void write(DataOutput dataOutput)
        throws IOException
    {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
    }

    @Override
    public void readFields(DataInput dataInput)
        throws IOException
    {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
    }

    @Override
    public String toString()
    {
        return upFlow + "\t" + downFlow + "\t" + sunFlow;
    }
}
