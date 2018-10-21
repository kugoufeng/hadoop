package cn.jeremy.study.hadoop.rpc.protocol;

public interface LoginProtocol
{
    
    public static final long versionID = 1L;

    public String login(String name, String password);
}
