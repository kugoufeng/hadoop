package cn.jeremy.study.hadoop.rpc.service;

import cn.jeremy.study.hadoop.rpc.protocol.LoginProtocol;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;

public class ServiceEngin
{
    public static void main(String[] args)
        throws IOException
    {
        Builder builder = new Builder(new Configuration());
        builder.setBindAddress("localhost")
            .setPort(8888)
            .setProtocol(LoginProtocol.class)
            .setInstance(new LoginService());
        Server server = builder.build();
        server.start();
    }
}
