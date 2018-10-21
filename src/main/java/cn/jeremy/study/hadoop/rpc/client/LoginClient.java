package cn.jeremy.study.hadoop.rpc.client;

import cn.jeremy.study.hadoop.rpc.protocol.LoginProtocol;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class LoginClient
{
    public static void main(String[] args)
        throws IOException
    {
        LoginProtocol loginProtocol =
            RPC.getProxy(LoginProtocol.class, 1L, new InetSocketAddress("localhost", 8888), new Configuration());
        String result = loginProtocol.login("kugoufeng", "11233333");
        System.out.println(result);
    }
}
