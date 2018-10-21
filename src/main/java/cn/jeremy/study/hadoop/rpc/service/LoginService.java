package cn.jeremy.study.hadoop.rpc.service;

import cn.jeremy.study.hadoop.rpc.protocol.LoginProtocol;

public class LoginService implements LoginProtocol
{
    public String login(String name, String password)
    {
        return name.concat("login success!!!!!!!!!!!!");
    }
}
