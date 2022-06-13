package org.apache.kafka.connect.transforms.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ShillaUtil {

    public static String[] getIPs(String str){
        Pattern p = Pattern.compile("((25[0-5]|2[0-4][0-9]|1[0-9]{2}|[0-9]{1,2})\\.){3}(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[0-9]{1,2})");
        Matcher m = p.matcher(str);
        StringBuffer sb = new StringBuffer();
        while(m.find()){
            sb.append(m.group() + " ");
        }

        return m.reset().find() ? sb.toString().split(" ") : new String[0];
    }

    public static String getIP(String str){
        Pattern p = Pattern.compile("(IP: )((25[0-5]|2[0-4][0-9]|1[0-9]{2}|[0-9]{1,2})\\.){3}(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[0-9]{1,2})");//
        Matcher m = p.matcher(str);
        StringBuffer sb = new StringBuffer();
        while(m.find()){
            sb.append(m.group());
        }
        String rtnStr = "";
        if(m.reset().find()){
            rtnStr = sb.toString();
        }

        return rtnStr.substring(4);
    }
    public static String ipsCiphered(String log, String[] ip) throws Exception {
        for(int i=0; i <ip.length; i++){
            log = log.replace(ip[i], ShillaCipherAES.encrypt(ip[i]));
        }

        return log;
    }

    public static String ipCiphered(String log, String ip, String cipheredStr){
        return log.replace(ip, cipheredStr);
    }
}
