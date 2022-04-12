package org.apache.kafka.connect.transforms.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JusoRegexUtil {
    public static final String ADDRESS_REGEX = "(([가-힣A-Za-z·\\d~\\-\\.]{2,}(로|길)+)|([가-힣A-Za-z·\\d~\\-\\.]+(읍|면|동|가|리)\\s))";

    public static String[] getAddress(String address) {
        Matcher matcher = Pattern.compile(ADDRESS_REGEX).matcher(address);
        String[] rtnAddress = new String[2];
        if(matcher.find()) {
            rtnAddress[0] = address.substring(0, matcher.end());
            rtnAddress[1] = address.substring(matcher.end(), address.length());
            return rtnAddress;
        }else{
            rtnAddress[0] = "NOMATCH";
            rtnAddress[1] = address;
            return rtnAddress;
        }
    }
}
