package org.apache.kafka.connect.transforms.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataUtils {
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

    public static Object round_nums(Object num, String sc) {

        int sc_int = Integer.parseInt(sc);
//        System.out.println("sc_int:"+sc_int);
        long sc_num = (long) Math.pow(10,Math.abs(sc_int));
//        System.out.println("sc_num:"+sc_num);
//        double d_value = 0;
        Object d_value = null;

        if (sc_int >= 0){
            d_value = (double)Math.round(Double.valueOf(num.toString())*sc_num)/sc_num;
        }else{
            d_value = (double)Math.round(Double.valueOf(num.toString())/sc_num)*sc_num;
        }

        return d_value;
    }

    public static Object delete_data(Object value) {

        return null;
    }

    public static Object change_data(Object value, String chgStr) {

        return chgStr;
    }
}
