package org.apache.kafka.connect.transforms.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;

public class MessageDigestTransform {


    public static String getTransformMessage(Object value){
        String md = "";
        String val = value.toString();
        if(value instanceof java.lang.String){
//            md = "MD5";
//            return getMD5STr(val);
            return getSHA256Str(val);
        }else{
//            md = "HASH";
            return getHashCode(val);
        }

//        System.out.println(":TIMEGATE: getTransformMessage Object value : " + val);
//        String returnVal = "";
//        switch (md) {
//            case "MD5":
//                return getMD5STr(val);
////                break;
//            case "SHA256":
//                return getSHA256Str(val);
////                break;
//            case "HASH":
//                return getHashCode(val);
////                break;
//            default:
//                return getMD5STr(val);
//        }

    }

    /**
     * 메세지 다이제스트에 랜덤한 문자열을 넣어서 해싱처리하여 보안강화
     * 하지만 랜덤 문자열을 호출시마다 넣기 때문에 해싱처리된 값이 매번 달라짐
     * @return
     * @throws NoSuchAlgorithmException
     */
    private static byte[] getSalt() throws NoSuchAlgorithmException {
        SecureRandom random = new SecureRandom();
        byte[] salt = new byte[32];
        random.nextBytes(salt);
        return salt;
    }
    /**
     * HashCode()변환 Integer 값
     * @param value
     * @return
     */
    private static String getHashCode(String value) {
//        System.out.println(":TIMEGATE:transformHashCode: value :" + value);
//        System.out.println(":TIMEGATE:transformHashCode: transform to hashcode :" + value.toString().hashCode());
        return Integer.toString(value.toString().hashCode());
    }

    /**
     * MD5 변환 - 32Byte
     * @param value
     * @return
     */
    private static String getMD5STr(String value){

        String transformMD5 = "";
//        System.out.println(":TIMEGATE:str Value:"+str);
//        System.out.println(":TIMEGATE:str.getBytes() Value :"+ Arrays.toString(str.getBytes()));

        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            //hash update()
            md.update(value.getBytes());
            //get hashvalue(digest)
            byte byteData[] = md.digest();

//            System.out.println(":TIMEGATE:byteData[] Value:"+Arrays.toString(byteData));

            StringBuffer sb = new StringBuffer();
            for(byte byteTemp : byteData) {
                sb.append(Integer.toString((byteTemp&0xff) + 0x100, 16).substring(1));
            }
            transformMD5 = sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
            transformMD5 = null;
        }
        return transformMD5;
    }

    /**
     * SHA256 변환 - 64Byte
     * @param value
     * @return
     */
    private static String getSHA256Str(String value) {
        String transformSHA256 = "";
        try{
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            sha256.update(value.getBytes());
            byte byteData[] = sha256.digest();
            StringBuffer sb = new StringBuffer();

            for(int i = 0 ; i < byteData.length ; i++){
                sb.append(Integer.toString((byteData[i]&0xff) + 0x100, 16).substring(1));
            }
            transformSHA256 = sb.toString();

        }catch(NoSuchAlgorithmException e){
            e.printStackTrace();
            transformSHA256 = null;
        }
        return transformSHA256;
    }
}
