package org.apache.kafka.connect.transforms.util;

import org.apache.kafka.connect.transforms.TimestampConverter;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.SecretKeySpec;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ShillaCipherAES {

    private static final String KEY = "F82799142FA202C1";

    private static final String TRANSFORM = "AES/ECB/PKCS5Padding";

    public static String encrypt(String plainText) throws Exception{
        if(null != plainText){
            KeyGenerator kgen = KeyGenerator.getInstance("AES");
            kgen.init(128);

            byte[] raw = KEY.getBytes();
            SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");

            Cipher cipher = Cipher.getInstance(TRANSFORM);
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec);

            byte[] encrypted = cipher.doFinal(plainText.getBytes());
            return asHex(encrypted);
        }else{
            return "";
        }
    }

    public static String decrypt(String cipherText) throws Exception{
        if(null != cipherText){
            KeyGenerator kgen = KeyGenerator.getInstance("AES");
            kgen.init(128);

            byte[] raw = KEY.getBytes();
            SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");

            Cipher cipher = Cipher.getInstance(TRANSFORM);
            cipher.init(Cipher.DECRYPT_MODE, skeySpec);

            byte[] original = cipher.doFinal(fromString(cipherText));
            String originalString = new String(original);
            return originalString;
        }else{
            return "";
        }
    }

    private static String asHex(byte buf[]){
        StringBuffer strbuf = new StringBuffer(buf.length * 2);
        int i;

        for (i = 0;i < buf.length;i++){
            if(((int) buf[i] & 0xff) < 0x10)
                strbuf.append("0");

            strbuf.append(Long.toString((int) buf[i] & 0xff, 16));
        }
        return  strbuf.toString();
    }

    private static byte[] fromString(String hex){
        int len = hex.length();
        byte[] buf = new byte[((len + 1) / 2)];

        int i = 0, j = 0;
        if((len % 2) == 1)
            buf[j++] = (byte) fromDigit(hex.charAt(i++));

        while (i < len){
            buf[j++] = (byte) ((fromDigit(hex.charAt(i++)) << 4) | fromDigit(hex.charAt(i++)));
        }
        return buf;
    }

    private static int fromDigit(char ch){
        if(ch>= '0' && ch <= '9')
            return ch - '0';
        if(ch>= 'A' && ch <= 'F')
            return ch - 'A' + 10;
        if(ch>= 'a' && ch <= 'f')
            return ch - 'a' + 10;

        throw new IllegalArgumentException("invalid hex digit '" + ch + "'");
    }

    public static String encryptWithKey(String plainText, String seed) throws Exception{
        if(null != plainText){
            KeyGenerator kgen = KeyGenerator.getInstance("AES");
            kgen.init(128);

            byte[] raw = seed.getBytes();
            SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");

            Cipher cipher = Cipher.getInstance(TRANSFORM);
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec);

            byte[] encrypted = cipher.doFinal(plainText.getBytes());
            return asHex(encrypted);
        }else{
            return "";
        }
    }

    public static String decryptWithKey(String cipherText, String seed) throws Exception{
        if(null != cipherText){
            KeyGenerator kgen = KeyGenerator.getInstance("AES");
            kgen.init(128);

            byte[] raw = seed.getBytes();
            SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");

            Cipher cipher = Cipher.getInstance(TRANSFORM);
            cipher.init(Cipher.DECRYPT_MODE, skeySpec);

            byte[] original = cipher.doFinal(fromString(cipherText));
            String originalString = new String(original);
            return originalString;
        }else{
            return "";
        }
    }

    public static String getSHA256(String str){
        String SHA = "";
        try{
            MessageDigest sh = MessageDigest.getInstance("SHA-256");
            sh.update(str.getBytes());
            byte byteData[] = sh.digest();
            StringBuffer sb = new StringBuffer();
            for(int i = 0; i < byteData.length; i++){
                sb.append(Integer.toString((byteData[i]&0xff) + 0x100, 16).substring(1));
            }
            SHA = sb.toString();

        }catch(NoSuchAlgorithmException e){
            e.printStackTrace();
            SHA = null;
        }
        return SHA;
    }


}
