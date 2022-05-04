package org.apache.kafka.connect.transforms.util;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.Random;

public class DefaultCipher {
    private static final String MESSAGE_DIGEST = "SHA-256";
    private static final String AL_MO_BL_PAD = "AES/CBC/PKCS5Padding";
    private static final String PBKDF2WITH_MO_SHA1 = "PBKDF2WithHmacSHA1";
    private static final String PBKDF2WITH_MO_SHA512 = "PBKDF2WithHmacSHA512";

    private static IvParameterSpec ivKeySpec;
    private static SecretKeySpec secretKeySpec;

    public static void setSpec(SecretKeySpec keySpec, IvParameterSpec ivSpec) {
        DefaultCipher.secretKeySpec = keySpec;
        DefaultCipher.ivKeySpec = ivSpec;
    }


//    public static void setSpec(int repeat) throws UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeySpecException {
//        String keyStr = randomStrGen(32);
//        String ivStr = keyStr.substring(0, 16);
//        createKeySpec(keyStr, repeat);
//        createIVSpec(ivStr, repeat);
//    }

    public static String transformType(Object value, String cipherType, String encrpKey) throws Exception {
//        현재는 데이터 마다 spec 및 key 생성

        switch (cipherType) {
            case "1":
                System.out.println(":LINASTDOUT: transformType sha encrpKey : " + encrpKey);
                return getSHA256Value(value, encrpKey);
//                break;
            case "2":
                String ivStr = encrpKey.substring(0, 16);
                System.out.println(":LINASTDOUT: transformType aes encrpKey : " + encrpKey);
                if(secretKeySpec == null || ivKeySpec == null) {
                    createSpec(encrpKey, ivStr);
                }
//                System.out.println(":LINASTDOUT: transformType createKeySpec : " + secretKeySpec);
//                System.out.println(":LINASTDOUT: transformType createIVSpec : " + ivSpec);
                return getEncryptValue(value.toString());
//                break;
            default:
                return getSHA256Value(value, encrpKey);
        }

    }

    private static String byteToString(byte[] temp) {
        StringBuilder sb = new StringBuilder();
        for(byte b : temp) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }


    /**
     * 가져온 salt 문자열을 바이트로 반환
     * @param saltStr
     * @return
     */
    private static byte[] saltBytes(String saltStr) {
        return saltStr.getBytes(StandardCharsets.UTF_8);
//        A21121376-1-0006A21121376-1-0006
    }

    /**
     * 32자리의 키값을 이용하여 SecretKeySpec 생성
     * 생성시 마다 다른 16자리 Initial Vector를 입력하여 ivSpec을 생성.
     * @param encrpKey
     * @param iVStr
     * @throws NoSuchAlgorithmException
     * @throws UnsupportedEncodingException
     * @throws InvalidKeySpecException
     */
    public static void createSpec(String encrpKey, String iVStr) throws NoSuchAlgorithmException, UnsupportedEncodingException, InvalidKeySpecException {

        //AES 알고리즘을 적용하여 암호화키 생성
        SecretKeySpec secretSpec = new SecretKeySpec(encrpKey.getBytes(StandardCharsets.UTF_8), "AES");
        IvParameterSpec ivSpec = new IvParameterSpec(iVStr.getBytes(StandardCharsets.UTF_8));
//        System.out.println("secretKeySpec:"+secretKeySpec+":");
//        System.out.println("ivKeySpec:"+ivKeySpec+":");
        if(secretKeySpec == null || ivKeySpec == null) {
            setSpec(secretSpec, ivSpec);
//            System.out.println("secretKeySpec:"+secretKeySpec+":");
//            System.out.println("ivKeySpec:"+ivKeySpec+":");
        }
    }


    /**
     * Base64 인코딩은 3바이트 마다 4바이트 생성
     * @param msg
     * @throws Exception
     */
    public static String getEncryptValue(String msg) throws Exception {
        Cipher c = Cipher.getInstance(AL_MO_BL_PAD);
        c.init(Cipher.ENCRYPT_MODE, secretKeySpec, ivKeySpec);

        byte[] encrpytionByte = c.doFinal(msg.getBytes("UTF-8"));
//        System.out.println(encrpytionByte.length);
//        System.out.println(Base64.getEncoder().encodeToString(encrpytionByte));
        return Base64.getEncoder().encodeToString(encrpytionByte);
//        return byteToHexString(encrpytionByte);
    }

    /**
     *
     * @param encodedMsg
     * @throws Exception
     */
    public static String getDecryptValue(String encodedMsg) throws Exception {
        Cipher c = Cipher.getInstance(AL_MO_BL_PAD);
        c.init(Cipher.DECRYPT_MODE, secretKeySpec, ivKeySpec);
        byte[] decodeByte = Base64.getDecoder().decode(encodedMsg);
//        System.out.println(decodeByte.length);
//        System.out.println(new String(c.doFinal(decodeByte), "UTF-8"));
        return new String(c.doFinal(decodeByte), "UTF-8");
    }


    /**
     * SHA-256의 보완 작업
     * 원문+Salt -> 새로운 Byte 생성
     *
     * 16진수로 64byte 생성
     * @param value
     * @return
     */
    public static String getSHA256Value(Object value, String saltStr) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance(MESSAGE_DIGEST);
        String temp = value + byteToString(saltBytes(saltStr));
        md.update(temp.getBytes());
        byte[] valueBytes = md.digest();
        return byteToString(valueBytes);
    }



}