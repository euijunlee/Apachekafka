package org.apache.kafka.connect.transforms.util;

import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.InvalidParameterSpecException;
import java.util.Base64;
import java.util.Random;

public class AesPbkdf2Cipher {
    private static final String MESSAGE_DIGEST = "SHA-256";
    private static final String AL_MO_BL_PAD = "AES/CBC/PKCS5Padding";
    private static final String PBKDF2WITH_MO_SHA1 = "PBKDF2WithHmacSHA1";
    private static final String PBKDF2WITH_MO_SHA512 = "PBKDF2WithHmacSHA512";

    private static IvParameterSpec ivSpec;
    private static SecretKeySpec secretKeySpec;

    public static void setIvSpec(IvParameterSpec ivSpec) {
        AesPbkdf2Cipher.ivSpec = ivSpec;
    }
    public static void setSecretKeySpec(SecretKeySpec keySpec) {
        AesPbkdf2Cipher.secretKeySpec = keySpec;
    }

    /**
     * 바이트 배열을 HEX 문자열로 변환한다.
     * @param data
     * @return
     */
    private static String byteToHexString(byte[] data) {
        StringBuilder sb = new StringBuilder();
        for(byte b : data) {
            sb.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }

    private static String byteToString(byte[] temp) {
        StringBuilder sb = new StringBuilder();
        for(byte b : temp) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * 숫자와 영 대/소문자를 사용한 램덤 문자열 생성하여 AES256의 키로 사용
     * @return
     */
    private static String randomStrGen(int rLen) {
//        숫자 "0" 위치
        int lLimit = 48;
//        영대문자 "Z" 위치
        int rLimit = 122;
        String genString = new Random().ints(lLimit,rLimit + 1)
//                아스키 코드에서 숫자와 영대/소문자의 문자열 범위만 사용
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(rLen)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();

        return genString;
    }

    private static byte[] saltBytes(int sLen) throws NoSuchAlgorithmException {
//        SecureRandom random = new SecureRandom();
        byte[] salt = new byte[sLen];
        new SecureRandom().nextBytes(salt);
        return salt;
    }

    /**
     * Salting 방식과 Key Stratching 방식을 혼합한 32자리의 키값을 이용하여 SecretKeySpec 생성
     * @param  baseKey                     생성된 Key로 KeySpec 생성
     * @throws UnsupportedEncodingException 인코딩 오류
     * @throws NoSuchAlgorithmException     알고리즘 오류
     * @throws InvalidKeySpecException      키스펙 오류
     */
    public static void createKeySpec(String baseKey) throws NoSuchAlgorithmException, UnsupportedEncodingException, InvalidKeySpecException {

        byte[] saltedBytes = mdSHA256(baseKey);
        SecretKeyFactory factory = SecretKeyFactory.getInstance(PBKDF2WITH_MO_SHA1);
        // 256bit (AES256은 256bit(32byte)의 키, 128bit(16byte)의 블록사이즈를 가짐.)
        // Password-Based Encryption방식으로 keyspec 생성시 Key Stratching용 반복해쉬작업수 10000, 암호키 크기 256
        PBEKeySpec pbeKeySpec = new PBEKeySpec(baseKey.toCharArray(), saltedBytes, 10000, 256);
        Key secretKey = factory.generateSecret(pbeKeySpec);

        byte[] key = new byte[32];
        System.arraycopy(secretKey.getEncoded(), 0, key, 0, key.length);

        //AES 알고리즘을 적용하여 암호화키 생성
        SecretKeySpec secret = new SecretKeySpec(key, "AES");
        setSecretKeySpec(secret);
    }
    /**
     * 생성시 마다 다른 16자리 Initial Vector를 입력하여 ivSpec을 생성.
     * @param  IV                           Initial Vector 생성을 위한 16byte 값
     * @throws UnsupportedEncodingException 인코딩 오류
     * @throws NoSuchAlgorithmException     알고리즘 오류
     * @throws InvalidKeySpecException      키스펙 오류
     * @
     */
    public static void createIVSpec(String IV) throws NoSuchAlgorithmException, UnsupportedEncodingException, InvalidKeySpecException {

        byte[] saltedBytes = mdSHA256(IV);

        SecretKeyFactory factory = SecretKeyFactory.getInstance(PBKDF2WITH_MO_SHA1);
        // 128bit(16byte) 스펙 생성
        PBEKeySpec pbeKeySpec = new PBEKeySpec(IV.toCharArray(), saltedBytes, 10000, 128);
        Key secretIV = factory.generateSecret(pbeKeySpec);

        byte[] iv = new byte[16];
        System.arraycopy(secretIV.getEncoded(), 0, iv, 0, 16);
        IvParameterSpec ivSpec = new IvParameterSpec(iv);
        setIvSpec(ivSpec);
    }

    /**
     * Base64 인코딩은 3바이트 마다 4바이트 생성
     * @param msg
     * @throws Exception
     */
    public String encrypt(String msg) throws Exception {
        Cipher c = Cipher.getInstance(AL_MO_BL_PAD);
        c.init(Cipher.ENCRYPT_MODE, secretKeySpec, ivSpec);

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
    public String decrypt(String encodedMsg) throws Exception {
        Cipher c = Cipher.getInstance(AL_MO_BL_PAD);
        c.init(Cipher.DECRYPT_MODE, secretKeySpec, ivSpec);
        byte[] decodeByte = Base64.getDecoder().decode(encodedMsg);
//        System.out.println(decodeByte.length);
//        System.out.println(new String(c.doFinal(decodeByte), "UTF-8"));
        return new String(c.doFinal(decodeByte), "UTF-8");
    }

    /**
     * KeySpec, IVSpec을 만들기 위한 SHA-256으로 해시를 통해 salting함
     * 16진수로 64byte 생성
     * @param txt
     * @return
     */
    public static byte[] mdSHA256(String txt) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        MessageDigest md = MessageDigest.getInstance(MESSAGE_DIGEST);
        byte[] txtBytes = txt.getBytes("UTF-8");
//        byte[] saltedBytes = md.digest(txtBytes);
//        md.update(txt.getBytes());
        return md.digest(txtBytes);
    }

}
