package org.apache.kafka.connect.transforms.util;

import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.Random;

public class Pbkdf2Cipher {
    private static final String MESSAGE_DIGEST = "SHA-256";
    private static final String AL_MO_BL_PAD = "AES/CBC/PKCS5Padding";
    private static final String PBKDF2WITH_MO_SHA1 = "PBKDF2WithHmacSHA1";
    private static final String PBKDF2WITH_MO_SHA512 = "PBKDF2WithHmacSHA512";

    private static IvParameterSpec ivSpec;
    private static SecretKeySpec secretKeySpec;

    public static void setIvSpec(IvParameterSpec ivSpec) {
        Pbkdf2Cipher.ivSpec = ivSpec;
    }
    public static void setSecretKeySpec(SecretKeySpec keySpec) {
        Pbkdf2Cipher.secretKeySpec = keySpec;
    }

    public static void setSpec(int repeat) throws UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeySpecException {
        String keyStr = randomStrGen(32);
        String ivStr = keyStr.substring(0, 16);
        createKeySpec(keyStr, repeat);
        createIVSpec(ivStr, repeat);
    }

    public static String transformType(Object value, String cipherType, int saltByte, int repeat) throws Exception {
//        현재는 데이터 마다 spec 및 key 생성

        switch (cipherType) {
            case "SHA-256":
                return getSHA256Value(value, repeat);
//                break;
            case "AES-256":
                String keyStr = randomStrGen(32);
                String ivStr = keyStr.substring(0, 16);
//                System.out.println(":TIMEGATE: transformType createKeySpec keyStr : " + keyStr);
//                System.out.println(":TIMEGATE: transformType createIVSpec ivStr : " + ivStr);
                createKeySpec(keyStr, repeat);
                createIVSpec(ivStr, repeat);
//                System.out.println(":TIMEGATE: transformType createKeySpec : " + secretKeySpec);
//                System.out.println(":TIMEGATE: transformType createIVSpec : " + ivSpec);
                return getEncryptValue(value.toString());
//                break;
            default:
                return getSHA256Value(value, repeat);
        }

    }

    /**
     * 바이트 배열을 HEX 문자열로 변환한다.
     * @param data
     * @return
     */
    private String byteToHexString(byte[] data) {
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
    public static void createKeySpec(String baseKey, int repeat) throws NoSuchAlgorithmException, UnsupportedEncodingException, InvalidKeySpecException {

        byte[] saltedBytes = getSHA256ForAES(baseKey);
        SecretKeyFactory factory = SecretKeyFactory.getInstance(PBKDF2WITH_MO_SHA1);
        // 256bit (AES256은 256bit(32byte)의 키, 128bit(16byte)의 블록사이즈를 가짐.)
        // Password-Based Encryption방식으로 keyspec 생성시 Key Stratching용 반복해쉬작업수 10000, 암호키 크기 256
        PBEKeySpec pbeKeySpec = new PBEKeySpec(baseKey.toCharArray(), saltedBytes, repeat, 256);
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
    public static void createIVSpec(String IV, int repeat) throws NoSuchAlgorithmException, UnsupportedEncodingException, InvalidKeySpecException {

        byte[] saltedBytes = getSHA256ForAES(IV);

        SecretKeyFactory factory = SecretKeyFactory.getInstance(PBKDF2WITH_MO_SHA1);
        // 128bit(16byte) 스펙 생성
        PBEKeySpec pbeIvSpec = new PBEKeySpec(IV.toCharArray(), saltedBytes, repeat, 128);
        Key secretIV = factory.generateSecret(pbeIvSpec);

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
    public static String getEncryptValue(String msg) throws Exception {
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
    public static String getDecryptValue(String encodedMsg) throws Exception {
        Cipher c = Cipher.getInstance(AL_MO_BL_PAD);
        c.init(Cipher.DECRYPT_MODE, secretKeySpec, ivSpec);
        byte[] decodeByte = Base64.getDecoder().decode(encodedMsg);
//        System.out.println(decodeByte.length);
//        System.out.println(new String(c.doFinal(decodeByte), "UTF-8"));
        return new String(c.doFinal(decodeByte), "UTF-8");
    }

    /**
     * key와 key-stretching 길이를 알고 복호화 수행
     * @param encodedMsg
     * @param baseKey
     * @param repeat
     * @return
     * @throws Exception
     */
    public static String decryptValue(String encodedMsg, String baseKey, int repeat) throws Exception {

        byte[] keySaltedBytes = getSHA256ForAES(baseKey);
        byte[] IvSaltedBytes = getSHA256ForAES(baseKey.substring(0, 16));
        String ivStr = baseKey.substring(0, 16);
        SecretKeyFactory factory = SecretKeyFactory.getInstance(PBKDF2WITH_MO_SHA1);
        PBEKeySpec pbeKeySpec = new PBEKeySpec(baseKey.toCharArray(), keySaltedBytes, repeat, 256);
        PBEKeySpec pbeIvSpec = new PBEKeySpec(ivStr.toCharArray(), IvSaltedBytes, repeat, 128);
        Key secretKey = factory.generateSecret(pbeKeySpec);
        Key secretIv = factory.generateSecret(pbeIvSpec);

        byte[] key = new byte[32];
        byte[] iv = new byte[16];
        System.arraycopy(secretKey.getEncoded(), 0, key, 0, key.length);
        System.arraycopy(secretIv.getEncoded(), 0, iv, 0, 16);

        //AES 알고리즘을 적용하여 암호화키 생성
        SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
        IvParameterSpec ivSpec = new IvParameterSpec(iv);

        Cipher c = Cipher.getInstance(AL_MO_BL_PAD);
        c.init(Cipher.DECRYPT_MODE, keySpec, ivSpec);
        byte[] decodeByte = Base64.getDecoder().decode(encodedMsg);
//        System.out.println(decodeByte.length);
//        System.out.println(new String(c.doFinal(decodeByte), "UTF-8"));
        return new String(c.doFinal(decodeByte), "UTF-8");
    }

    /**
     * KeySpec, IVSpec을 만들기 위한 SHA-256으로 해시를 통해 salting함
     * 16진수로 64byte 생성
     * @param strValue
     * @return
     */
    public static byte[] getSHA256ForAES(String strValue) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        MessageDigest md = MessageDigest.getInstance(MESSAGE_DIGEST);
        byte[] txtBytes = strValue.getBytes("UTF-8");

        return md.digest(txtBytes);
    }

    /**
     * SHA-256의 보완 작업
     * 원문+Salt -> 새로운 Byte 생성
     * Digest+Salt * 복수(Key-Stretching) -> 새로운 Digest 생성
     * 16진수로 64byte 생성
     * @param value
     * @return
     */
    public static String getSHA256Value(Object value, int repeat) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        MessageDigest md = MessageDigest.getInstance(MESSAGE_DIGEST);
        byte[] valueBytes = value.toString().getBytes();
        for(int i = 0; i < repeat; i++) {
            String temp = value + byteToString(saltBytes(16));
            md.update(temp.getBytes());
            valueBytes = md.digest();
        }
        return byteToString(valueBytes);
    }



}