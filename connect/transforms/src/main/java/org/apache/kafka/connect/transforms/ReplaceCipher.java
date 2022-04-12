/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;


/**
 * 암호화 변환 대상이 되는 필드의 데이터를 암호화 한다.
 * cipher.type에 따라 SHA-256, AES-256으로 암호화 한다.
 * AES-256의 경우 데이터 건 별 KEY생성 또는 전체 데이터에 하나의 KEY를 생성 할 예정(요구사항에 따라 변경예정)
 * AES-256의 경우 암호화시 생성한 KEY를 어떻게 처리 할 것인가? DB 혹은 파일등... (요구사항에 따라 변경예정)
 * @param <R>
 */
public abstract class ReplaceCipher<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Cipher specified column fields with a valid null value for the field type (i.e. 0, false, empty string, and so on)."
                    + "<p/>For numeric and string fields, an optional replacement value can be specified that is converted to the correct type."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + ReplaceCipher.Key.class.getName()
                    + "</code>) or value (<code>" + ReplaceCipher.Value.class.getName() + "</code>).";

    private SimpleConfig config;
    private static final String COLUMN_FIELD_CONFIG = "column.field";
    private static final String COLUMN_FIELD_DEFAULT = "";

    private static final String CIPHER_TYPE_CONFIG = "cipher.type";
//    private static final String CIPHER_TYPE_DEFAULT = "SHA-256";
    private static final String TYPE_SHA256 = "SHA-256";
    private static final String TYPE_AES256 = "AES-256";

    private static final String SALT_LEN_CONFIG = "salt.byte";
    private static final int SALT_LEN_DEFALUT = 16;

    private static final String KEY_STRETCHING_CONFIG = "key.stretching.repeat";
    private static final int KEY_STRETCHING_DEFALUT = 100;


    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(COLUMN_FIELD_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(),
                    ConfigDef.Importance.HIGH, "DB column including the field name described is subject to encryption.")
            .define(CIPHER_TYPE_CONFIG, ConfigDef.Type.STRING, TYPE_SHA256,ConfigDef.ValidString.in(TYPE_SHA256, TYPE_AES256),
                    ConfigDef.Importance.HIGH, "The desired cipher type: SHA-256, AES-256")
            .define(SALT_LEN_CONFIG, ConfigDef.Type.INT, SALT_LEN_DEFALUT, ConfigDef.Importance.MEDIUM,
                    "Specifies the salt byte length. The longer the length, the more complex the encryption.")
            .define(KEY_STRETCHING_CONFIG, ConfigDef.Type.INT, KEY_STRETCHING_DEFALUT, ConfigDef.Importance.MEDIUM,
                    "Specifies the number of key-stretching repetitions. The more recalls, the more complex the encryption.");


    private static final String PURPOSE = "Data encription replacement for CIGNA.";

    private static final Map<Class<?>, Function<String, ?>> REPLACEMENT_MAPPING_FUNC = new HashMap<>();
    private static final Map<Class<?>, Object> PRIMITIVE_VALUE_MAPPING = new HashMap<>();


    static {
        PRIMITIVE_VALUE_MAPPING.put(Boolean.class, Boolean.FALSE);
        PRIMITIVE_VALUE_MAPPING.put(Byte.class, (byte) 0);
        PRIMITIVE_VALUE_MAPPING.put(Short.class, (short) 0);
        PRIMITIVE_VALUE_MAPPING.put(Integer.class, 0);
        PRIMITIVE_VALUE_MAPPING.put(Long.class, 0L);
        PRIMITIVE_VALUE_MAPPING.put(Float.class, 0f);
        PRIMITIVE_VALUE_MAPPING.put(Double.class, 0d);
        PRIMITIVE_VALUE_MAPPING.put(BigInteger.class, BigInteger.ZERO);
        PRIMITIVE_VALUE_MAPPING.put(BigDecimal.class, BigDecimal.ZERO);
        PRIMITIVE_VALUE_MAPPING.put(Date.class, new Date(0));
        PRIMITIVE_VALUE_MAPPING.put(String.class, "");

        REPLACEMENT_MAPPING_FUNC.put(Byte.class, v -> Values.convertToByte(null, v));
        REPLACEMENT_MAPPING_FUNC.put(Short.class, v -> Values.convertToShort(null, v));
        REPLACEMENT_MAPPING_FUNC.put(Integer.class, v -> Values.convertToInteger(null, v));
        REPLACEMENT_MAPPING_FUNC.put(Long.class, v -> Values.convertToLong(null, v));
        REPLACEMENT_MAPPING_FUNC.put(Float.class, v -> Values.convertToFloat(null, v));
        REPLACEMENT_MAPPING_FUNC.put(Double.class, v -> Values.convertToDouble(null, v));
        REPLACEMENT_MAPPING_FUNC.put(String.class, Function.identity());
        REPLACEMENT_MAPPING_FUNC.put(BigDecimal.class, BigDecimal::new);
        REPLACEMENT_MAPPING_FUNC.put(BigInteger.class, BigInteger::new);

    }

    private Set<String> columnField;
    private  String cipherType;
    private  int saltLen;
    private  int keyStretchingRepeat;

    @Override
    public void configure(final Map<String, ?> props) {
        this.config = new SimpleConfig(CONFIG_DEF, props);
        columnField = new HashSet<>(config.getList(COLUMN_FIELD_CONFIG));
        cipherType = config.getString(CIPHER_TYPE_CONFIG);
        saltLen = config.getInt(SALT_LEN_CONFIG);
        keyStretchingRepeat = config.getInt(KEY_STRETCHING_CONFIG);
//        System.out.println(":TIMEGATE: configure method cipherType :"+cipherType);
//        System.out.println(":TIMEGATE: configure method saltLen :"+saltLen);
//        System.out.println(":TIMEGATE: configure method keyStretchingRepeat :"+keyStretchingRepeat);

//        여기서 최초 스펙 1회 생성
//        Pbkdf2Cipher.setSpec(keyStretchingRepeat);


//        for (String field : columnField) {
//            System.out.println(":TIMEGATE: configure method columnFiled :"+field);
//        }
//        System.out.println(":TIMEGATE: configure method =============================== End!! :");

    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }


    //    스키마가 없는 데이터의 변환
    private R applySchemaless(R record) {
//        System.out.println(":TIMEGATE: applySchemaless class :");
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final HashMap<String, Object> updatedValue = new HashMap<>(value);
        for (String field : columnField) {
            updatedValue.put(field, ciphered(value.get(field)));
        }
        return newRecord(record, updatedValue);
    }

    private R applyWithSchema(R record) {
//        System.out.println(":TIMEGATE: applyWithSchema class :");
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
//        System.out.println(":TIMEGATE: applyWithSchema class : Struct Type value :"+value);
//        System.out.println(":TIMEGATE: applyWithSchema class : value.schema :"+value.schema());
//        value.put(String fieldName, Object value)
//        value.put(Field field, Object. value)

        final Struct updatedValue = new Struct(value.schema());
        for (Field field : value.schema().fields()) {
//            System.out.println(":TIMEGATE: applyWithSchema class : value.schema().fields() :"+field);

            final Object origFieldValue = value.get(field);
//            System.out.println(":TIMEGATE: applyWithSchema class : value.get(field) -> origiFieldValue :"+origFieldValue);
//            System.out.println(":TIMEGATE: applyWithSchema class : record.topic() :"+record.topic());
//            System.out.println(":TIMEGATE: applyWithSchema class : field.name() :"+field.name());
//            columnfield의 값이 table.column 즉 topic.column

//            System.out.println(":TIMEGATE: applyWithSchema class : columnField :"+columnField);


            if(columnField.contains(record.topic()+"."+field.name())){

//                System.out.println(":TIMEGATE: applyWithSchema class : record.topic()+field.name() IF TRUE:"+record.topic()+"."+field.name());
                updatedValue.put(field, ciphered(origFieldValue));
            }else if(columnField.contains(record.topic()+"."+field.name()+"#JUMIN")){

                updatedValue.put(field, cipheredEtc(origFieldValue, "JUMIN"));
            }else if(columnField.contains(record.topic()+"."+field.name()+"#JUSO")){

                updatedValue.put(field, cipheredEtc(origFieldValue, "JUSO"));
            }else{
//                System.out.println(":TIMEGATE: applyWithSchema class : record.topic()+field.name() ELSE FALSE:"+record.topic()+"."+field.name());
                updatedValue.put(field, origFieldValue);
            }
//            updatedValue.put(field, columnfield.contains(record.topic()+"."+field.name()) ? ciphered(origFieldValue) : origFieldValue);
//            updatedValue.put(field, columnfield.contains(field.name()) ? ciphered(origFieldValue) : origFieldValue);
        }
        return newRecord(record, updatedValue);
    }

    private static Object cipherWithNullValue(Object value) {
        Object cipheredValue = PRIMITIVE_VALUE_MAPPING.get(value.getClass());
//        System.out.println(":TIMEGATE: cipherWithNullValue :cipheredValue:"+cipheredValue);
//        System.out.println(":TIMEGATE: cipherWithNullValue :value.getClass():"+value.getClass());
        if (cipheredValue == null) {
            if (value instanceof List)
                cipheredValue = Collections.emptyList();
            else if (value instanceof Map)
                cipheredValue = Collections.emptyMap();
            else
                throw new DataException("Cannot cipher value of type: " + value.getClass());
        }
        return cipheredValue;
    }


    //    데이터 변환 실행
    private Object ciphered(Object value) {
        if (value == null) {
            return null;
        }
        return cipherWithCustomTransforms(value, cipherType, saltLen, keyStretchingRepeat);
//        if(customreplacement == null){
//            System.out.println(":TIMEGATE: customreplacement == null:"+customreplacement);
//            return cipherWithNullValue(value);
//        }else{
//            System.out.println(":TIMEGATE: customreplacement != null:"+customreplacement);
//            return cipherWithCustomReplacement(value, customreplacement);
//        }
//        return customreplacement == null ? cipherWithNullValue(value) : cipherWithCustomReplacement(value, customreplacement);
    }

    private Object cipheredEtc(Object value, String type) {
        if (value == null) {
            return null;
        }
        return cipherWithCustomTransformsEtc(value, cipherType, saltLen, keyStretchingRepeat, type);

    }

    /**
     * 데이터 Value를 암호화 변환 하는 Method
     * type이 JUMIN, JUSO에 따른 일부 변환
     * @param value
     * @return 암호화 된 Value를 리턴
     */
    private static Object cipherWithCustomTransformsEtc(Object value, String cipherType, int saltLen, int keyStretchingRepeat, String type) {
//        System.out.println(":TIMEGATE: cipherWithCustomReplacement value : " + value);
        Function<String, ?> replacementMapper = REPLACEMENT_MAPPING_FUNC.get(value.getClass());
        if (replacementMapper == null) {
            throw new DataException("Cannot Encription value of type " + value.getClass() + " with custom replacement.");
        }
        try {
            // 암호화 타입 및 설정에 따른 암호화
            String encryptedValue = "";
//            System.out.println(":TIMEGATE: cipherWithCustomReplacement cipherType : " + cipherType);

            // 주민번호 7자리 이외의 문자열을 sha-256으로 암호화 후에 주민번호와 합치기
            if("JUMIN".equals(type)){
                String juminStr = value.toString().substring(0,7);
                String juminStrEtc = value.toString().substring(7,value.toString().length());
                encryptedValue = juminStr+Pbkdf2Cipher.transformType(juminStrEtc, cipherType, saltLen, keyStretchingRepeat);

//                System.out.println(":TIMEGATE: cipherWithCustomReplacement encryptedValue : " + encryptedValue);
            }else if("JUSO".equals(type)){
                String[] juso = new String[2];
                juso = JusoRegexUtil.getAddress(value.toString());
                if ("NOMATCH".equals(juso[0])){
                    encryptedValue = Pbkdf2Cipher.transformType(juso[1], cipherType, saltLen, keyStretchingRepeat);
                }else{
                    encryptedValue = juso[0]+Pbkdf2Cipher.transformType(juso[1], cipherType, saltLen, keyStretchingRepeat);
                }

            }

            return replacementMapper.apply(encryptedValue);
        } catch (NumberFormatException ex) {
            throw new DataException("Unable to convert SHA256 to number", ex);
        } catch (Exception ex){
            throw new DataException("Unable to convert SHA256 to other types", ex);
        }
    }

    /**
     * 데이터 Value를 암호화 변환 하는 Method
     * @param value
     * @return 암호화 된 Value를 리턴
     */
    private static Object cipherWithCustomTransforms(Object value, String cipherType, int saltLen, int keyStretchingRepeat) {
        System.out.println(":TIMEGATE: cipherWithCustomReplacement value : " + value);
        Function<String, ?> replacementMapper = REPLACEMENT_MAPPING_FUNC.get(value.getClass());
        if (replacementMapper == null) {
            throw new DataException("Cannot Encription value of type " + value.getClass() + " with custom replacement.");
        }
        try {
//            암호화 타입 및 설정에 따른 암호화
            String encryptedValue = Pbkdf2Cipher.transformType(value, cipherType, saltLen, keyStretchingRepeat);
//            System.out.println(":TIMEGATE: cipherWithCustomReplacement cipherType : " + cipherType);
//            System.out.println(":TIMEGATE: cipherWithCustomReplacement encryptedValue : " + encryptedValue);
            return replacementMapper.apply(encryptedValue);
//            return replacementMapper.apply(MessageDigestTransform.getTransformMessage(value));
        } catch (NumberFormatException ex) {
            throw new DataException("Unable to convert SHA256 to number", ex);
        } catch (Exception ex){
            throw new DataException("Unable to convert SHA256 to other types", ex);
        }
    }

    /**
     * HashCode()변환 조건 수정
     * @param value
     * @param replacement
     * @return
     */
    private static Object cipherWithCustomReplacement(Object value, String replacement) {
//        System.out.println(":TIMEGATE: cipherWithCustomReplacement value : " + value);
//        System.out.println(":TIMEGATE: cipherWithCustomReplacement replacement : " + replacement);
        Function<String, ?> replacementMapper = REPLACEMENT_MAPPING_FUNC.get(value.getClass());
        if (replacementMapper == null) {
            throw new DataException("Cannot hashCode() value of type " + value.getClass() + " with custom replacement.");
        }
        try {
            if ("customcigna".equals(replacement)) {
//                return replacementMapper.apply(md.gethashCode(value));
//                System.out.println(":TIMEGATE: cipherWithCustomReplacement messagedigest : " + messagedigest);
//                System.out.println(":TIMEGATE: MessageDigestTransform.getTransformMessage : " + MessageDigestTransform.getTransformMessage(value, messagedigest));
                return replacementMapper.apply(MessageDigestTransform.getTransformMessage(value));
            } else {
                return replacementMapper.apply(replacement);
            }
        } catch (NumberFormatException ex) {
            throw new DataException("Unable to convert " + replacement + " (" + replacement.getClass() + ") to number", ex);
        } catch (Exception ex){
            throw new DataException("Unable to convert " + replacement + " (" + replacement.getClass() + ") to other types", ex);
        }
    }



    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }


    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R base, Object value);

    protected abstract R customNewRecord(R base, Object value);


    public static final class Key<R extends ConnectRecord<R>> extends ReplaceCipher<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Object updatedValue) {
//            System.out.println(":TIMEGATE: newRecord ===Key=== class : record.topic() :"+record.topic()+",record.kafkaPartition():"+
//                    record.kafkaPartition()+",record.keySchema():"+record.keySchema()+",updatedValue:"+updatedValue+",record.valueSchema()"+
//                    record.valueSchema()+",record.value():"+record.value());
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

        @Override
        protected R customNewRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static final class Value<R extends ConnectRecord<R>> extends ReplaceCipher<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Object updatedValue) {
//            System.out.println(":TIMEGATE: newRecord ===Value=== class : record.topic() :"+record.topic()+",record.kafkaPartition():"+
//                    record.kafkaPartition()+",record.keySchema():"+record.keySchema()+",record.key():"+record.key()+",record.valueSchema()"+
//                    record.valueSchema()+",updatedValue:"+updatedValue);
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
        }

        @Override
        protected R customNewRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
        }
    }



}

