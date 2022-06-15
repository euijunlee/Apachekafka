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
import org.apache.kafka.connect.transforms.util.MessageDigestTransform;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.ShillaCipherAES;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.function.Function;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;


/**
 * 호텔신라 AES128 암호화
 * @param <R>
 */
public abstract class ShillaReplaceCipher<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Cipher specified column fields with a valid null value for the field type (i.e. 0, false, empty string, and so on)."
                    + "<p/>For numeric and string fields, an optional replacement value can be specified that is converted to the correct type."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + ShillaReplaceCipher.Key.class.getName()
                    + "</code>) or value (<code>" + ShillaReplaceCipher.Value.class.getName() + "</code>).";

    private SimpleConfig config;
    private static final String COLUMNFIELD_CONFIG = "columnfield";



    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(COLUMNFIELD_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(),
                    ConfigDef.Importance.HIGH, "Name of the field to cipher as the default value of the data type.");


    private static final String PURPOSE = "Data Encrypting replacement for Hotel Shilla.";

    private static final Map<Class<?>, Function<String, ?>> REPLACEMENT_MAPPING_FUNC = new HashMap<>();
    private static final Map<Class<?>, Object> PRIMITIVE_VALUE_MAPPING = new HashMap<>();


    private static final String shilladfsKR = "_shilladfsKR";
    private static final String shilladfsCN = "_shilladfsCN";
    private static final String shilladfsJP = "_shilladfsJP";
    private static final String shilladfsEN = "_shilladfsEN";


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

    private Set<String> columnfield;
//    private String cipheredvalue;

    @Override
    public void configure(final Map<String, ?> props) {
        this.config = new SimpleConfig(CONFIG_DEF, props);
        columnfield = new HashSet<>(config.getList(COLUMNFIELD_CONFIG));

        for (String field : columnfield) {
//            System.out.println(":TIMEGATE: configure method columnfiled size :"+columnfield.size());
//            System.out.println(":TIMEGATE: configure method columnfiled :"+field);
//            System.out.println(":TIMEGATE: configure method =============================== :");
        }

    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
//            System.out.println(":TIMEGATE: applySchemaless");
            return applySchemaless(record);
        } else {
//            System.out.println(":TIMEGATE: applyWithSchema");
            return applyWithSchema(record);
        }
    }


    //    스키마가 없는 데이터의 변환
    private R applySchemaless(R record) {
//        System.out.println(":TIMEGATE: applySchemaless class :");
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final HashMap<String, Object> updatedValue = new HashMap<>(value);
        for (String field : columnfield) {
            updatedValue.put(field, ciphered(value.get(field)));
        }
        return newRecord(record, updatedValue);
    }

    private R applyWithSchema(R record) {
//        System.out.println(":TIMEGATE: applyWithSchema class :");
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
//        System.out.println(":TIMEGATE: applyWithSchema class : Struct Type value :"+value);
//        System.out.println(":TIMEGATE: applyWithSchema class : value.schema :"+value.schema());
        final Struct updatedValue = new Struct(value.schema());

//      코드 값을 찾는 루프 추가

        String onlineCode = "";
        for (Field field : value.schema().fields()) {
            final Object origFieldValue = value.get(field);
            if("STOR_CD".equals(field.name())) {
                String strCd = origFieldValue.toString();
                if (strCd == null || strCd.equals("")){
                    strCd = "";
                }

                switch (strCd) {
                    case "51":
                        onlineCode = shilladfsKR;
                        break;
                    case "52":
                        onlineCode = shilladfsCN;
                        break;
                    case "54":
                        onlineCode = shilladfsJP;
                        break;
                    case "55":
                        onlineCode = shilladfsEN;
                        break;
                    default:
                        onlineCode = "";
                        break;
                }
            }
        }


        for (Field field : value.schema().fields()) {
//            System.out.println(":TIMEGATE: applyWithSchema class : value.schema().fields() :"+field);
            final Object origFieldValue = value.get(field);
//            System.out.println(":TIMEGATE: applyWithSchema class : value.get(field) -> origiFieldValue :"+origFieldValue);
//            System.out.println(":TIMEGATE: applyWithSchema class : record.topic() :"+record.topic());
//            System.out.println(":TIMEGATE: applyWithSchema class : field.name() :"+field.name());
//            field.name()에 대한 값을 columnfield와 비교 하여 암호화 처리
//            columnfield의 값이 table.column 즉 topic.column


//            System.out.println(":TIMEGATE: applyWithSchema class : columnfield :"+columnfield);
            if(columnfield.contains(record.topic()+"."+field.name())){
//                System.out.println(":SHILLA: applyWithSchema class : record.topic()+field.name() 암호화 대상 필드:"+record.topic()+"."+field.name());
//                System.out.println(":SHILLA: applyWithSchema class : 암호화 대상 값 origFieldValue :"+origFieldValue);
//                System.out.println(":SHILLA: applyWithSchema class : 암호화 코드 onlineCode :"+onlineCode);

                updatedValue.put(field, ciphered(origFieldValue+onlineCode));
            }else{
//                System.out.println(":TIMEGATE: applyWithSchema class : record.topic()+field.name() ELSE FALSE:"+record.topic()+"."+field.name());
                updatedValue.put(field, origFieldValue);
            }
//            updatedValue.put(field, columnfield.contains(record.topic()+"."+field.name()) ? ciphered(origFieldValue) : origFieldValue);
//            updatedValue.put(field, columnfield.contains(field.name()) ? ciphered(origFieldValue) : origFieldValue);
        }
        return newRecord(record, updatedValue);
    }

    //    데이터 변환 실행
    private Object ciphered(Object value) {
//        String cipheredvalue = "";
        if (value == null) {
            return null;
        }
        return cipherWithCustomReplacement(value);
//        return cipheredvalue == null ? cipherWithNullValue(value) : cipherWithCustomReplacement(value);
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

    /**
     * 암호화
     * @param value
     * @param
     * @return
     */
    private static Object cipherWithCustomReplacement(Object value) {
        Function<String, ?> replacementMapper = REPLACEMENT_MAPPING_FUNC.get(value.getClass());
        if (replacementMapper == null) {
            throw new DataException("Cannot mask value of type " + value.getClass() + " with custom replacement.");
        }
        try {
            return replacementMapper.apply(ShillaCipherAES.encrypt(value.toString()));
        } catch (NumberFormatException ex) {
            throw new DataException("Unable to convert Data", ex);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
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


    public static final class Key<R extends ConnectRecord<R>> extends ShillaReplaceCipher<R> {

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

    }

    public static final class Value<R extends ConnectRecord<R>> extends ShillaReplaceCipher<R> {

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

    }



}

