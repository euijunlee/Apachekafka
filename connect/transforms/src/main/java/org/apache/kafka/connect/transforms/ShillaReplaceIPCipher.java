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
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.ShillaCipherAES;
import org.apache.kafka.connect.transforms.util.ShillaUtil;
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
public abstract class ShillaReplaceIPCipher<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Cipher specified column fields with a valid null value for the field type (i.e. 0, false, empty string, and so on)."
                    + "<p/>For numeric and string fields, an optional replacement value can be specified that is converted to the correct type."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + ShillaReplaceIPCipher.Key.class.getName()
                    + "</code>) or value (<code>" + ShillaReplaceIPCipher.Value.class.getName() + "</code>).";

    private SimpleConfig config;
    private static final String REPLACEMENT_CONFIG = "replacement";
    private static final String REPLACEMENT_CONFIG_DEFAULT = "IP";



    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(REPLACEMENT_CONFIG, ConfigDef.Type.STRING, REPLACEMENT_CONFIG_DEFAULT, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.LOW, "Custom Target replacement, that will be applied to all"
                            + " 'fields' values (numeric or non-empty string values only).");


    private static final String PURPOSE = "IP Data Encrypting replacement for Hotel Shilla.";

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

    private String replacement;
    private String ip;

    @Override
    public void configure(final Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        replacement = config.getString(REPLACEMENT_CONFIG);

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
        System.out.println(":TIMEGATE: applySchemaless class :");
//        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
//        final HashMap<String, Object> updatedValue = new HashMap<>(value);
        System.out.println(":TIMEGATE: applySchemaless class : operatingValue(record) : " + operatingValue(record));
        Object updatedValue = ciphered(operatingValue(record));
//        for (String field : columnfield) {
//            updatedValue.put(field, ciphered(value.get(field)));
//        }

        return newRecord(record, updatedValue);
    }

    private R applyWithSchema(R record) {
//        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        Object value = operatingValue(record);
//        System.out.println(":TIMEGATE: applyWithSchema class : No Struct Type value :"+value);
//        System.out.println(":TIMEGATE: applyWithSchema class : value.schema :"+value.schema());
//        final Struct updatedValue = new Struct(value.schema());

//        for (Field field : value.schema().fields()) {
//            System.out.println(":TIMEGATE: applyWithSchema class : value.schema().fields() :"+field);
//            final Object origFieldValue = value.get(field);
//            System.out.println(":TIMEGATE: applyWithSchema class : value.get(field) -> origiFieldValue :"+origFieldValue);
//            System.out.println(":TIMEGATE: applyWithSchema class : record.topic() :"+record.topic());
//            System.out.println(":TIMEGATE: applyWithSchema class : field.name() :"+field.name());

//            updatedValue.put(field, ciphered(origFieldValue));
//            updatedValue.put(field, columnfield.contains(record.topic()+"."+field.name()) ? ciphered(origFieldValue) : origFieldValue);
//            updatedValue.put(field, columnfield.contains(field.name()) ? ciphered(origFieldValue) : origFieldValue);
//        }
        return newRecord(record, ciphered(value));
    }

    //    데이터 변환 실행
    private Object ciphered(Object value) {
//        String cipheredvalue = "";
        if (value == null) {
            return null;
        }
//        return cipheredvalue == null ? cipherWithNullValue(value) : cipherWithCustomReplacement(value);
        return cipherWithCustomReplacement(value);
    }


    private static Object cipherWithNullValue(Object value) {
        Object cipheredValue = PRIMITIVE_VALUE_MAPPING.get(value.getClass());
        System.out.println(":TIMEGATE: cipherWithNullValue :cipheredValue:"+cipheredValue);
        System.out.println(":TIMEGATE: cipherWithNullValue :value.getClass():"+value.getClass());
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
            String IP = ShillaUtil.getIp(value.toString());
            System.out.println("TIMEGATE:IP:"+IP);
            String encryptedIP = ShillaCipherAES.encrypt(IP);
            System.out.println("TIMEGATE:encryptedIP:"+encryptedIP);
            String replacedIP = ShillaUtil.ipCiphered(value.toString(), IP, encryptedIP);
            System.out.println("TIMEGATE:replacedIP:"+replacedIP);
            return replacementMapper.apply(replacedIP);
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


    public static final class Key<R extends ConnectRecord<R>> extends ShillaReplaceIPCipher<R> {

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

    public static final class Value<R extends ConnectRecord<R>> extends ShillaReplaceIPCipher<R> {

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

