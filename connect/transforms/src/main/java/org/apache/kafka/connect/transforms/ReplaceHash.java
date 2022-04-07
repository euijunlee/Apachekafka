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
import org.apache.kafka.connect.transforms.util.SimpleConfig;

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
 * 기본적으로 MaskField의 Field는 동일한 동작을 하나, customreplacement는
 * "customcigna"일 경우 Hashcode()로 Hashing처리함
 * "customcigna" 이외의 값일 경우 MaskField의 Replacement와 동일하게 처리
 * @param <R>
 */
public abstract class ReplaceHash<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Mask specified column fields with a valid null value for the field type (i.e. 0, false, empty string, and so on)."
                    + "<p/>For numeric and string fields, an optional replacement value can be specified that is converted to the correct type."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + ReplaceHash.Key.class.getName()
                    + "</code>) or value (<code>" + ReplaceHash.Value.class.getName() + "</code>).";

    private SimpleConfig config;
    private static final String COLUMNFIELD_CONFIG = "columnfield";
    private static final String CUSTOMREPLACEMENT_CONFIG = "customreplacement";
//    private static final String MESSAGEDIGEST_CONFIG = "messagedigest";


    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(COLUMNFIELD_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(),
                    ConfigDef.Importance.HIGH, "Name of the field to mask as the default value of the data type.")
            .define(CUSTOMREPLACEMENT_CONFIG, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.LOW, "Custom value hashcode replacement, that will be applied to all"
                            + " 'columnfield' values (numeric or non-empty string values only).");
//            .define(MESSAGEDIGEST_CONFIG, ConfigDef.Type.STRING, "MD5", new ConfigDef.NonEmptyString(),
//                    ConfigDef.Importance.HIGH, "Set the message digest type. Valid values are MD5(Default Value), SHA256, HASH.");

    private static final String PURPOSE = "Data hashing replacement for cigna.";

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

    private Set<String> columnfield;
    private String customreplacement;
    private static String messagedigest;

    @Override
    public void configure(final Map<String, ?> props) {
        this.config = new SimpleConfig(CONFIG_DEF, props);
        columnfield = new HashSet<>(config.getList(COLUMNFIELD_CONFIG));

        for (String field : columnfield) {
            System.out.println(":TIMEGATE: configure method columnfiled size :"+columnfield.size());
            System.out.println(":TIMEGATE: configure method columnfiled :"+field);
            System.out.println(":TIMEGATE: configure method =============================== :");
        }
        System.out.println(":TIMEGATE: configure method columnfiled.contain EMPLOYEES.JOB_ID :"+columnfield.contains("EMPLOYEES.JOB_ID"));
        System.out.println(":TIMEGATE: configure method columnfiled.contain EMPLOYEES.FIRST_NAME :"+columnfield.contains("EMPLOYEES.FIRST_NAME"));
        System.out.println(":TIMEGATE: configure method columnfiled.contain EMPLOYEES.EMPLOYEE_ID :"+columnfield.contains("EMPLOYEES.EMPLOYEE_ID"));
        System.out.println(":TIMEGATE: configure method ------------------------- :");
        customreplacement = config.getString(CUSTOMREPLACEMENT_CONFIG);
//        messagedigest = config.getString(MESSAGEDIGEST_CONFIG);
        System.out.println(":TIMEGATE: configure method messagedigest :"+messagedigest);
        System.out.println(":TIMEGATE: configure method messagedigest #########################################:");
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
        for (String field : columnfield) {
            updatedValue.put(field, masked(value.get(field)));
        }
        return newRecord(record, updatedValue);
    }

    private R applyWithSchema(R record) {
//        System.out.println(":TIMEGATE: applyWithSchema class :");
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        System.out.println(":TIMEGATE: applyWithSchema class : Struct Type value :"+value);
        System.out.println(":TIMEGATE: applyWithSchema class : value.schema :"+value.schema());
        final Struct updatedValue = new Struct(value.schema());

//        String

        for (Field field : value.schema().fields()) {
            System.out.println(":TIMEGATE: applyWithSchema class : value.schema().fields() :"+field);
            final Object origFieldValue = value.get(field);
            System.out.println(":TIMEGATE: applyWithSchema class : value.get(field) -> origiFieldValue :"+origFieldValue);
            System.out.println(":TIMEGATE: applyWithSchema class : record.topic() :"+record.topic());
            System.out.println(":TIMEGATE: applyWithSchema class : field.name() :"+field.name());
//            field.name()에 대한 값을 columnfield와 비교 하여 마스크 처리
//            columnfield의 값이 table.column 즉 topic.column

            System.out.println(":TIMEGATE: applyWithSchema class : columnfield :"+columnfield);
            if(columnfield.contains(record.topic()+"."+field.name())){

                System.out.println(":TIMEGATE: applyWithSchema class : record.topic()+field.name() IF TRUE:"+record.topic()+"."+field.name());
                updatedValue.put(field, masked(origFieldValue));
            }else{
                System.out.println(":TIMEGATE: applyWithSchema class : record.topic()+field.name() ELSE FALSE:"+record.topic()+"."+field.name());
                updatedValue.put(field, origFieldValue);
            }
//            updatedValue.put(field, columnfield.contains(record.topic()+"."+field.name()) ? masked(origFieldValue) : origFieldValue);
//            updatedValue.put(field, columnfield.contains(field.name()) ? masked(origFieldValue) : origFieldValue);
        }
        return newRecord(record, updatedValue);
    }

    //    데이터 변환 실행
    private Object masked(Object value) {
        if (value == null) {
            return null;
        }
        if(customreplacement == null){
            System.out.println(":TIMEGATE: customreplacement == null:"+customreplacement);
            return maskWithNullValue(value);
        }else{
            System.out.println(":TIMEGATE: customreplacement != null:"+customreplacement);
            return maskWithCustomReplacement(value, customreplacement);
        }
//        return customreplacement == null ? maskWithNullValue(value) : maskWithCustomReplacement(value, customreplacement);
    }


    private static Object maskWithNullValue(Object value) {
        Object maskedValue = PRIMITIVE_VALUE_MAPPING.get(value.getClass());
        System.out.println(":TIMEGATE: maskWithNullValue :maskedValue:"+maskedValue);
        System.out.println(":TIMEGATE: maskWithNullValue :value.getClass():"+value.getClass());
        if (maskedValue == null) {
            if (value instanceof List)
                maskedValue = Collections.emptyList();
            else if (value instanceof Map)
                maskedValue = Collections.emptyMap();
            else
                throw new DataException("Cannot mask value of type: " + value.getClass());
        }
        return maskedValue;
    }

    /**
     * HashCode()변환 조건 수정
     * @param value
     * @param replacement
     * @return
     */
    private static Object maskWithCustomReplacement(Object value, String replacement) {
        System.out.println(":TIMEGATE: maskWithCustomReplacement value : " + value);
        System.out.println(":TIMEGATE: maskWithCustomReplacement replacement : " + replacement);
        Function<String, ?> replacementMapper = REPLACEMENT_MAPPING_FUNC.get(value.getClass());
        if (replacementMapper == null) {
            throw new DataException("Cannot hashCode() value of type " + value.getClass() + " with custom replacement.");
        }
        try {
            if ("customcigna".equals(replacement)) {
//                return replacementMapper.apply(md.gethashCode(value));
//                System.out.println(":TIMEGATE: maskWithCustomReplacement messagedigest : " + messagedigest);
//                System.out.println(":TIMEGATE: MessageDigestTransform.getTransformMessage : " + MessageDigestTransform.getTransformMessage(value, messagedigest));
                return replacementMapper.apply(MessageDigestTransform.getTransformMessage(value));
            } else {
                return replacementMapper.apply(replacement);
            }
        } catch (NumberFormatException ex) {
            throw new DataException("Unable to convert " + replacement + " (" + replacement.getClass() + ") to number", ex);
        } catch (Exception ex){
            throw new DataException("Unable to convert " + replacement + " (" + replacement.getClass() + ") to xxxxxx", ex);
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


    public static final class Key<R extends ConnectRecord<R>> extends ReplaceHash<R> {

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
            System.out.println(":TIMEGATE: newRecord ===Key=== class : record.topic() :"+record.topic()+",record.kafkaPartition():"+
                    record.kafkaPartition()+",record.keySchema():"+record.keySchema()+",updatedValue:"+updatedValue+",record.valueSchema()"+
                    record.valueSchema()+",record.value():"+record.value());
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

        @Override
        protected R customNewRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static final class Value<R extends ConnectRecord<R>> extends ReplaceHash<R> {

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
            System.out.println(":TIMEGATE: newRecord ===Value=== class : record.topic() :"+record.topic()+",record.kafkaPartition():"+
                    record.kafkaPartition()+",record.keySchema():"+record.keySchema()+",record.key():"+record.key()+",record.valueSchema()"+
                    record.valueSchema()+",updatedValue:"+updatedValue);
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
        }

        @Override
        protected R customNewRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
        }
    }



}

