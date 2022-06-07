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

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.Date;
import java.util.function.Function;
import java.util.logging.Logger;

import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;


/**
 * 암호화 변환 대상이 되는 필드의 데이터를 암호화 한다.
 * cipher.type에 따라 SHA-256, AES-256으로 암호화 한다.
 * AES-256의 경우 데이터 건 별 KEY생성 또는 전체 데이터에 하나의 KEY를 생성 할 예정(요구사항에 따라 변경예정)
 * AES-256의 경우 암호화시 생성한 KEY를 어떻게 처리 할 것인가? DB 혹은 파일등... (요구사항에 따라 변경예정)
 * @param <R>
 */
public abstract class LinaReplaceCipherTest<R extends ConnectRecord<R>> implements Transformation<R> {
    public static Logger logger = Logger.getLogger(LinaReplaceCipher.class.getName());
//    private static org.apache.log4j.Logger log = Logger.getLogger(LinaReplaceCipher.class.getName());

    public static final String OVERVIEW_DOC =
            "Cipher specified column fields with a valid null value for the field type (i.e. 0, false, empty string, and so on)."
                    + "<p/>For numeric and string fields, an optional replacement value can be specified that is converted to the correct type."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + LinaReplaceCipherTest.Key.class.getName()
                    + "</code>) or value (<code>" + LinaReplaceCipherTest.Value.class.getName() + "</code>).";

    private SimpleConfig config;
    private static final String DB_DRIVER_CONFIG = "db.driver";
    private static final String DB_DRIVER_DEFAULT = "oracle.jdbc.driver.OracleDriver";
    private static final String DB_IP_CONFIG = "db.ip";
    //    private static final String DB_IP_DEFAULT = "";
    private static final String DB_PORT_CONFIG = "db.port";
    private static final String DB_PORT_DEFAULT = "1521";
    private static final String DB_SCHEMA_CONFIG = "db.schema";
    //    private static final String DB_SCHEMA_DEFAULT = "orcl";
    private static final String DB_USER_CONFIG = "db.user";
    //    private static final String DB_USER_DEFAULT = "";
    private static final String DB_PWD_CONFIG = "db.pwd";
//    private static final String DB_PWD_DEFAULT = "";
    private static final String DB_INSNAME_CONFIG ="db.insname";

    //    oriField(주민등록 번호 필드) : 9510231XXXXXXXXXXXXX(34자리) -> oriField [전체 암호화], addField1(생년월 4자리) [9510], addfield2(성별 1자리) [1]
    private static final String COLUMN_RRNO_CONFIG = "column.rrno";
    private static final String COLUMN_RRNO_DEFAULT = "RRNO";
    //    oriField(주소 필드) : 서울시 은평구 응암동 132번지 -> oriField [전체 암호화], addField1 [서울시 은평구 응암동]
    private static final String COLUMN_ADDR_CONFIG = "column.addr";
    private static final String COLUMN_ADDR_DEFAULT = "ADDR";


    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DB_DRIVER_CONFIG, ConfigDef.Type.STRING, DB_DRIVER_DEFAULT, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH, "The database driver class where the encryption target column data is stored.")
            .define(DB_IP_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH, "The database ip where the encryption target column data is stored.")
            .define(DB_PORT_CONFIG, ConfigDef.Type.STRING, DB_PORT_DEFAULT, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH, "The database port where the encryption target column data is stored.")
            .define(DB_SCHEMA_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH, "The database schema(sid) where the encryption target column data is stored.")
            .define(DB_USER_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH, "The database user where the encryption target column data is stored.")
            .define(DB_PWD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH, "The database password where the encryption target column data is stored.")
            .define(DB_INSNAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH, "The database password where the encryption target column data is stored.")
            .define(COLUMN_RRNO_CONFIG, ConfigDef.Type.STRING, COLUMN_RRNO_DEFAULT, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH, "The database password where the encryption target column data is stored.")
            .define(COLUMN_ADDR_CONFIG, ConfigDef.Type.STRING, COLUMN_ADDR_DEFAULT, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH, "The database password where the encryption target column data is stored.")
            ;


    private static final String PURPOSE = "Data encription replacement for LINA.";

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

    private static final class InsertionSpec {
        final String name;
        final boolean optional;

        private InsertionSpec(String name, boolean optional) {
            this.name = name;
            this.optional = optional;
        }

        public static InsertionSpec parse(String spec) {
            if (spec == null) return null;
            if (spec.endsWith("?")) {
                return new InsertionSpec(spec.substring(0, spec.length() - 1), true);
            }
            if (spec.endsWith("!")) {
                return new InsertionSpec(spec.substring(0, spec.length() - 1), false);
            }
            return new InsertionSpec(spec, true);
        }
    }

    //    암호화 대상 정보를 획득을 위한 DB 정보
    private static String dbDriver;
    private static String dbIp;
    private static String dbPort;
    private static String dbSchema;
    private static String dbUser;
    private static String dbPwd;
    private static String dbInsName;

    //    컬럼 추가 대상 정보
    private static String columnRrno;
    private static String columnAddr;

    //    암호화 대상 컬럼 정보
    private Map<String, String[]> infoHmap = new HashMap<>();


    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(final Map<String, ?> props) {
        this.config = new SimpleConfig(CONFIG_DEF, props);
        dbDriver = config.getString(DB_DRIVER_CONFIG);
        dbIp = config.getString(DB_IP_CONFIG);
        dbPort = config.getString(DB_PORT_CONFIG);
        dbSchema = config.getString(DB_SCHEMA_CONFIG);
        dbUser = config.getString(DB_USER_CONFIG);
        dbPwd = config.getString(DB_PWD_CONFIG);
        dbInsName = config.getString(DB_INSNAME_CONFIG);

        logger.info(":TIMEGATE: configure logger.info");
        logger.log(SEVERE,":TIMEGATE: configure logger.log.level severe");
        logger.log(INFO,":TIMEGATE: configure logger.log.level info");

        columnRrno = config.getString(COLUMN_RRNO_CONFIG);
        columnAddr = config.getString(COLUMN_ADDR_CONFIG);
        System.out.println(":TIMEGATE: configure method TEST :");
        System.out.println(":TIMEGATE: DB접속정보 :"+dbIp+":"+dbPort+":"+dbSchema+":"+dbUser+":"+dbPwd);
        infoHmap = TargetColumnInfo.getColumnInfo(dbIp, dbPort, dbSchema, dbUser, dbPwd, dbDriver, dbInsName);
        System.out.println(":TIMEGATE: configure method infoHmap :"+infoHmap);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));


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
            System.out.println(":TIMEGATE: apply method 스키마 있음 record :"+record);
            return applyWithSchema(record);
        }
    }


    //    스키마가 없는 데이터의 변환
    private R applySchemaless(R record) {
//        System.out.println(":TIMEGATE: applySchemaless class :");
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final HashMap<String, Object> updatedValue = new HashMap<>(value);
        for (String field : infoHmap.keySet()) {
            updatedValue.put(field, ciphered(value.get(field), infoHmap.get(field)[0], infoHmap.get(field)[1], infoHmap.get(field)[2]));
//            return newRecord(record, null, updatedValue);
        }
        return newRecord(record, updatedValue);
    }

    private R applyWithSchema(R record) {
        System.out.println(":TIMEGATE: applyWithSchema class :");
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
//        System.out.println(":TIMEGATE: applyWithSchema class : Struct Type value :"+value);
//        System.out.println(":TIMEGATE: applyWithSchema class : value.schema :"+value.schema());
//        value.put(String fieldName, Object value)
//        value.put(Field field, Object. value)

        Schema defaultSchema = value.schema();
//        스키마 업데이트가 필요한 항목일때 업데이트 함
        Schema updatedSchemaRrno = null;
//        Schema updatedSchemaRrno = schemaUpdateCache.get(value.schema());
//        if (updatedSchemaRrno == null) {
//            updatedSchemaRrno = rrnoUpdatedSchema(value.schema(),"RRNO");
//            schemaUpdateCache.put(value.schema(), updatedSchemaRrno);
//        }

        Schema updatedSchemaAddr = null;
//        Schema updatedSchemaAddr = schemaUpdateCache.get(value.schema());
//        if (updatedSchemaAddr == null) {
//            updatedSchemaAddr = addrUpdatedSchema(value.schema(),"ADDR");
//            schemaUpdateCache.put(value.schema(), updatedSchemaAddr);
//        }


//        기존 스키마 사용
        final Struct updatedValue = new Struct(defaultSchema);
//        변경된 스키마 rrno
//        final Struct updatedValueRrno = null;
        Struct updatedValueRrno = null;
//        변경된 스키마 addr
//        final Struct updatedValueAddr = null;
        Struct updatedValueAddr = null;

        System.out.println(":TIMEGATE: applyWithSchema class : value.schema().fields() :"+value.schema().fields());
        for (Field field : value.schema().fields()) {
            updatedValueRrno.put(field, updatedValue.get(field));

            System.out.println(":TIMEGATE: applyWithSchema class : value.schema().fields() :"+field);

            final Object origFieldValue = value.get(field);
            System.out.println(":TIMEGATE: applyWithSchema class : value.get(field) -> origiFieldValue :"+origFieldValue);
            System.out.println(":TIMEGATE: applyWithSchema class : record.topic() :"+record.topic());
            System.out.println(":TIMEGATE: applyWithSchema class : field.name() :"+field.name());
            /**
             * 기본적으로 대상 필드의 데이터는 전체 암호화
             * 주민 번호는 필드 2개 추가
             * 주소는 필드 1개 추가
             */

            if(infoHmap.containsKey(field.name())){
                System.out.println(":TIMEGATE: applyWithSchema class : record.topic()+field.name() DB리스트 포함 :"+record.topic()+"."+field.name());
                updatedValue.put(field, ciphered(origFieldValue, infoHmap.get(field.name())[0], infoHmap.get(field.name())[1], infoHmap.get(field.name())[2]));

//                주민번호일 경우 컬럼 두개 추가 및 기존 컬럼 전체 암호화
                if(Arrays.asList(infoHmap.get(field.name())).contains(columnRrno)){
                    System.out.println(":TIMEGATE: applyWithSchema class : if(Arrays.asList(infoHmap.get(field.name())).contains(columnRrno)) :"+field.name()+"::"+columnRrno);
                    updatedSchemaRrno = schemaUpdateCache.get(value.schema());
                    if (updatedSchemaRrno == null) {
                        updatedSchemaRrno = rrnoUpdatedSchema(value.schema(),field.name());
                        schemaUpdateCache.put(value.schema(), updatedSchemaRrno);
                        updatedValueRrno = new Struct(updatedSchemaRrno);
                        System.out.println(":TIMEGATE: applyWithSchema class : updatedSchemaRrno 스키마 업데이트 :"+value.schema()+"::"+updatedSchemaRrno);
                    }


                    updatedValueRrno.put(field.name()+"1", origFieldValue.toString().substring(0,4));
                    updatedValueRrno.put(field.name()+"2", origFieldValue.toString().substring(6,7));

                    System.out.println(":TIMEGATE: applyWithSchema class : updatedValueRrno.put(field.name()+\"1,2\", origFieldValue :"+origFieldValue.toString().substring(0,4)+"::"+origFieldValue.toString().substring(6,7));

//                    return newRecord(record, updatedSchemaRrno, updatedValueRrno);

//                주소일 경우 컬럼 한개 추가 및 기존 컬럼 전체 암호화
                }else if(Arrays.asList(infoHmap.get(field.name())).contains(columnAddr)){
                    System.out.println(":TIMEGATE: applyWithSchema class : if(Arrays.asList(infoHmap.get(field.name())).contains(columnAddr)) :"+field.name()+"::"+columnAddr);
                    updatedSchemaAddr = schemaUpdateCache.get(value.schema());
                    if (updatedSchemaAddr == null) {
                        updatedSchemaAddr = addrUpdatedSchema(value.schema(),field.name());
                        schemaUpdateCache.put(value.schema(), updatedSchemaAddr);
                        updatedValueAddr = new Struct(updatedSchemaAddr);
                        System.out.println(":TIMEGATE: applyWithSchema class : updatedSchemaAddr 스키마 업데이트 :"+value.schema()+"::"+updatedSchemaAddr);
                    }

                    String[] juso = DataUtils.getAddress(origFieldValue.toString());
                    if ("NOMATCH".equals(juso[0])){
                        updatedValueAddr.put(field.name()+"1", origFieldValue.toString().substring(0,13));
                    }else{
                        updatedValueAddr.put(field.name()+"1", juso[0]);
                    }

//                    return newRecord(record, updatedSchemaAddr, updatedValueAddr);

                }
            }else{
                System.out.println(":TIMEGATE: applyWithSchema class : record.topic()+field.name() ELSE FALSE:"+record.topic()+"."+field.name());
                updatedValue.put(field, origFieldValue);

            }
        }
//        return newRecord(record, updatedSchemaAddr, updatedValueAddr);
        return newRecord(record, updatedValue);
    }



    //    데이터 변환 실행
    private Object ciphered(Object value, String dmn_pnm, String encrp_cd, String encrp_key) {
        if (value == null) {
            return null;
        }
        return cipherWithCustomTransforms(value, dmn_pnm, encrp_cd, encrp_key);
//        if(customreplacement == null){
//            System.out.println(":TIMEGATE: customreplacement == null:"+customreplacement);
//            return cipherWithNullValue(value);
//        }else{
//            System.out.println(":TIMEGATE: customreplacement != null:"+customreplacement);
//            return cipherWithCustomReplacement(value, customreplacement);
//        }
//        return customreplacement == null ? cipherWithNullValue(value) : cipherWithCustomReplacement(value, customreplacement);
    }



    /**
     * 데이터 Value를 암호화 변환 하는 Method임을 확인
     * @param value
     * @return 암호화 된 Value를 리턴
     */
    private static Object cipherWithCustomTransforms(Object value, String dmn_pnm, String encrp_cd, String encrp_key) {
        System.out.println(":TIMEGATE: cipherWithCustomReplacement value : " + value);
        Function<String, ?> replacementMapper = REPLACEMENT_MAPPING_FUNC.get(value.getClass());
        if (replacementMapper == null) {
            throw new DataException("Cannot Encription value of type " + value.getClass() + " with custom replacement.");
        }
        try {
//            암호화 타입 및 설정에 따른 암호화
            String encryptedValue = (DefaultCipher.transformType(value, encrp_cd, encrp_key)).toString();
            System.out.println(":TIMEGATE: cipherWithCustomTransforms cipherType : " + encrp_cd);
            System.out.println(":TIMEGATE: cipherWithCustomTransforms encrp_key : " + encrp_key);
            return replacementMapper.apply(encryptedValue);
//            return replacementMapper.apply(MessageDigestTransform.getTransformMessage(value));
        } catch (NumberFormatException ex) {
            throw new DataException("Unable to convert SHA256 to number", ex);
        } catch (Exception ex){
            throw new DataException("Unable to convert SHA256 to other types", ex);
        }
    }

    private Schema rrnoUpdatedSchema(Schema schema, String insertField) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        System.out.println(":TIMEGATE: rrnoUpdatedSchema Method :"+schema+"::"+insertField);
        for (Field field : schema.fields()) {
            System.out.println(":TIMEGATE: AS-IS field.name(), field.schema() :"+field.name()+"::"+field.schema());
            builder.field(field.name(), field.schema());
        }
        builder.field(insertField+"1", Schema.OPTIONAL_STRING_SCHEMA);
        builder.field(insertField+"2", Schema.OPTIONAL_STRING_SCHEMA);
        for (Field field : builder.build().fields()) {
            System.out.println(":TIMEGATE: TO-BE field.name(), field.schema() :"+field.name()+"::"+field.schema());

        }
        System.out.println(":TIMEGATE: rrnoUpdatedSchema Method ================ end");
//        builder.field(insertField, staticField.optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
        return builder.build();
    }

    private Schema addrUpdatedSchema(Schema schema, String insertField) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }
        builder.field(insertField+"1", Schema.STRING_SCHEMA);

        return builder.build();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }


    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R base, Object value);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);


    public static final class Key<R extends ConnectRecord<R>> extends LinaReplaceCipherTest<R> {

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
            System.out.println(":TIMEGATE: newRecord ===Key1=== class : record.topic() :"+record.topic()+",record.kafkaPartition():"+
                    record.kafkaPartition()+",record.keySchema():"+record.keySchema()+",updatedValue:"+updatedValue+",record.valueSchema()"+
                    record.valueSchema()+",record.value():"+record.value());
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            System.out.println(":TIMEGATE: newRecord ===Key2=== class : record.topic() :"+record.topic()+",record.kafkaPartition():"+
                    record.kafkaPartition()+",updatedSchema:"+updatedSchema+",updatedValue:"+updatedValue+",record.valueSchema()"+
                    record.valueSchema()+",record.value():"+record.value());
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }


    }

    public static final class Value<R extends ConnectRecord<R>> extends LinaReplaceCipherTest<R> {

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
            System.out.println(":TIMEGATE: newRecord ===Value1=== class : record.topic() :"+record.topic()+",record.kafkaPartition():"+
                    record.kafkaPartition()+",record.keySchema():"+record.keySchema()+",record.key():"+record.key()+",record.valueSchema():"+
                    record.valueSchema()+",updatedValue:"+updatedValue);
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {

            for (Field field : updatedSchema.fields()) {
                System.out.println(":TIMEGATE: newRecord === field.name(), field.schema() :"+field.name()+"::"+field.schema());
            }
            System.out.println(":TIMEGATE: newRecord ===Value2=== class : record.topic() :"+record.topic()+",record.kafkaPartition():"+
                    record.kafkaPartition()+",record.keySchema():"+record.keySchema()+",record.key():"+record.key()+",updatedSchema:"+
                    updatedSchema+",updatedValue:"+updatedValue);
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }



}


