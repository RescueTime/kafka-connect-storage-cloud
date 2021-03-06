/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.rescuetime.connect.transform;

import jline.internal.Log;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

public class TextShortener<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final Logger log = LoggerFactory.getLogger(TextShortener.class);

  private static final String PURPOSE = "string shortening";

  private Integer maxLength;

  private List<String> fields;

  public interface ConfigName {
    String MAX_LENGTH = "topics";
    String FIELDS = "fields";
  }

  @SuppressWarnings("WeakerAccess")
  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(ConfigName.MAX_LENGTH, ConfigDef.Type.INT,
          15000, ConfigDef.Importance.HIGH,
          "Maximum length (in characters) to allow when shortening strings. Default: 15000.")
      .define(ConfigName.FIELDS, ConfigDef.Type.LIST,
          Collections.emptyList(), ConfigDef.Importance.HIGH,
          "Fields to be shortened. If blank, shortens all fields.");

  @Override
  public R apply(R record) {
    log.debug("Shortening record " + record.toString());

    R processedRecord;
    Schema schema = record.valueSchema();
    if (schema == null) {
      processedRecord = applySchemaless(record);
    } else {
      processedRecord = applyWithSchema(record);
    }
    Log.debug("Record after shortening: " + processedRecord.toString());
    return processedRecord;
  }

  private R applySchemaless(R record) {
    final Map<String, Object> value = requireMap(record.value(), PURPOSE);
    return record.newRecord(record.topic(), record.kafkaPartition(),
        record.keySchema(), record.key(), null,
        updateValue(value),
        record.timestamp());
  }

  private Map<String, Object> updateValue(Map<String, Object> value) {
    final Map<String, Object> updatedValue = new HashMap<>(value.size());
    for (Map.Entry<String, Object> e : value.entrySet()) {
      final Object fieldValue = e.getValue();
      final String fieldName = e.getKey();
      if (fieldValue instanceof Map) {
        // recurse into map structures
        updatedValue.put(fieldName, updateValue(requireMap(fieldValue, PURPOSE)));
      } else if (fieldValue instanceof String && filter(fieldName)) {
        String rawValue = fieldValue.toString();
        updatedValue.put(fieldName, rawValue.substring(0,
            Math.min(maxLength, rawValue.length())));
      } else {
        updatedValue.put(fieldName, fieldValue);
      }
    }
    return updatedValue;
  }

  private boolean filter(String fieldname) {
    return fields.isEmpty() || fields.contains(fieldname);
  }

  private R applyWithSchema(R record) {
    final Struct value = requireStructOrNull(record.value(), PURPOSE);
    if (value == null) {
      return record;
    }
    Struct updatedValue = updateValueWithSchema(record.valueSchema(), value);
    return record.newRecord(record.topic(), record.kafkaPartition(),
        record.keySchema(), record.key(),
        record.valueSchema(), updatedValue,
        record.timestamp());
  }

  private Struct updateValueWithSchema(Schema schema, Struct value) {
    final Struct updatedValue = new Struct(schema);

    for (Field field : value.schema().fields()) {
      final String fieldName = field.name();
      switch (field.schema().type()) {
        case STRING:
          String rawValue = (String) value.get(fieldName);
          if (rawValue != null) {
            if (filter(fieldName)) {
              updatedValue.put(fieldName, value.get(fieldName).toString()
                  .substring(0, Math.min(maxLength, rawValue.length())));
            } else {
              updatedValue.put(fieldName, rawValue);
            }
          } else {
            updatedValue.put(fieldName, null);
          }
          break;
        case STRUCT:
          updatedValue.put(fieldName,
              updateValueWithSchema(field.schema(), value.getStruct(field.name())));
          break;
        default:
          updatedValue.put(field, value.get(fieldName));
      }
    }
    return updatedValue;
  }

  @Override
  public void close() {
  }

  @Override
  public void configure(Map<String, ?> configs) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    maxLength = config.getInt(ConfigName.MAX_LENGTH);
    fields = config.getList(ConfigName.FIELDS);
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

}
