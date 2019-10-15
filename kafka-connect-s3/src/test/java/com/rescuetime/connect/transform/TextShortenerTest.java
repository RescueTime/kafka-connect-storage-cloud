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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class TextShortenerTest {
  private TextShortener<SinkRecord> xform = new TextShortener<>();

  @After
  public void teardown() {
    xform.close();
  }

  @Test
  public void schemaless() {
    final Map<String, String> props = new HashMap<>();
    props.put(TextShortener.ConfigName.MAX_LENGTH, "10");
    props.put(TextShortener.ConfigName.FIELDS, "details");
    xform.configure(props);

    final Map<String, Object> value = new HashMap<>();
    value.put("unshortened", "blah blah blah sdlkflksdjfkljsdflkjsdajklfasdklf 03");
    value.put("details", "details will be truncated after 10 chars");
    value.put("count", 5);
    value.put("empty", null);

    final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
    final SinkRecord transformedRecord = xform.apply(record);

    final Map updatedValue = (Map) transformedRecord.value();
    assertEquals(4, updatedValue.size());
    assertEquals("blah blah blah sdlkflksdjfkljsdflkjsdajklfasdklf 03", updatedValue.get("unshortened"));
    assertEquals("details wi", updatedValue.get("details"));
    assertEquals(5, updatedValue.get("count"));
    assertNull(updatedValue.get("empty"));
  }

  @Test
  public void withSchema() {
    final Map<String, String> props = new HashMap<>();
    props.put(TextShortener.ConfigName.MAX_LENGTH, "10");
    props.put(TextShortener.ConfigName.FIELDS, "details");
    xform.configure(props);

    final Schema schema = SchemaBuilder.struct()
        .field("unshortened", Schema.STRING_SCHEMA)
        .field("details", Schema.STRING_SCHEMA)
        .field("count", Schema.INT32_SCHEMA)
        .field("empty", Schema.OPTIONAL_INT32_SCHEMA)
        .build();

    final Struct value = new Struct(schema);
    value.put("unshortened", "blah blah blah sdlkflksdjfkljsdflkjsdajklfasdklf 03");
    value.put("details", "details will be truncated after 10 chars");
    value.put("count", 5);
    value.put("empty", null);

    final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
    final SinkRecord transformedRecord = xform.apply(record);

    final Struct updatedValue = (Struct) transformedRecord.value();
    assertEquals(4, updatedValue.schema().fields().size());
    assertEquals("blah blah blah sdlkflksdjfkljsdflkjsdajklfasdklf 03", updatedValue.get("unshortened"));
    assertEquals("details wi", updatedValue.get("details"));
    assertEquals(5, updatedValue.get("count"));
    assertNull(updatedValue.get("empty"));
  }

  @Test
  public void emptyMessage() {
    final Map<String, String> props = new HashMap<>();
    xform.configure(props);
    final Map<String, Object> value = new HashMap<>();
    final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
    final SinkRecord transformedRecord = xform.apply(record);

    final Map updatedValue = (Map) transformedRecord.value();
    assertTrue(updatedValue.isEmpty());

  }

  @Test
  public void nestedMapNoSchema() {
    final Map<String, String> props = new HashMap<>();
    props.put(TextShortener.ConfigName.MAX_LENGTH, "10");
    props.put(TextShortener.ConfigName.FIELDS, "details");
    xform.configure(props);

    final Map<String, Object> value = new HashMap<>();
    final Map<String, Object> subtree = new HashMap<>();
    final Map<String, Object> subsubtree = new HashMap<>();
    subtree.put("details", "short");
    subsubtree.put("details", "subsubtreedetails will be truncated after 10 chars");
    subtree.put("subsubtree", subsubtree);
    value.put("subtree", subtree);

    final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
    final SinkRecord transformedRecord = xform.apply(record);

    final Map updatedValue = (Map) transformedRecord.value();
    assertEquals(1, updatedValue.size());
    final Map updatedSubtree = (Map) updatedValue.get("subtree");
    assertEquals("short", updatedSubtree.get("details"));
    final Map updatedSubsubtree = (Map) updatedSubtree.get("subsubtree");
    assertEquals("subsubtree", updatedSubsubtree.get("details"));
  }

  @Test
  public void nestedMapWithSchema() {
    final Map<String, String> props = new HashMap<>();
    props.put(TextShortener.ConfigName.MAX_LENGTH, "10");
    props.put(TextShortener.ConfigName.FIELDS, "details");
    xform.configure(props);

    final Schema subsubTreeSchema = SchemaBuilder.struct()
        .field("details", Schema.STRING_SCHEMA)
        .build();
    final Schema subtreeSchema = SchemaBuilder.struct()
        .field("subsubtree", subsubTreeSchema)
        .field("details", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    final Schema rootSchema = SchemaBuilder.struct()
        .field("subtree", subtreeSchema)
        .build();

    final Struct subsubTree = new Struct(subsubTreeSchema);
    subsubTree.put("details", "short");
    final Struct subTree = new Struct(subtreeSchema);
    subTree.put("subsubtree", subsubTree);
    subTree.put("details", "a string on the subtree that should be trimmed");
    final Struct root = new Struct(rootSchema);
    root.put("subtree", subTree);

    final SinkRecord record = new SinkRecord("test", 0, null, null, rootSchema, root, 0);
    final SinkRecord transformedRecord = xform.apply(record);

    final Struct updatedRoot = (Struct) transformedRecord.value();
    final Struct updatedSubtree = (Struct) updatedRoot.get("subtree");
    final Struct updatedSubSubtree = (Struct) updatedSubtree.get("subsubtree");

    Log.info("updatedSubSubtree = " + updatedSubSubtree.toString());
    assertEquals("short", updatedSubSubtree.get("details"));
    assertEquals("a string o", updatedSubtree.get("details"));
  }

}
