package io.confluent.connect.s3.storage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStreamReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ChunkedDiskBufferTest {

  private static final Logger log = LoggerFactory.getLogger(ChunkedDiskBufferTest.class);

  private ChunkedDiskBuffer buffer;

  @Before
  public void setup() {
    buffer = new ChunkedDiskBuffer("myBucket", "myKey", 1000);
    File bufferFile = new java.io.File(buffer.chunks.get(0).getFilename());
    assertTrue(bufferFile.exists());
  }

  @After
  public void tearDown() throws Exception {
    buffer.closeBuffer();
    File bufferFile = new java.io.File(buffer.chunks.get(0).getFilename());
    assertFalse(bufferFile.exists());
  }

  @Test
  public void testBufferInit() {
    assertNotNull(buffer);
  }

  @Test
  public void testWriting() throws Exception {
    buffer.write((int) 'p');
    InputStreamReader inputStreamReader = new InputStreamReader(buffer.getInputStream());
    File bufferFile = new java.io.File(buffer.chunks.get(0).getFilename());
    char[] charArray = new char[(int) bufferFile.length()];
    int numRead = inputStreamReader.read(charArray);
    assertEquals(1000, numRead);
    log.info("bufferFile contents: {}", charArray);
    assertEquals('p', charArray[0]);
  }

  @Test
  public void testWritingMultipleChunks() throws Exception {
    for (int i = 0; i < 2999; i++) {
      buffer.write((int) 'x');
    }
    assertEquals(3, buffer.chunks.size());
    // make sure buffer can start a new chunk when needed
    buffer.write(0);
    assertEquals(4, buffer.chunks.size());
  }
}

