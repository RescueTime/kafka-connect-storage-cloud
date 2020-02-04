package io.confluent.connect.s3.storage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ChunkedDiskBufferTest {

  private static final Logger log = LoggerFactory.getLogger(ChunkedDiskBufferTest.class);

  private ChunkedDiskBuffer buffer;

  @Before
  public void setup() {
    buffer = new ChunkedDiskBuffer("myBucket", "myKey", 10);
    File bufferFile = new java.io.File(buffer.chunks.get(0).filename());
    assertTrue(bufferFile.exists());
  }

  @After
  public void tearDown() throws Exception {
    buffer.close();
    File bufferFile = new java.io.File(buffer.chunks.get(0).filename());
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
    File bufferFile = new java.io.File(buffer.chunks.get(0).filename());
    char[] charArray = new char[(int) bufferFile.length()];
    int numRead = inputStreamReader.read(charArray);
    assertEquals(10, numRead);
    log.info("bufferFile contents: {}", charArray);
    assertEquals('p', charArray[0]);
  }

  @Test
  public void testWritingMultipleChunks() throws Exception {
    for (int i = 0; i < 29; i++) {
      buffer.write((int) 'x');
    }
    assertEquals(3, buffer.chunks.size());
    // make sure buffer can start a new chunk when needed
    buffer.write(0);
    assertEquals(4, buffer.chunks.size());
  }

  @Test
  public void testWritingArray() throws Exception {
    byte[] array = new byte[] {0x1, 0x2, 0x3, 0x4};
    buffer.write(array, 0, 4);
    assertEquals(1, buffer.chunks.size());
  }

  @Test
  public void testWritingMultiChunkedArray() throws Exception {
    for (int i = 0; i < 5; i++) {
      byte[] array = new byte[10];
      Arrays.fill(array, (byte) ('t' + i));
      buffer.write(array, 0, 10);
    }

    assertEquals(6, buffer.chunks.size());

    InputStreamReader inputStreamReader = new InputStreamReader(buffer.getInputStream());
    char[] charArray = new char[50];
    int numRead = inputStreamReader.read(charArray);
    assertEquals(50, numRead);
    log.info("bufferFile contents: {}", charArray);
    assertEquals('t', charArray[0]);
    assertEquals('t', charArray[5]);
    assertEquals('u', charArray[10]);
    assertEquals('u', charArray[14]);
    assertEquals('v', charArray[20]);
    assertEquals('v', charArray[27]);
    assertEquals('w', charArray[30]);
    assertEquals('w', charArray[33]);
    assertEquals('x', charArray[40]);
    assertEquals('x', charArray[49]);
  }

}

