package io.confluent.connect.s3.storage;

import com.amazonaws.services.s3.internal.MD5DigestCalculatingInputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

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
    File bufferFile = new java.io.File(buffer.streams.get(0).currentChunk().filename());
    assertTrue(bufferFile.exists());
  }

  @After
  public void tearDown() throws Exception {
    buffer.close();
    File bufferFile = new java.io.File(buffer.streams.get(0).chunks.get(0).filename());
    assertFalse(bufferFile.exists());
  }

  @Test
  public void testBufferInit() {
    assertNotNull(buffer);
  }

  @Test
  public void testWriting() throws Exception {
    buffer.write('p');
    ChunkedDiskBuffer.ByteBufferBackedInputStream stream = buffer.getInputStreams().get(0);
    stream.rewind();
    InputStreamReader inputStreamReader = new InputStreamReader(stream);
    File bufferFile = new java.io.File(buffer.streams.get(0).currentChunk().filename());
    char[] charArray = new char[(int) bufferFile.length()];
    int numRead = inputStreamReader.read(charArray);
    assertEquals(10, numRead);
    log.info("bufferFile contents: {}", charArray);
    assertEquals('p', charArray[0]);
  }

  @Test
  public void testWritingMultipleChunks() throws Exception {
    for (int i = 0; i < 29; i++) {
      buffer.write('x');
    }
    assertEquals(2, buffer.streams.get(0).chunks.size());
    assertEquals(1, buffer.streams.get(1).chunks.size());
    // make sure buffer can start a new chunk when needed
    buffer.write(0);
    assertEquals(2, buffer.streams.get(1).chunks.size());
  }

  @Test
  public void testWritingArray() throws Exception {
    byte[] array = new byte[]{0x1, 0x2, 0x3, 0x4};
    buffer.write(array, 0, 4);
    assertEquals(1, buffer.streams.get(0).chunks.size());
  }

  @Test
  public void testWritingMultiChunkedArray() throws Exception {
    for (int i = 0; i < 5; i++) {
      byte[] array = new byte[10];
      Arrays.fill(array, (byte) ('t' + i));
      buffer.write(array, 0, 10);
    }

    assertEquals(3, buffer.streams.size());

    ChunkedDiskBuffer.ByteBufferBackedInputStream stream = buffer.getInputStreams().get(0);
    stream.rewind();
    InputStreamReader inputStreamReader = new InputStreamReader(stream);
    char[] charArray = new char[20];
    int numRead = inputStreamReader.read(charArray);
    assertEquals(20, numRead);
    log.info("bufferFile contents: {}", charArray);
    assertEquals('t', charArray[0]);
    assertEquals('t', charArray[5]);
    assertEquals('u', charArray[10]);
    assertEquals('u', charArray[14]);
  }

  @Test
  public void testWritingMultiPart() throws Exception {
    for (int i = 0; i < 10; i++) {
      byte[] array = new byte[10];
      Arrays.fill(array, (byte) ('a' + i));
      buffer.write(array, 0, 10);
    }

    assertEquals(2, buffer.getInputStreams().get(0).chunks.size());

    List<ChunkedDiskBuffer.ByteBufferBackedInputStream> streams = buffer.getInputStreams();
    assertEquals(5, streams.size());

    char letter = 96;   // a = 97
    for (ChunkedDiskBuffer.ByteBufferBackedInputStream stream : streams) {
      stream.rewind();
      InputStreamReader inputStreamReader = new InputStreamReader(stream);
      char[] charArray = new char[20];
      int numRead = inputStreamReader.read(charArray);
      log.info("bufferFile numRead {}, contents: {}", numRead, charArray);
      for (int i = 0; i < charArray.length; i++) {
        if (i % 10 == 0) {
          letter++;
        }
        assertEquals(letter, charArray[i]);
      }
      assertEquals(20, numRead);
    }
  }

  @Test
  public void testMD5Digestion() throws Exception {
    for (int i = 0; i < 10; i++) {
      byte[] array = new byte[10];
      Arrays.fill(array, (byte) ('a' + i));
      buffer.write(array, 0, 10);
    }

    List<ChunkedDiskBuffer.ByteBufferBackedInputStream> streams = buffer.getInputStreams();
    assertEquals(5, streams.size());

    // now check all 5 streams that we should have
    for (ChunkedDiskBuffer.ByteBufferBackedInputStream stream : streams) {
      assertEquals(2, stream.chunks.size());
      stream.rewind();
      MD5DigestCalculatingInputStream thisMD5stream = new MD5DigestCalculatingInputStream(stream);
      InputStreamReader thisInputStreamReader = new InputStreamReader(thisMD5stream);
      char[] thisCharArray = new char[20];
      int thisNumRead = thisInputStreamReader.read(thisCharArray);
      log.info("MD5 digest of this stream: {}", thisMD5stream.getMd5Digest());
      assertEquals(20, thisNumRead);
    }

  }

}

