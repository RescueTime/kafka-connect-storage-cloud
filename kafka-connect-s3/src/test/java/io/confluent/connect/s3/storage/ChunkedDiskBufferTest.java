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
    File bufferFile = new java.io.File(buffer.streams.get(0).filename());
    assertTrue(bufferFile.exists());
  }

  @After
  public void tearDown() throws Exception {
    buffer.close();
    File bufferFile = new java.io.File(buffer.streams.get(0).filename());
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
    File bufferFile = new java.io.File(buffer.streams.get(0).filename());
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
    assertEquals(3, buffer.streams.size());
    // make sure buffer can start a new chunk when needed
    buffer.write(0);
    assertEquals(4, buffer.streams.size());
  }

  @Test
  public void testWritingArray() throws Exception {
    byte[] array = new byte[]{0x1, 0x2, 0x3, 0x4};
    buffer.write(array, 0, 4);
    assertEquals(1, buffer.streams.size());
  }

  @Test
  public void testWritingMultiPart() throws Exception {
    for (int i = 0; i < 10; i++) {
      byte[] array = new byte[10];
      Arrays.fill(array, (byte) ('a' + i));
      buffer.write(array, 0, 10);
    }

    assertEquals(10, buffer.getInputStreams().size());

    char letter = 96;   // a = 97
    for (ChunkedDiskBuffer.ByteBufferBackedInputStream stream : buffer.getInputStreams()) {
      stream.rewind();
      InputStreamReader inputStreamReader = new InputStreamReader(stream);
      char[] charArray = new char[10];
      int numRead = inputStreamReader.read(charArray);
      log.info("bufferFile numRead {}, contents: {}", numRead, charArray);
      for (int i = 0; i < charArray.length; i++) {
        if (i % 10 == 0) {
          letter++;
        }
        assertEquals(letter, charArray[i]);
      }
      assertEquals(10, numRead);
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
    assertEquals(10, streams.size());

    // now check all 5 streams that we should have
    for (ChunkedDiskBuffer.ByteBufferBackedInputStream stream : streams) {
      stream.rewind();
      MD5DigestCalculatingInputStream thisMD5stream = new MD5DigestCalculatingInputStream(stream);
      InputStreamReader thisInputStreamReader = new InputStreamReader(thisMD5stream);
      char[] thisCharArray = new char[10];
      int thisNumRead = thisInputStreamReader.read(thisCharArray);
      log.info("MD5 digest of this stream: {}", thisMD5stream.getMd5Digest());
      assertEquals(10, thisNumRead);
    }

  }

}

