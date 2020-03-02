package io.confluent.connect.s3.storage;

import com.amazonaws.services.s3.internal.MD5DigestCalculatingInputStream;
import io.confluent.connect.s3.S3SinkConnectorTestBase;
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

public class ChunkedDiskBufferTest extends S3SinkConnectorTestBase {

  private static final Logger log = LoggerFactory.getLogger(ChunkedDiskBufferTest.class);

  private ChunkedDiskBuffer buffer;

  @Before
  public void setup() throws Exception {
    super.setUp();
    buffer = new ChunkedDiskBuffer("myKey", connectorConfig);
    File bufferFile = new java.io.File(buffer.part.filename());
    assertTrue(bufferFile.exists());
  }

  @After
  public void tearDown() throws Exception {
    buffer.close();
    File bufferFile = new java.io.File(buffer.part.filename());
    assertFalse(bufferFile.exists());
  }

  @Test
  public void testBufferInit() {
    assertNotNull(buffer);
  }

  @Test
  public void testWriting() throws Exception {
    buffer.write('p');
    ChunkedDiskBuffer.UploadPart part = buffer.part;
    part.rewind();
    InputStreamReader inputStreamReader = new InputStreamReader(part.getInputStream());
    File bufferFile = new java.io.File(buffer.part.filename());
    char[] charArray = new char[(int) bufferFile.length()];
    int numRead = inputStreamReader.read(charArray);
    assertEquals(1, numRead);
    assertEquals(1, bufferFile.length());
    log.info("bufferFile size {} contents: {}", charArray.length, charArray);
    assertEquals('p', charArray[0]);
  }

  @Test
  public void testWritingArray() throws Exception {
    byte[] array = new byte[]{0x1, 0x2, 0x3, 0x4};
    buffer.write(array, 0, 4);
    assertEquals(4, buffer.part.numBytesWritten);
  }

  @Test
  public void testMD5Digestion() throws Exception {
    for (int i = 0; i < 10; i++) {
      byte[] array = new byte[10];
      Arrays.fill(array, (byte) ('a' + i));
      buffer.write(array, 0, 10);
    }

    ChunkedDiskBuffer.UploadPart part = buffer.part;
    // now check all 5 streams that we should have
    part.rewind();
    MD5DigestCalculatingInputStream thisMD5stream = new MD5DigestCalculatingInputStream(part.getInputStream());
    InputStreamReader thisInputStreamReader = new InputStreamReader(thisMD5stream);
    char[] thisCharArray = new char[10];
    int thisNumRead = thisInputStreamReader.read(thisCharArray);
    log.info("MD5 digest of this stream: {}", thisMD5stream.getMd5Digest());
    assertEquals(10, thisNumRead);
  }

}

