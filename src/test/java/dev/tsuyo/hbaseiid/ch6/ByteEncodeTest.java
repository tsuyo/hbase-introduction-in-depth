package dev.tsuyo.hbaseiid.ch6;

import org.apache.hadoop.hbase.types.*;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class ByteEncodeTest {

  @Test
  void testSingle() {
    byte[] bytes = new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
    byte[] expAscBytes = new byte[] {(byte) 56, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
    byte[] expDescBytes =
        new byte[] {(byte) -57, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) -1};
    byte[] expIntBytes = new byte[] {(byte) 0, (byte) 0, (byte) 0, (byte) 100};

    SimplePositionedMutableByteRange ascByteRange =
        new SimplePositionedMutableByteRange(OrderedBlob.ASCENDING.encodedLength(bytes));
    OrderedBlob.ASCENDING.encode(ascByteRange, bytes);
    byte[] ascBytes = ascByteRange.getBytes();
    assertArrayEquals(expAscBytes, ascBytes);

    SimplePositionedMutableByteRange descByteRange =
        new SimplePositionedMutableByteRange(OrderedBlob.DESCENDING.encodedLength(bytes));
    OrderedBlob.DESCENDING.encode(descByteRange, bytes);
    byte[] descBytes = descByteRange.getBytes();
    assertArrayEquals(expDescBytes, descBytes);

    int intValue = 100;
    RawInteger rawInteger = new RawInteger();
    SimplePositionedByteRange rawIntByteRange =
        new SimplePositionedByteRange(rawInteger.encodedLength(intValue));
    rawInteger.encode(rawIntByteRange, intValue);
    byte[] rawIntBytes = rawIntByteRange.getBytes();
    assertArrayEquals(expIntBytes, rawIntBytes);
  }

  @Test
  void testMultiple() {
    byte[] expBytes = new byte[] {43, -128, 0, 0, 10, 44, -128, 0, 0, 0, 0, 0, 0, 20, 0, 30};

    Struct struct =
        new StructBuilder()
            .add(OrderedInt32.ASCENDING)
            .add(OrderedInt64.ASCENDING)
            .add(new RawShort())
            .toStruct();

    int userId = 10;
    // long timestamp = System.currentTimeMillis();
    long timestamp = 20L;
    short age = 30;

    Object[] values = new Object[] {userId, timestamp, age};
    SimplePositionedMutableByteRange positionedByteRange =
        new SimplePositionedMutableByteRange(struct.encodedLength(values));
    struct.encode(positionedByteRange, values);
    byte[] bytes = positionedByteRange.getBytes();
    assertArrayEquals(expBytes, bytes);
  }
}
