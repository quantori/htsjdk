/*
 * The MIT License
 *
 * Copyright (c) 2016 Daniel Cameron
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package htsjdk.samtools.util;

import htsjdk.HtsjdkTest;
import htsjdk.samtools.seekablestream.SeekableFileStream;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class SequentialBlockCompressedInputStreamTest extends HtsjdkTest {

    private final File BAM_FILE = new File("src/test/resources/htsjdk/samtools/BAMFileIndexTest/index_test.bam");
	private static final File BLOCK_UNCOMPRESSED = new File("src/test/resources/htsjdk/samtools/util/random.bin");
	private static final File BLOCK_COMPRESSED = new File("src/test/resources/htsjdk/samtools/util/random.bin.gz");

    @Test
    public void testSequential() throws Exception {
    	BlockCompressedInputStream sync = new BlockCompressedInputStream(new SeekableFileStream(BAM_FILE));
    	List<byte[]> expected = new ArrayList<>();
    	List<Long> virtualOffset = new ArrayList<>();
    	List<Integer> length = new ArrayList<>();
    	byte[] buffer = new byte[BlockCompressedStreamConstants.MAX_COMPRESSED_BLOCK_SIZE / 2];
    	virtualOffset.add(sync.getFilePointer());
    	int len = sync.read(buffer);
    	length.add(len);
    	while (len > 0) {
    		expected.add(buffer);
    		buffer = new byte[buffer.length];
    		len = sync.read(buffer);
    		length.add(len);
    		virtualOffset.add(sync.getFilePointer());
    	}
    	sync.close();
    	buffer = new byte[buffer.length];
    	List<BlockCompressedInputStream> list = new ArrayList<>();
    	for (int i = 0; i < 8; i++) {
    		list.add(new SequentialBlockCompressedInputStream(new SeekableFileStream(BAM_FILE)));
    	}
    	// read till EOF
    	for (int i = 0; i < expected.size(); i++) {
	    	for (BlockCompressedInputStream async : list) {
	    		len = async.read(buffer);
	    		Assert.assertEquals(len, (int)length.get(i));
	    		Assert.assertEquals(buffer[0], expected.get(i)[0]);
	    	}
    	}

    	for (BlockCompressedInputStream async : list) {
    		async.close();
    	}
    }

	@Test
	public void stream_should_match_uncompressed_stream() throws Exception {
		byte[] uncompressed = Files.readAllBytes(BLOCK_UNCOMPRESSED.toPath());
		try (BlockCompressedInputStream stream = new BlockCompressedInputStream(new FileInputStream(BLOCK_COMPRESSED))) {
			for (int i = 0; i < uncompressed.length; i++) {
				Assert.assertEquals(stream.read(), Byte.toUnsignedInt(uncompressed[i]));
			}
			Assert.assertTrue(stream.endOfBlock());
		}
	}
}
