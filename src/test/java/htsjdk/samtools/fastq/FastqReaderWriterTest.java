package htsjdk.samtools.fastq;

import htsjdk.HtsjdkTest;
import htsjdk.samtools.SAMUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FastqReaderWriterTest extends HtsjdkTest {

    private static final Random RANDOM = new Random();

    private static final char[] BASES = {'A', 'C', 'G', 'T'};

    private static final String FASTQ_WITH_BLANK_LINES = String.join("\n",
            "",
            "@SL-XBG:1:1:4:1663#0/2",
            "NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN",
            "+SL-XBG:1:1:4:1663#0/2",
            "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");

    /** Make a temporary file that will get cleaned up at the end of testing. */
    private static Path makeTempFile(String prefix, String suffix) throws IOException {
        Path path = Files.createTempFile(prefix, suffix);
        path.toFile().deleteOnExit();
        return path;
    }

    private static Path makeTempFastq() throws IOException {
        return makeTempFile("test.", ".fastq");
    }

    /** Generates a random string of bases of the desired length. */
    private static String bases(int length) {
        char[] chs = new char[length];
        IntStream.range(0, length).forEach(i -> chs[i] = BASES[RANDOM.nextInt(BASES.length)]);
        return new String(chs);
    }

    /** Generates a FastqRecord with random bases at a given length. */
    private static FastqRecord fq(String name, int length, int qual) {
        return new FastqRecord(name, bases(length), "",
                IntStream.range(0, length)
                        .map(value -> SAMUtils.phredToFastq(qual))
                        .mapToObj(c -> String.valueOf((char) c))
                        .collect(Collectors.joining()));
    }

    private static FastqRecord fq(String name, int length) {
        return fq(name, length, 30);
    }

    @Test
    public void testWriteFourLinesPerRecord() throws Exception {
        Path path = makeTempFastq();
        List<FastqRecord> recs = Arrays.asList(fq("q1", 50), fq("q2", 48), fq("q3", 55));

        try (FastqWriter out = new FastqWriterFactory().newWriter(path.toFile())) {
            recs.forEach(out::write);
        }

        List<String> lines = Files.readAllLines(path);
        Assert.assertEquals(lines.size(), 12);

        Assert.assertEquals(lines.get(0), "@q1");
        Assert.assertEquals(lines.get(1), recs.get(0).getReadString());
        Assert.assertEquals(lines.get(4), "@q2");
        Assert.assertEquals(lines.get(5), recs.get(1).getReadString());
        Assert.assertEquals(lines.get(8), "@q3");
        Assert.assertEquals(lines.get(9), recs.get(2).getReadString());
    }

    @Test
    public void testWriteRecordSingleBase() throws Exception {
        Path path = makeTempFastq();
        try (FastqWriter out = new FastqWriterFactory().newWriter(path.toFile())) {
            out.write(fq("q1", 1));
        }
        List<String> lines = Files.readAllLines(path);
        lines.forEach(System.out::println);
        Assert.assertEquals(lines.get(1).length(), 1);
        Assert.assertEquals(lines.get(3).length(), 1);
    }

    @Test
    public void testWriteRecordZeroLengthBasesAndQuals() throws Exception {
        Path path = makeTempFastq();
        try (FastqWriter out = new FastqWriterFactory().newWriter(path.toFile())) {
            out.write(fq("q1", 0));
        }
        List<String> lines = Files.readAllLines(path);
        Assert.assertEquals(lines.get(1).length(), 0);
        Assert.assertEquals(lines.get(3).length(), 0);
    }

    @Test
    public void testReadFileWrittenByWriter() throws Exception {
        Path path = makeTempFastq();
        List<FastqRecord> recs = Arrays.asList(fq("q1", 50), fq("q2", 48), fq("q3", 55));

        try (FastqWriter out = new FastqWriterFactory().newWriter(path.toFile())) {
            recs.forEach(out::write);
        }

        try (FastqReader in = new FastqReader(path.toFile())) {
            recs.forEach(rec -> Assert.assertEquals(in.next(), rec));
        }
    }

    @Test(expectedExceptions = Exception.class)
    public void testReadGarbledFastq() {
        String fastq = String.join("\n",
                "@q1",
                "AACCGGTT",
                "+",
                "########",
                "@q2",
                "ACGT",
                "####");

        try (FastqReader in = new FastqReader(null, new BufferedReader(new StringReader(fastq)))) {
            in.next();
        }
    }

    @Test(expectedExceptions = Exception.class)
    public void testReadMissingFile() {
        new FastqReader(new File("/some/path/that/shouldnt/exist.fq"));
    }

    @Test
    public void testReadEmptyFile() throws Exception {
        Path path = makeTempFastq();
        int records = 0;
        try (FastqReader in = new FastqReader(path.toFile())) {
            while (in.hasNext()) {
                records++;
                in.next();
            }
        }
        Assert.assertEquals(records, 0);
    }

    @Test
    public void testReaderSkipBlankLines() {
        int records = 0;
        try (FastqReader in = new FastqReader(null, new BufferedReader(new StringReader(FASTQ_WITH_BLANK_LINES)), true)) {
            while (in.hasNext()) {
                records++;
                in.next();
            }
        }
        Assert.assertEquals(records, 1);
    }

    @Test(expectedExceptions = Exception.class)
    public void testReaderBlankLinesFail() {
        try (FastqReader in = new FastqReader(null, new BufferedReader(new StringReader(FASTQ_WITH_BLANK_LINES)), false)) {
            in.next();
        }
    }

    @DataProvider
    public Iterator<Object[]> getTruncatedFile() {
        List<String> fastq = Arrays.asList(
                "@q1",
                "AACCGGTT",
                "+",
                "########");
        return IntStream.range(1, 4).mapToObj(n ->
                new Object[] { fastq.stream().limit(n).collect(Collectors.joining("\n")) })
                .collect(Collectors.toList()).iterator();
    }

    @Test(expectedExceptions = Exception.class, dataProvider = "getTruncatedFile")
    public void testReaderTruncatedFileFail(String fastq) {
        try (FastqReader in = new FastqReader(null, new BufferedReader(new StringReader(fastq)))) {
            while (in.hasNext()) {
                in.next();
            }
        }
    }

    @Test(expectedExceptions = Exception.class)
    public void testReaderSeqQualDifferentLengthsFail() {
        String fastq = String.join("\n",
                "@q1",
                "AACC",
                "+",
                "########");
        try (FastqReader in = new FastqReader(null, new BufferedReader(new StringReader(fastq)))) {
            while (in.hasNext()) {
                in.next();
            }
        }
    }

    @DataProvider
    public Object[][] getReaderFailData() {
        String fastq = String.join("\n",
                "@q1",
                "AACC",
                "+",
                "########");
        return new Object[][] {
                // fail if the seq and qual lines are different lengths
                {fastq},
                // fail if either header line is empty
                {fastq.replace("@q1", "")},
                {fastq.replace("+q1", "")}
        };
    }

    @Test(expectedExceptions = Exception.class, dataProvider = "getReaderFailData")
    public void testReaderFail(String fastq) {
        try (FastqReader in = new FastqReader(null, new BufferedReader(new StringReader(fastq)))) {
            while (in.hasNext()) {
                in.next();
            }
        }
    }
}