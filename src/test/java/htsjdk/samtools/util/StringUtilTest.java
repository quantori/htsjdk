package htsjdk.samtools.util;

import htsjdk.HtsjdkTest;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StringUtilTest extends HtsjdkTest {

    @Test
    public void testJoin() {
        Assert.assertEquals(StringUtil.join(",", 1, "hello", 'T'), "1,hello,T");
        Assert.assertEquals(StringUtil.join(","), "");
    }

    @Test
    public void testSplit() {
        Arrays.asList("A:BB:C", "A:BB", "A:BB:", "A:BB:C:DDD", "A:", "A", "A:BB:C").forEach(s -> {
            String[] arr = new String[10];
            int count = StringUtil.split(s, arr, ':');
            Assert.assertEquals(Arrays.copyOf(arr, count), s.split(":"));
        });
    }

    @Test
    public void testSplitConcatenateExcessTokens() {
        Arrays.asList("A:BB:C", "A:BB", "A:BB:", "A:BB:C:DDD", "A:", "A", "A:BB:C").forEach(s -> {
            String[] arr = new String[3];
            int count = StringUtil.splitConcatenateExcessTokens(s, arr, ':');
            Assert.assertEquals(Arrays.copyOf(arr, count),
                    Arrays.stream(s.split(":", 3)).filter(string -> !string.isEmpty()).toArray());
        });
    }

    @DataProvider
    public Object[][] getHammingDistanceData() {
        return new Object[][] {
                // should return zero for two empty sequences
                {"", "", 0},
                {"ATAC", "GCAT", 3},
                {"ATAGC", "ATAGC", 0},
                // should be case sensitive
                {"ATAC", "atac", 4},
                // should count Ns as matching when computing distance
                {"nAGTN", "nAGTN", 0},
        };
    }

    @Test(dataProvider = "getHammingDistanceData")
    public void testHammingDistance(String s1, String s2, int distance) {
        Assert.assertEquals(StringUtil.hammingDistance(s1, s2), distance);
    }

    @DataProvider
    public Object[][] getHammingDistanceFailData() {
        return new Object[][] {
                // should throw an exception if two strings of different lengths are provided
                {"", "ABC"},
                {"Abc", "wxyz"}
        };
    }

    @Test(dataProvider = "getHammingDistanceFailData", expectedExceptions = Exception.class)
    public void testHammingDistanceFail(String s1, String s2) {
        StringUtil.hammingDistance(s1, s2);
    }

    @DataProvider
    public Object[][] getIsWithinHammingDistanceData() {
        return new Object[][]{
                {"ATAC", "GCAT", 3},
                {"ATAC", "GCAT", 2},
                {"ATAC", "GCAT", 1},
                {"ATAC", "GCAT", 0}
        };
    }

    @Test(dataProvider = "getIsWithinHammingDistanceData")
    public void testIsWithinHammingDistance(String s1, String s2, int within) {
        Assert.assertEquals(StringUtil.isWithinHammingDistance(s1, s2, within),
                (StringUtil.hammingDistance(s1, s2) <= within));
    }

    @Test(dataProvider = "getHammingDistanceFailData", expectedExceptions = Exception.class)
    public void testIsWithinHammingDistanceFail(String s1, String s2) {
        StringUtil.isWithinHammingDistance(s1, s2, 2);
    }

    @Test
    public void testToLowerCase() {
        IntStream.range(0, 128).forEach(i ->
                Assert.assertEquals(StringUtil.toLowerCase((byte) i), Character.toLowerCase(i)));
    }

    @Test
    public void testToUpperCase() {
        IntStream.range(0, 128).forEach(i ->
                Assert.assertEquals(StringUtil.toUpperCase((byte) i), Character.toUpperCase(i)));
    }

    @Test
    public void testToUpperCaseArray() {
        String seq = "atACgtaCGTgatcCAtATATgATtatgacNryuAN";
        byte[] bytes = seq.getBytes();
        StringUtil.toUpperCase(bytes);
        Assert.assertEquals(bytes, seq.toUpperCase().getBytes());
    }

    @Test
    public void testAssertCharactersNotInString() {
        StringUtil.assertCharactersNotInString("HelloWorld", ' ', '!', '_');
    }

    @Test(expectedExceptions = Exception.class)
    public void testAssertCharactersNotInStringFail() {
        StringUtil.assertCharactersNotInString("Hello World!", ' ', '!', '_');
    }

    private static final String TEXT_FOR_WRAPPING =
            String.join("\n", "This is a little bit", "of text with nice short", "lines.");

    @Test
    public void testWordWrap() {
        // should not wrap when lines are shorter than the given length
        Assert.assertEquals(StringUtil.wordWrap(TEXT_FOR_WRAPPING, 50), TEXT_FOR_WRAPPING);

        // should wrap text when lines are longer than length give
        String[] result = StringUtil.wordWrap(TEXT_FOR_WRAPPING, 15).split("\n");
        Assert.assertEquals(result.length, 5);
        Arrays.stream(result).forEach(line -> Assert.assertTrue(line.length() <= 15));
    }

    @Test
    public void testIntValuesToStringInt() {
        int[] ints = {1, 2, 3, 11, 22, 33, Integer.MIN_VALUE, 0, Integer.MAX_VALUE};
        Assert.assertEquals(StringUtil.intValuesToString(ints),
                Arrays.stream(ints).mapToObj(String::valueOf).collect(Collectors.joining(", ")));
    }

    @Test
    public void testIntValuesToStringShort() {
        short[] ints = {1, 2, 3, 11, 22, 33, Short.MIN_VALUE, 0, Short.MAX_VALUE};
        List<String> expected = new ArrayList<>();
        for (short s : ints) {
            expected.add(String.valueOf(s));
        }
        Assert.assertEquals(StringUtil.intValuesToString(ints), String.join(", ", expected));
    }
}