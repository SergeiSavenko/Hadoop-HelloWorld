package com.savenko.hadoop.helloworld;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mock.MockInputSplit;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;

/**
 * Test cases for the inverted index mapper.
 */
public class LineIndexMapperTest {

    private MapDriver<LongWritable, Text, Text, Text> driver;

    /**
     * We expect pathname@offset for the key from each of these
     */
    private final Text EXPECTED_OFFSET = new Text(MockInputSplit.getMockPath().toString() + "@0");

    @Before
    public void setUp() {
        driver = new MapDriver<LongWritable, Text, Text, Text>(new LineIndexMapper());
    }

    @Test
    public void testEmpty() {
        List<Pair<Text, Text>> out;

        try {
            out = driver.withInput(new LongWritable(0), new Text("")).run();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

        List<Pair<Text, Text>> expected = new ArrayList<Pair<Text, Text>>();

        assertListEquals(expected, out);
    }

    @Test
    public void testOneWord() {
        List<Pair<Text, Text>> out;

        try {
            out = driver.withInput(new LongWritable(0), new Text("foo")).run();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

        List<Pair<Text, Text>> expected = new ArrayList<Pair<Text, Text>>();
        expected.add(new Pair<Text, Text>(new Text("foo"), EXPECTED_OFFSET));

        assertListEquals(expected, out);
    }

    @Test
    public void testMultiWords() {
        List out;

        try {
            out = driver.withInput(new LongWritable(0), new Text("foo bar baz!!!! ????")).run();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

        List<Pair<Text, Text>> expected = new ArrayList<Pair<Text, Text>>();
        expected.add(new Pair<Text, Text>(new Text("foo"), EXPECTED_OFFSET));
        expected.add(new Pair<Text, Text>(new Text("bar"), EXPECTED_OFFSET));
        expected.add(new Pair<Text, Text>(new Text("baz"), EXPECTED_OFFSET));

        assertListEquals(expected, out);
    }
}