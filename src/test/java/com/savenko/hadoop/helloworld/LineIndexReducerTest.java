package com.savenko.hadoop.helloworld;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;

public class LineIndexReducerTest {

    private ReduceDriver<Text, Text, Text, Text> driver;

    @Before
    public void setUp() {
        driver = new ReduceDriver<Text, Text, Text, Text>(new LineIndexReducer());
    }

    @Test
    public void testOneOffset() {
        List<Pair<Text, Text>> out;

        try {
            out = driver.withInputKey(new Text("word")).withInputValue(new Text("offset")).run();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

        List<Pair<Text, Text>> expected = new ArrayList<Pair<Text, Text>>();
        expected.add(new Pair<Text, Text>(new Text("word"), new Text("offset")));

        assertListEquals(expected, out);
    }

    @Test
    public void testMultiOffset() {

        List<Pair<Text, Text>> out;

        try {
            out = driver.withInputKey(new Text("word")).withInputValues(Arrays.asList(new Text("offset1"), new Text("offset2"))).run();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

        List<Pair<Text, Text>> expected = new ArrayList<Pair<Text, Text>>();
        expected.add(new Pair<Text, Text>(new Text("word"), new Text("offset1,offset2")));

        assertListEquals(expected, out);
    }

}
