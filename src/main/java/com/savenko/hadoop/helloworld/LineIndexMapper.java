package com.savenko.hadoop.helloworld;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * LineIndexMapper Maps each observed word in a line to a (filename@offset) string.
 */
public class LineIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    public LineIndexMapper() {
    }

    private static Set<String> googleStopwords;

    static {
        googleStopwords = new HashSet<String>();
        googleStopwords.add("I");
        googleStopwords.add("a");
        googleStopwords.add("about");
        googleStopwords.add("an");
        googleStopwords.add("are");
        googleStopwords.add("as");
        googleStopwords.add("at");
        googleStopwords.add("be");
        googleStopwords.add("by");
        googleStopwords.add("com");
        googleStopwords.add("de");
        googleStopwords.add("en");
        googleStopwords.add("for");
        googleStopwords.add("from");
        googleStopwords.add("how");
        googleStopwords.add("in");
        googleStopwords.add("is");
        googleStopwords.add("it");
        googleStopwords.add("la");
        googleStopwords.add("of");
        googleStopwords.add("on");
        googleStopwords.add("or");
        googleStopwords.add("that");
        googleStopwords.add("the");
        googleStopwords.add("this");
        googleStopwords.add("to");
        googleStopwords.add("was");
        googleStopwords.add("what");
        googleStopwords.add("when");
        googleStopwords.add("where");
        googleStopwords.add("who");
        googleStopwords.add("will");
        googleStopwords.add("with");
        googleStopwords.add("and");
        googleStopwords.add("the");
        googleStopwords.add("www");
    }

    /**
     * @param key      is the byte offset of the current line in the file;
     * @param value    is the line from the file
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
// Compile all the words using regex
        Pattern p = Pattern.compile("\\w+");
        Matcher m = p.matcher(value.toString());

// Get the name of the file from the inputsplit in the context
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

// build the values and write pairs through the context
        StringBuilder valueBuilder = new StringBuilder();
        while (m.find()) {
            String matchedKey = m.group().toLowerCase();
// remove names starting with non letters, digits, considered stopwords or containing other chars
            if (!Character.isLetter(matchedKey.charAt(0)) || Character.isDigit(matchedKey.charAt(0))
                    || googleStopwords.contains(matchedKey) || matchedKey.contains("_")) {
                continue;
            }
            valueBuilder.append(fileName);
            valueBuilder.append("@");
            valueBuilder.append(key.get());
// emit the partial
            context.write(new Text(matchedKey), new Text(valueBuilder.toString()));
            valueBuilder.setLength(0);
        }
    }
}