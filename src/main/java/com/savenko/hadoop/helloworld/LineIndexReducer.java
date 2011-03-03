package com.savenko.hadoop.helloworld;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * LineIndexReducer Takes a list of filename@offset entries for a single word and concatenates them into a list.
 */
public class LineIndexReducer extends Reducer<Text, Text, Text, Text> {

    public LineIndexReducer() {
    }

    /**
     * @param key     is the key of the mapper
     * @param values  are all the values aggregated during the mapping phase
     * @param context contains the context of the job run
     *                <p/>
     *                PRE-CONDITION: receive a list of <"word", "filename@offset"> pairs
     *                <"marcello", ["a.txt@3345", "b.txt@344", "c.txt@785"]>
     *                <p/>
     *                POST-CONDITION: emit the output a single key-value where all the file names
     *                are separated by a comma ",".
     *                <"marcello", "a.txt@3345,b.txt@344,c.txt@785">
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder valueBuilder = new StringBuilder();

        for (Text val : values) {
            valueBuilder.append(val);
            valueBuilder.append(",");
        }
//write the key and the adjusted value (removing the last comma)
        context.write(key, new Text(valueBuilder.substring(0, valueBuilder.length() - 1)));
        valueBuilder.setLength(0);
    }
}