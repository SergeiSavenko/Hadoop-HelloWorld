package com.savenko.hadoop.helloworld;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * LineIndexer Creates an inverted index over all the words in a document corpus, mapping each observed word to a list
 * of filename@offset locations where it occurs.
 */
public class LineIndexer extends Configured implements Tool {

    private static final String OUTPUT_PATH = "output";
    private static final String INPUT_PATH = "input";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LineIndexer(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = new Job(conf, "Line Indexer 1");

        job.setJarByClass(LineIndexer.class);
        job.setMapperClass(LineIndexMapper.class);
        job.setReducerClass(LineIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
