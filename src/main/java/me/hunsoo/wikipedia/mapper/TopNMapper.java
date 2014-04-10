package me.hunsoo.wikipedia.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

/**
 * implements top N query by finding local top N of each mapper
 * then a reducer consolidates the final top N
 */
public class TopNMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private static int N;
    private static TreeMap<Float, Text> topN;

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        N = Integer.parseInt(conf.get("N"));
        topN = new TreeMap<Float, Text>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // assume each input page of interest has one keyword, followed by a tab,
        // and multiple document ids separated by comma.
        String[] fields = line.split("\t");
        String docId = fields[0];
        Float pageRank = Float.valueOf(fields[1]);

        // add this record to local top N map
        topN.put(pageRank, new Text(docId + "\t" + pageRank));
        // if we have more than N records in top N now, remove the lowest
        if (topN.size() > N) {
            topN.remove(topN.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // we are guaranteed to have at most N records in top N map now
        // emit those records here
        for (Text record: topN.values()) {
            context.write(NullWritable.get(), record);
        }
    }
}
