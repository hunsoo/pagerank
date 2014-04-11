package me.hunsoo.wikipedia.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Scanner;
import java.util.TreeMap;

public class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    private static int N;
    private static final TreeMap<Float, Text> topN = new TreeMap<Float, Text>();
    private static HashMap<Integer, String> lookupTable;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        N = Integer.parseInt(conf.get("N"));
    }

    /**
     * read lookup table from distributed cache
     *
     * @param path lookup table distributed cache file path
     */
    private void setupLookupTable(String path) {
        Scanner scanner;

        try {
            scanner = new Scanner(new File(path));
            while (scanner.hasNextLine()) {
                // assume the excluded words are delimited by a comma
                // ignore character case
                String line = scanner.nextLine();
                String[] fields = line.split("\t");
                Integer docId = Integer.parseInt(fields[0]);
                String title = fields[1];
                lookupTable.put(docId, title);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) {
        // value is <document id, PageRank>
        for (Text record : values) {
            String[] fields = record.toString().split("\t");
            Float pageRank = Float.parseFloat(fields[1]);

            // add this record to local top N map
            topN.put(pageRank, new Text(record));
            // if we have more than N records in top N now, remove the lowest
            if (topN.size() > N) {
                topN.remove(topN.firstKey());
            }
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        // there is a bug in hadoop 2.2 with context.getCacheFiles() method due to typo.
        // when called from reducer (if called from mapper, it works fine) it returns null.
        // this bug is fixed in hadoop 2.3, but we don't have 2.3 on EMR yet.
        // so we use getCacheArchives() instead.
        lookupTable = new HashMap<Integer, String>();

        URI[] caches = context.getCacheArchives();
        for (URI cache: caches) {
            setupLookupTable(cache.getPath());
        }

        // we are guaranteed to have at most N records in top N map now
        // emit those records in descending order here
        // transform document id to title using lookup table
        for (Text record : topN.descendingMap().values()) {
            String[] fields = record.toString().split("\t");
            Integer docId = Integer.parseInt(fields[0]);
            String pageRank = fields[1];
            String title = lookupTable.get(docId);

            context.write(NullWritable.get(), new Text(title + "\t" + pageRank));
        }
    }
}