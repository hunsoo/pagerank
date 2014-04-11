package me.hunsoo.wikipedia.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Scanner;

public class LinkGraphMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private HashMap<String, Integer> lookupTable;

    @Override
    protected void setup(Context context) throws IOException {
        lookupTable = new HashMap<String, Integer>();

        URI[] caches = context.getCacheArchives();
        for (URI cache : caches) {
            setupLookupTable(cache.getPath());
        }
    }

    /**
     * read reverse lookup table from distributed cache
     *
     * @param path reverse lookup table distributed cache file path
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
                lookupTable.put(title, docId);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();
        String[] fields = record.split("\t");
        Integer docId = Integer.parseInt(fields[0].split("\\|")[0]);
        //float pageRank = Float.valueOf(fields[1]);

        StringBuilder linkBuilder = new StringBuilder();
        boolean isFirst = true;
        for (int i = 2; i < fields.length; i++) {
            String link = fields[i];
            Integer linkDocId = lookupTable.get(link);

            if (linkDocId != null) {
                if (!isFirst) {
                    linkBuilder.append("\t");
                }
                // we found a matching document id
                linkBuilder.append(linkDocId);
                isFirst = false;
            }
        }

        context.write(new IntWritable(docId), new Text("1.0\t" + linkBuilder.toString()));
    }
}