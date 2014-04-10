package me.hunsoo.wikipedia.mapper;

import me.hunsoo.wikipedia.util.WikiPage;
import me.hunsoo.wikipedia.util.WikiTextParser;
import me.hunsoo.wikipedia.util.WikiXmlSAXParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.SAXException;

import java.io.IOException;

public class LinkExtractionMapper extends Mapper<LongWritable, Text, Text, Text> {

    public static enum linkCounter {PAGE_COUNT}

    @Override
    /**
     * Converts XML representation of a page into original document id|title and a list of its links.
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String page = value.toString();
        // assume each input page of interest has the entire contents of <page> tag, just not enclosed by one
        // and metadata tags outside <page> are discarded
        WikiPage wikiPage = parseXml(page);

        String wikiText = wikiPage.getWikiText();
        String title = wikiPage.getTitle();
        Long docId = wikiPage.getDocumentId();

        WikiTextParser wikiTextParser = new WikiTextParser(wikiText);
        String[] links = wikiTextParser.getLinks();

        boolean firstValue = true;
        StringBuilder strLinks = new StringBuilder();

        for (String link : links) {
            if (!firstValue && !link.equals("")) {
                strLinks.append("\t");
            }
            firstValue = false;
            strLinks.append(link.trim());
        }

        //context.write(new Text(title), new Text("1.0\t" + strLinks.toString()));
        context.write(new Text(docId + "|" + title), new Text("1.0\t" + strLinks.toString()));
        context.getCounter(linkCounter.PAGE_COUNT).increment(1);
    }

    /**
     * produce an object representation of a wiki page
     *
     * @param xml well-formed xml representation of a wiki page
     */
    private WikiPage parseXml(String xml) {
        WikiPage wikiPage = null;
        try {
            wikiPage = WikiXmlSAXParser.parse(xml);
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return wikiPage;
    }
}