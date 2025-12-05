import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopMilestone1 {

    public static class BibleMapper extends Mapper<Object, Text, Text, Text> {
        private String currentBook = "Header";
        private StringBuilder titleBuffer = new StringBuilder();
        private boolean readingTitle = false;
        private int emptyLines = 0;

        public void map(Object key, Text val, Context ctx) throws IOException, InterruptedException {
            String line = val.toString().trim();
            if (line.isEmpty()) { emptyLines++; return; }

            if (emptyLines >= 2 && !line.matches("^\\d+:\\d+.*")) { // Start Title
                readingTitle = true; titleBuffer.setLength(0); titleBuffer.append(line); emptyLines = 0; return;
            }
            if (line.matches("^1:1(?![0-9]).*")) { // New Book
                if (readingTitle) currentBook = cleanTitle(titleBuffer.toString());
                readingTitle = false; process(currentBook, line, ctx); emptyLines = 0; return;
            }
            if (line.matches("^\\d+:\\d+.*")) { // Normal Verse
                readingTitle = false; process(currentBook, line, ctx); emptyLines = 0; return;
            }
            if (readingTitle) titleBuffer.append(" ").append(line);
            else process(currentBook, line, ctx);
            emptyLines = 0;
        }

        private void process(String book, String text, Context ctx) throws IOException, InterruptedException {
            for (String t : text.replaceFirst("^\\d+:\\d+", "").trim().split("\\s+")) {
                if (t.matches(".*[a-zA-Z].*")) {
                    String w = t.replaceAll("[^a-zA-Z]", "").toLowerCase();
                    if (!w.isEmpty()) { ctx.write(new Text(book), new Text(w)); ctx.write(new Text("BIBLE_TOTAL"), new Text(w)); }
                }
            }
        }

        private static String cleanTitle(String t) {
            String l = t.toLowerCase();
            if (l.contains("revelation")) return "Revelation";
            if (l.contains("genesis")) return "Genesis"; if (l.contains("exodus")) return "Exodus";
            if (l.contains("leviticus")) return "Leviticus"; if (l.contains("numbers")) return "Numbers";
            if (l.contains("deuteronomy")) return "Deuteronomy"; if (l.contains("joshua")) return "Joshua";
            if (l.contains("judges")) return "Judges"; if (l.contains("ruth")) return "Ruth";
            if (l.contains("samuel")) return l.contains("2") || l.contains("second") ? "2 Samuel" : "1 Samuel";
            if (l.contains("kings")) return l.contains("2") || l.contains("second") ? "2 Kings" : "1 Kings";
            if (l.contains("chronicles")) return l.contains("2") || l.contains("second") ? "2 Chronicles" : "1 Chronicles";
            if (l.contains("ezra")) return "Ezra"; if (l.contains("nehemiah")) return "Nehemiah";
            if (l.contains("esther")) return "Esther"; if (l.contains("job")) return "Job";
            if (l.contains("psalms")) return "Psalms"; if (l.contains("proverbs")) return "Proverbs";
            if (l.contains("ecclesiastes")) return "Ecclesiastes"; if (l.contains("song")) return "Song of Solomon";
            if (l.contains("isaiah")) return "Isaiah"; if (l.contains("jeremiah")) return "Jeremiah";
            if (l.contains("lamentations")) return "Lamentations"; if (l.contains("ezekiel")) return "Ezekiel";
            if (l.contains("daniel")) return "Daniel"; if (l.contains("hosea")) return "Hosea";
            if (l.contains("joel")) return "Joel"; if (l.contains("amos")) return "Amos";
            if (l.contains("obadiah")) return "Obadiah"; if (l.contains("jonah")) return "Jonah";
            if (l.contains("micah")) return "Micah"; if (l.contains("nahum")) return "Nahum";
            if (l.contains("habakkuk")) return "Habakkuk"; if (l.contains("zephaniah")) return "Zephaniah";
            if (l.contains("haggai")) return "Haggai"; if (l.contains("zechariah")) return "Zechariah";
            if (l.contains("malachi")) return "Malachi"; if (l.contains("matthew")) return "Matthew";
            if (l.contains("mark")) return "Mark"; if (l.contains("luke")) return "Luke";
            if (l.contains("john")) return (l.contains("1")||l.contains("first"))?"1 John":(l.contains("2")||l.contains("second"))?"2 John":(l.contains("3")||l.contains("third"))?"3 John":"John";
            if (l.contains("acts")) return "Acts"; if (l.contains("romans")) return "Romans";
            if (l.contains("corinthians")) return l.contains("2") || l.contains("second") ? "2 Corinthians" : "1 Corinthians";
            if (l.contains("galatians")) return "Galatians"; if (l.contains("ephesians")) return "Ephesians";
            if (l.contains("philippians")) return "Philippians"; if (l.contains("colossians")) return "Colossians";
            if (l.contains("thessalonians")) return l.contains("2") || l.contains("second") ? "2 Thessalonians" : "1 Thessalonians";
            if (l.contains("timothy")) return l.contains("2") || l.contains("second") ? "2 Timothy" : "1 Timothy";
            if (l.contains("titus")) return "Titus"; if (l.contains("philemon")) return "Philemon";
            if (l.contains("hebrews")) return "Hebrews"; if (l.contains("james")) return "James";
            if (l.contains("peter")) return l.contains("2") || l.contains("second") ? "2 Peter" : "1 Peter";
            if (l.contains("jude")) return "Jude";
            return t.trim();
        }
    }

    public static class StatsReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, Long> wordCounts = new HashMap<>();
        private Map<String, String> top5Map = new HashMap<>();
        private static final List<String> BOOKS = Arrays.asList("Genesis","Exodus","Leviticus","Numbers","Deuteronomy","Joshua","Judges","Ruth","1 Samuel","2 Samuel","1 Kings","2 Kings","1 Chronicles","2 Chronicles","Ezra","Nehemiah","Esther","Job","Psalms","Proverbs","Ecclesiastes","Song of Solomon","Isaiah","Jeremiah","Lamentations","Ezekiel","Daniel","Hosea","Joel","Amos","Obadiah","Jonah","Micah","Nahum","Habakkuk","Zephaniah","Haggai","Zechariah","Malachi","Matthew","Mark","Luke","John","Acts","Romans","1 Corinthians","2 Corinthians","Galatians","Ephesians","Philippians","Colossians","1 Thessalonians","2 Thessalonians","1 Timothy","2 Timothy","Titus","Philemon","Hebrews","James","1 Peter","2 Peter","1 John","2 John","3 John","Jude","Revelation");

        public void reduce(Text key, Iterable<Text> values, Context ctx) {
            Map<String, Integer> freq = new HashMap<>();
            long total = 0;
            for (Text v : values) { freq.put(v.toString(), freq.getOrDefault(v.toString(), 0) + 1); total++; }
            
            StringBuilder sb = new StringBuilder();
            freq.entrySet().stream().sorted((a,b)->b.getValue().compareTo(a.getValue())).limit(5)
                .forEach(e -> {
                    if(key.toString().equals("BIBLE_TOTAL")) sb.append(e.getKey()).append(": ").append(e.getValue()).append("\n");
                    else sb.append(e.getKey()).append("(").append(e.getValue()).append(")  ");
                });

            if (key.toString().equals("BIBLE_TOTAL")) top5Map.put("BIBLE", sb.toString());
            else { wordCounts.put(key.toString(), total); top5Map.put(key.toString(), sb.toString()); }
        }

        @Override protected void cleanup(Context ctx) throws IOException, InterruptedException {
            printHeader(ctx, "MILESTONE 1a: Number of words per book (Sorted by Word Count)");
            ctx.write(new Text(String.format("%-25s | %s", "Book Name", "Word Count")), new Text(""));
            ctx.write(new Text("--------------------------+------------------"), new Text(""));
            
            wordCounts.entrySet().stream().sorted((a,b)->b.getValue().compareTo(a.getValue()))
                .filter(e->e.getValue()>50).forEach(e-> write(ctx, String.format("%-25s | %d", e.getKey(), e.getValue())));
            ctx.write(new Text("\n\n"), new Text(""));

            printHeader(ctx, "MILESTONE 1b: Top 5 most common words per book");
            for(String b : BOOKS) if(top5Map.containsKey(b)) write(ctx, String.format("%-25s : %s", b, top5Map.get(b)));
            ctx.write(new Text("\n\n"), new Text(""));

            printHeader(ctx, "MILESTONE 1c: Top 5 most common words in the whole Bible");
            write(ctx, top5Map.get("BIBLE"));
        }
        private void printHeader(Context ctx, String t) throws IOException, InterruptedException {
            ctx.write(new Text("================================================================================"), new Text(""));
            ctx.write(new Text(t), new Text(""));
            ctx.write(new Text("================================================================================"), new Text(""));
        }
        private void write(Context ctx, String s) { try{ctx.write(new Text(s), new Text(""));}catch(Exception e){} }
    }

    public static void main(String[] args) throws Exception {
        Configuration c = new Configuration();
        Job j = Job.getInstance(c, "M1");
        j.setJarByClass(HadoopMilestone1.class); j.setMapperClass(BibleMapper.class); j.setReducerClass(StatsReducer.class);
        j.setOutputKeyClass(Text.class); j.setOutputValueClass(Text.class); j.setNumReduceTasks(1);
        FileInputFormat.addInputPath(j, new Path(args[0])); FileOutputFormat.setOutputPath(j, new Path(args[1]));
        System.exit(j.waitForCompletion(true)?0:1);
    }
}