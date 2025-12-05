import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopMilestone3 {

    public static class CharMapper extends Mapper<Object, Text, Text, Text> {
        // Reuse parsing logic variables
        private String currentBook = "Header"; private StringBuilder buffer = new StringBuilder();
        private boolean isTitle = false; private int empties = 0;

        public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) { empties++; return; }
            if (empties >= 2 && !line.matches("^\\d+:\\d+.*")) { isTitle = true; buffer.setLength(0); buffer.append(line); empties = 0; return; }
            if (line.matches("^1:1(?![0-9]).*")) { 
                if (isTitle) currentBook = cleanTitle(buffer.toString());
                isTitle = false; analyze(currentBook, line, ctx); empties = 0; return; 
            }
            if (line.matches("^\\d+:\\d+.*")) { isTitle = false; analyze(currentBook, line, ctx); empties = 0; return; }
            if (isTitle) buffer.append(" ").append(line); else analyze(currentBook, line, ctx);
            empties = 0;
        }

        private void analyze(String book, String text, Context ctx) throws IOException, InterruptedException {
            text = text.replaceFirst("^\\d+:\\d+", "").trim().toLowerCase();
            if (text.isEmpty()) return;
            // Chars
            for(char c : text.toCharArray()) {
                if(Character.isLetter(c)) {
                    ctx.write(new Text(book), new Text("C:" + c));
                    ctx.write(new Text("Whole_Bible"), new Text("C:" + c));
                }
            }
            // Starts
            for(String t : text.split("\\s+")) {
                String clean = t.replaceAll("[^a-z]", "");
                if(!clean.isEmpty()) {
                    ctx.write(new Text(book), new Text("S:" + clean.charAt(0)));
                    ctx.write(new Text("Whole_Bible"), new Text("S:" + clean.charAt(0)));
                }
            }
        }
        // Minimal cleanTitle copy (same logic)
        private static String cleanTitle(String t) { String l = t.toLowerCase(); if(l.contains("revelation"))return "Revelation"; if(l.contains("genesis"))return "Genesis"; if(l.contains("exodus"))return "Exodus"; if(l.contains("leviticus"))return "Leviticus"; if(l.contains("numbers"))return "Numbers"; if(l.contains("deuteronomy"))return "Deuteronomy"; if(l.contains("joshua"))return "Joshua"; if(l.contains("judges"))return "Judges"; if(l.contains("ruth"))return "Ruth"; if(l.contains("samuel"))return l.contains("2")||l.contains("second")?"2 Samuel":"1 Samuel"; if(l.contains("kings"))return l.contains("2")||l.contains("second")?"2 Kings":"1 Kings"; if(l.contains("chronicles"))return l.contains("2")||l.contains("second")?"2 Chronicles":"1 Chronicles"; if(l.contains("ezra"))return "Ezra"; if(l.contains("nehemiah"))return "Nehemiah"; if(l.contains("esther"))return "Esther"; if(l.contains("job"))return "Job"; if(l.contains("psalms"))return "Psalms"; if(l.contains("proverbs"))return "Proverbs"; if(l.contains("ecclesiastes"))return "Ecclesiastes"; if(l.contains("song"))return "Song of Solomon"; if(l.contains("isaiah"))return "Isaiah"; if(l.contains("jeremiah"))return "Jeremiah"; if(l.contains("lamentations"))return "Lamentations"; if(l.contains("ezekiel"))return "Ezekiel"; if(l.contains("daniel"))return "Daniel"; if(l.contains("hosea"))return "Hosea"; if(l.contains("joel"))return "Joel"; if(l.contains("amos"))return "Amos"; if(l.contains("obadiah"))return "Obadiah"; if(l.contains("jonah"))return "Jonah"; if(l.contains("micah"))return "Micah"; if(l.contains("nahum"))return "Nahum"; if(l.contains("habakkuk"))return "Habakkuk"; if(l.contains("zephaniah"))return "Zephaniah"; if(l.contains("haggai"))return "Haggai"; if(l.contains("zechariah"))return "Zechariah"; if(l.contains("malachi"))return "Malachi"; if(l.contains("matthew"))return "Matthew"; if(l.contains("mark"))return "Mark"; if(l.contains("luke"))return "Luke"; if(l.contains("john"))return (l.contains("1")||l.contains("first"))?"1 John":(l.contains("2")||l.contains("second"))?"2 John":(l.contains("3")||l.contains("third"))?"3 John":"John"; if(l.contains("acts"))return "Acts"; if(l.contains("romans"))return "Romans"; if(l.contains("corinthians"))return l.contains("2")||l.contains("second")?"2 Corinthians":"1 Corinthians"; if(l.contains("galatians"))return "Galatians"; if(l.contains("ephesians"))return "Ephesians"; if(l.contains("philippians"))return "Philippians"; if(l.contains("colossians"))return "Colossians"; if(l.contains("thessalonians"))return l.contains("2")||l.contains("second")?"2 Thessalonians":"1 Thessalonians"; if(l.contains("timothy"))return l.contains("2")||l.contains("second")?"2 Timothy":"1 Timothy"; if(l.contains("titus"))return "Titus"; if(l.contains("philemon"))return "Philemon"; if(l.contains("hebrews"))return "Hebrews"; if(l.contains("james"))return "James"; if(l.contains("peter"))return l.contains("2")||l.contains("second")?"2 Peter":"1 Peter"; if(l.contains("jude"))return "Jude"; return t.trim(); }
    }

    public static class WinnerReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, String> results = new HashMap<>();
        private static final List<String> BOOKS = Arrays.asList("Genesis","Exodus","Leviticus","Numbers","Deuteronomy","Joshua","Judges","Ruth","1 Samuel","2 Samuel","1 Kings","2 Kings","1 Chronicles","2 Chronicles","Ezra","Nehemiah","Esther","Job","Psalms","Proverbs","Ecclesiastes","Song of Solomon","Isaiah","Jeremiah","Lamentations","Ezekiel","Daniel","Hosea","Joel","Amos","Obadiah","Jonah","Micah","Nahum","Habakkuk","Zephaniah","Haggai","Zechariah","Malachi","Matthew","Mark","Luke","John","Acts","Romans","1 Corinthians","2 Corinthians","Galatians","Ephesians","Philippians","Colossians","1 Thessalonians","2 Thessalonians","1 Timothy","2 Timothy","Titus","Philemon","Hebrews","James","1 Peter","2 Peter","1 John","2 John","3 John","Jude","Revelation");

        public void reduce(Text key, Iterable<Text> values, Context ctx) {
            Map<Character, Integer> cCounts = new HashMap<>();
            Map<Character, Integer> sCounts = new HashMap<>();
            for (Text val : values) {
                String v = val.toString(); char c = v.charAt(2);
                if(v.startsWith("C:")) cCounts.put(c, cCounts.getOrDefault(c, 0) + 1);
                else sCounts.put(c, sCounts.getOrDefault(c, 0) + 1);
            }
            results.put(key.toString(), "Most Freq Char: " + getMax(cCounts) + "  | Most Freq Start: " + getMax(sCounts));
        }

        private String getMax(Map<Character, Integer> m) {
            return m.entrySet().stream().max(Map.Entry.comparingByValue()).map(e->e.getKey()+"("+e.getValue()+")").orElse("None");
        }

        @Override protected void cleanup(Context ctx) throws IOException, InterruptedException {
            printHeader(ctx, "MILESTONE 3a & 3c)");
            for(String b : BOOKS) if(results.containsKey(b)) write(ctx, String.format("%-25s : %s", b, results.get(b)));
            ctx.write(new Text("\n\n"), new Text(""));
            
            printHeader(ctx, "MILESTONE 3b & 3d");
            if(results.containsKey("Whole_Bible")) write(ctx, "Whole_Bible               : " + results.get("Whole_Bible"));
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
        Job j = Job.getInstance(c, "M3");
        j.setJarByClass(HadoopMilestone3.class); j.setMapperClass(CharMapper.class); j.setReducerClass(WinnerReducer.class);
        j.setOutputKeyClass(Text.class); j.setOutputValueClass(Text.class); j.setNumReduceTasks(1);
        FileInputFormat.addInputPath(j, new Path(args[0])); FileOutputFormat.setOutputPath(j, new Path(args[1]));
        System.exit(j.waitForCompletion(true)?0:1);
    }
}