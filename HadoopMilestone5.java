import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopMilestone5 {

    public static class VerseMapper extends Mapper<Object, Text, Text, Text> {
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
            if (isTitle) buffer.append(" ").append(line); else if(!isTitle) analyze(currentBook, line, ctx);
            empties = 0;
        }

        private void analyze(String book, String line, Context ctx) throws IOException, InterruptedException {
            if (line.matches("^\\d+:\\d+.*")) {
                 String[] parts = line.split(" ", 2);
                 String ref = parts[0]; String text = (parts.length > 1) ? parts[1] : "";
                 int count = 0; for(String t : text.split("\\s+")) if(t.matches(".*[a-zA-Z].*")) count++;
                 String val = ref + "|||" + count + "|||" + text;
                 ctx.write(new Text(book), new Text(val));
                 ctx.write(new Text("Whole Bible"), new Text(val));
            }
        }
        private static String cleanTitle(String t) { String l = t.toLowerCase(); if(l.contains("revelation"))return "Revelation"; if(l.contains("genesis"))return "Genesis"; if(l.contains("exodus"))return "Exodus"; if(l.contains("leviticus"))return "Leviticus"; if(l.contains("numbers"))return "Numbers"; if(l.contains("deuteronomy"))return "Deuteronomy"; if(l.contains("joshua"))return "Joshua"; if(l.contains("judges"))return "Judges"; if(l.contains("ruth"))return "Ruth"; if(l.contains("samuel"))return l.contains("2")||l.contains("second")?"2 Samuel":"1 Samuel"; if(l.contains("kings"))return l.contains("2")||l.contains("second")?"2 Kings":"1 Kings"; if(l.contains("chronicles"))return l.contains("2")||l.contains("second")?"2 Chronicles":"1 Chronicles"; if(l.contains("ezra"))return "Ezra"; if(l.contains("nehemiah"))return "Nehemiah"; if(l.contains("esther"))return "Esther"; if(l.contains("job"))return "Job"; if(l.contains("psalms"))return "Psalms"; if(l.contains("proverbs"))return "Proverbs"; if(l.contains("ecclesiastes"))return "Ecclesiastes"; if(l.contains("song"))return "Song of Solomon"; if(l.contains("isaiah"))return "Isaiah"; if(l.contains("jeremiah"))return "Jeremiah"; if(l.contains("lamentations"))return "Lamentations"; if(l.contains("ezekiel"))return "Ezekiel"; if(l.contains("daniel"))return "Daniel"; if(l.contains("hosea"))return "Hosea"; if(l.contains("joel"))return "Joel"; if(l.contains("amos"))return "Amos"; if(l.contains("obadiah"))return "Obadiah"; if(l.contains("jonah"))return "Jonah"; if(l.contains("micah"))return "Micah"; if(l.contains("nahum"))return "Nahum"; if(l.contains("habakkuk"))return "Habakkuk"; if(l.contains("zephaniah"))return "Zephaniah"; if(l.contains("haggai"))return "Haggai"; if(l.contains("zechariah"))return "Zechariah"; if(l.contains("malachi"))return "Malachi"; if(l.contains("matthew"))return "Matthew"; if(l.contains("mark"))return "Mark"; if(l.contains("luke"))return "Luke"; if(l.contains("john"))return (l.contains("1")||l.contains("first"))?"1 John":(l.contains("2")||l.contains("second"))?"2 John":(l.contains("3")||l.contains("third"))?"3 John":"John"; if(l.contains("acts"))return "Acts"; if(l.contains("romans"))return "Romans"; if(l.contains("corinthians"))return l.contains("2")||l.contains("second")?"2 Corinthians":"1 Corinthians"; if(l.contains("galatians"))return "Galatians"; if(l.contains("ephesians"))return "Ephesians"; if(l.contains("philippians"))return "Philippians"; if(l.contains("colossians"))return "Colossians"; if(l.contains("thessalonians"))return l.contains("2")||l.contains("second")?"2 Thessalonians":"1 Thessalonians"; if(l.contains("timothy"))return l.contains("2")||l.contains("second")?"2 Timothy":"1 Timothy"; if(l.contains("titus"))return "Titus"; if(l.contains("philemon"))return "Philemon"; if(l.contains("hebrews"))return "Hebrews"; if(l.contains("james"))return "James"; if(l.contains("peter"))return l.contains("2")||l.contains("second")?"2 Peter":"1 Peter"; if(l.contains("jude"))return "Jude"; return t.trim(); }
    }

    public static class MiddleReducer extends Reducer<Text, Text, Text, Text> {
        class VObj { String r; int c; String t; VObj(String rr, int cc, String tt){r=rr;c=cc;t=tt;} }
        private Map<String, String> results = new HashMap<>();
        private static final List<String> BOOKS = Arrays.asList("Genesis","Exodus","Leviticus","Numbers","Deuteronomy","Joshua","Judges","Ruth","1 Samuel","2 Samuel","1 Kings","2 Kings","1 Chronicles","2 Chronicles","Ezra","Nehemiah","Esther","Job","Psalms","Proverbs","Ecclesiastes","Song of Solomon","Isaiah","Jeremiah","Lamentations","Ezekiel","Daniel","Hosea","Joel","Amos","Obadiah","Jonah","Micah","Nahum","Habakkuk","Zephaniah","Haggai","Zechariah","Malachi","Matthew","Mark","Luke","John","Acts","Romans","1 Corinthians","2 Corinthians","Galatians","Ephesians","Philippians","Colossians","1 Thessalonians","2 Thessalonians","1 Timothy","2 Timothy","Titus","Philemon","Hebrews","James","1 Peter","2 Peter","1 John","2 John","3 John","Jude","Revelation");

        public void reduce(Text key, Iterable<Text> values, Context ctx) {
            List<VObj> list = new ArrayList<>();
            long total = 0;
            for(Text val : values) {
                String[] p = val.toString().split("\\|\\|\\|");
                int c = Integer.parseInt(p[1]);
                list.add(new VObj(p[0], c, p[2]));
                total += c;
            }
            list.sort((a,b) -> {
                String[] pA = a.r.split(":"); String[] pB = b.r.split(":");
                int cA = Integer.parseInt(pA[0]); int cB = Integer.parseInt(pB[0]);
                if(cA!=cB) return cA-cB;
                return Integer.parseInt(pA[1])-Integer.parseInt(pB[1]);
            });
            long target = total/2; long current = 0;
            for(VObj v : list) {
                if(current + v.c >= target) {
                    String snippet = v.t.length() > 50 ? v.t.substring(0,50)+"..." : v.t;
                    results.put(key.toString(), "Middle (" + v.r + "): " + snippet);
                    return;
                }
                current += v.c;
            }
        }

        @Override protected void cleanup(Context ctx) throws IOException, InterruptedException {
            printHeader(ctx, "MILESTONE 5a");
            for(String b : BOOKS) if(results.containsKey(b)) write(ctx, String.format("%-25s : %s", b, results.get(b)));
            ctx.write(new Text("\n\n"), new Text(""));
            printHeader(ctx, "MILESTONE 5b");
            if(results.containsKey("Whole Bible")) write(ctx, "Whole Bible               : " + results.get("Whole Bible"));
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
        Job j = Job.getInstance(c, "M5");
        j.setJarByClass(HadoopMilestone5.class); j.setMapperClass(VerseMapper.class); j.setReducerClass(MiddleReducer.class);
        j.setOutputKeyClass(Text.class); j.setOutputValueClass(Text.class); j.setNumReduceTasks(1);
        FileInputFormat.addInputPath(j, new Path(args[0])); FileOutputFormat.setOutputPath(j, new Path(args[1]));
        System.exit(j.waitForCompletion(true)?0:1);
    }
}