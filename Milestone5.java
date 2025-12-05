import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Milestone5 {

    static class Verse {
        String book;
        String ref; 
        String text;
        int wordCount;

        Verse(String b, String r, String t, int c) {
            book = b; ref = r; text = t; wordCount = c;
        }
    }

    static List<Verse> allVerses = new ArrayList<>();
    static Map<String, List<Verse>> bookVerses = new LinkedHashMap<>();

    public static void main(String[] args) {
        try {
            List<String> lines = Files.readAllLines(Paths.get("bible.txt"));
            
            String currentBook = "Header";
            StringBuilder potentialTitle = new StringBuilder();
            boolean isReadingTitle = false;
            int emptyLineCount = 0;

            for (String line : lines) {
                String trimmedLine = line.trim();
                if (trimmedLine.isEmpty()) { emptyLineCount++; continue; }

                if (emptyLineCount >= 2 && !trimmedLine.matches("^\\d+:\\d+.*")) { 
                    isReadingTitle = true; potentialTitle.setLength(0); potentialTitle.append(trimmedLine);
                    emptyLineCount = 0; continue; 
                }

                if (trimmedLine.matches("^1:1(?![0-9]).*")) {
                    if (isReadingTitle && potentialTitle.length() > 0) {
                        currentBook = cleanTitle(potentialTitle.toString().trim());
                    }
                    isReadingTitle = false; processLine(currentBook, trimmedLine); emptyLineCount = 0; continue;
                }

                if (trimmedLine.matches("^\\d+:\\d+.*")) {
                    isReadingTitle = false; processLine(currentBook, trimmedLine); emptyLineCount = 0; continue;
                }

                if (isReadingTitle) potentialTitle.append(" ").append(trimmedLine);
                else if (!isReadingTitle) processLine(currentBook, trimmedLine);
                
                emptyLineCount = 0;
            }

            // --- REDUCE PHASE ---
            System.out.println("=================================================");
            System.out.println("MILESTONE 5");
            System.out.println("=================================================");

            System.out.println("5A");
            for (String book : bookVerses.keySet()) {
                if(bookVerses.get(book).isEmpty()) continue;
                System.out.print(book + " --> ");
                findMiddle(bookVerses.get(book));
            }

            System.out.println("\n5B");
            System.out.println("Whole Bible:");
            findMiddle(allVerses);
            System.out.println("-------------------------------------------------");

        } catch (IOException e) { e.printStackTrace(); }
    }

    private static void processLine(String book, String line) {
        // Simple logic: if line starts with Number, it's new verse. Else append to last.
        if (line.matches("^\\d+:\\d+.*")) {
            String[] parts = line.split(" ", 2);
            String verseRef = parts[0];
            String text = (parts.length > 1) ? parts[1] : "";
            int count = countWords(text);
            
            Verse v = new Verse(book, book + " " + verseRef, text, count);
            allVerses.add(v);
            bookVerses.putIfAbsent(book, new ArrayList<>());
            bookVerses.get(book).add(v);
        } else {
            // Append continuation text to previous verse
            if (!allVerses.isEmpty()) {
                Verse last = allVerses.get(allVerses.size() - 1);
                if (last.book.equals(book)) {
                    last.wordCount += countWords(line);
                    last.text += " " + line;
                }
            }
        }
    }

    private static int countWords(String text) {
        if (text == null || text.isEmpty()) return 0;
        String[] tokens = text.split("\\s+");
        int c = 0;
        for(String t : tokens) if(t.matches(".*[a-zA-Z].*")) c++;
        return c;
    }

    private static void findMiddle(List<Verse> verses) {
        long totalWords = 0;
        for (Verse v : verses) totalWords += v.wordCount;
        
        long targetIndex = totalWords / 2;
        long currentCount = 0;

        for (Verse v : verses) {
            if (currentCount + v.wordCount >= targetIndex) {
                String snippet = v.text.length() > 50 ? v.text.substring(0, 50) + "..." : v.text;
                System.out.println(v.ref + " (" + snippet + ")");
                return;
            }
            currentCount += v.wordCount;
        }
    }

    private static String cleanTitle(String rawTitle) {
        String lower = rawTitle.toLowerCase();

        // --- CRITICAL FIX: Check Revelation FIRST ---
        // We do this first because the title "Revelation of St. John" contains "John"
        if (lower.contains("revelation")) return "Revelation";

        // --- OLD TESTAMENT ---
        if (lower.contains("genesis")) return "Genesis";
        if (lower.contains("exodus")) return "Exodus";
        if (lower.contains("leviticus")) return "Leviticus";
        if (lower.contains("numbers")) return "Numbers";
        if (lower.contains("deuteronomy")) return "Deuteronomy";
        if (lower.contains("joshua")) return "Joshua";
        if (lower.contains("judges")) return "Judges";
        if (lower.contains("ruth")) return "Ruth";

        // Samuel
        if (lower.contains("1 samuel") || (lower.contains("first") && lower.contains("samuel"))) return "1 Samuel";
        if (lower.contains("2 samuel") || (lower.contains("second") && lower.contains("samuel"))) return "2 Samuel";

        // Kings
        if (lower.contains("1 kings") || (lower.contains("first") && lower.contains("kings"))) return "1 Kings";
        if (lower.contains("2 kings") || (lower.contains("second") && lower.contains("kings"))) return "2 Kings";

        // Chronicles
        if (lower.contains("1 chronicles") || (lower.contains("first") && lower.contains("chronicles"))) return "1 Chronicles";
        if (lower.contains("2 chronicles") || (lower.contains("second") && lower.contains("chronicles"))) return "2 Chronicles";

        if (lower.contains("ezra")) return "Ezra";
        if (lower.contains("nehemiah")) return "Nehemiah";
        if (lower.contains("esther")) return "Esther";
        if (lower.contains("job")) return "Job";
        if (lower.contains("psalms")) return "Psalms";
        if (lower.contains("proverbs")) return "Proverbs";
        if (lower.contains("ecclesiastes")) return "Ecclesiastes";
        if (lower.contains("song of solomon")) return "Song of Solomon";
        if (lower.contains("isaiah")) return "Isaiah";
        
        // Jeremiah & Lamentations
        if (lower.contains("lamentations")) return "Lamentations"; 
        if (lower.contains("jeremiah")) return "Jeremiah";
        
        if (lower.contains("ezekiel")) return "Ezekiel";
        if (lower.contains("daniel")) return "Daniel";
        if (lower.contains("hosea")) return "Hosea";
        if (lower.contains("joel")) return "Joel";
        if (lower.contains("amos")) return "Amos";
        if (lower.contains("obadiah")) return "Obadiah";
        if (lower.contains("jonah")) return "Jonah";
        if (lower.contains("micah")) return "Micah";
        if (lower.contains("nahum")) return "Nahum";
        if (lower.contains("habakkuk")) return "Habakkuk";
        if (lower.contains("zephaniah")) return "Zephaniah";
        if (lower.contains("haggai")) return "Haggai";
        if (lower.contains("zechariah")) return "Zechariah";
        if (lower.contains("malachi")) return "Malachi";

        // --- NEW TESTAMENT ---
        if (lower.contains("matthew")) return "Matthew";
        if (lower.contains("mark")) return "Mark";
        if (lower.contains("luke")) return "Luke";

        // John Epistles & Gospel
        if (lower.contains("1 john") || (lower.contains("first") && lower.contains("john"))) return "1 John";
        if (lower.contains("2 john") || (lower.contains("second") && lower.contains("john"))) return "2 John";
        if (lower.contains("3 john") || (lower.contains("third") && lower.contains("john"))) return "3 John";
        
        // This check must happen AFTER checking for 1/2/3 John, and AFTER checking for Revelation
        if (lower.contains("john")) return "John"; 

        if (lower.contains("acts")) return "Acts";
        if (lower.contains("romans")) return "Romans";

        // Corinthians
        if (lower.contains("1 corinthians") || (lower.contains("first") && lower.contains("corinthians"))) return "1 Corinthians";
        if (lower.contains("2 corinthians") || (lower.contains("second") && lower.contains("corinthians"))) return "2 Corinthians";

        if (lower.contains("galatians")) return "Galatians";
        if (lower.contains("ephesians")) return "Ephesians";
        if (lower.contains("philippians")) return "Philippians";
        if (lower.contains("colossians")) return "Colossians";

        // Thessalonians
        if (lower.contains("1 thessalonians") || (lower.contains("first") && lower.contains("thessalonians"))) return "1 Thessalonians";
        if (lower.contains("2 thessalonians") || (lower.contains("second") && lower.contains("thessalonians"))) return "2 Thessalonians";

        // Timothy
        if (lower.contains("1 timothy") || (lower.contains("first") && lower.contains("timothy"))) return "1 Timothy";
        if (lower.contains("2 timothy") || (lower.contains("second") && lower.contains("timothy"))) return "2 Timothy";

        if (lower.contains("titus")) return "Titus";
        if (lower.contains("philemon")) return "Philemon";
        if (lower.contains("hebrews")) return "Hebrews";
        if (lower.contains("james")) return "James";

        // Peter
        if (lower.contains("1 peter") || (lower.contains("first") && lower.contains("peter"))) return "1 Peter";
        if (lower.contains("2 peter") || (lower.contains("second") && lower.contains("peter"))) return "2 Peter";

        if (lower.contains("jude")) return "Jude";

        // Fallback
        return rawTitle.trim();
    }
}