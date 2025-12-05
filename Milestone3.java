import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Milestone3 {

    // Maps to store character counts
    // LinkedHashMap is CRITICAL here to preserve the Bible order (Genesis -> Revelation)
    static Map<String, Map<Character, Integer>> bookCharCounts = new LinkedHashMap<>();
    static Map<Character, Integer> globalCharCounts = new HashMap<>();

    static Map<String, Map<Character, Integer>> bookStartCounts = new LinkedHashMap<>();
    static Map<Character, Integer> globalStartCounts = new HashMap<>();

    public static void main(String[] args) {
        try {
            List<String> lines = Files.readAllLines(Paths.get("bible.txt"));
            
            String currentBook = "Header";
            StringBuilder potentialTitle = new StringBuilder();
            boolean isReadingTitle = false;
            int emptyLineCount = 0;

            // --- MAP PHASE (PARSING) ---
            for (String line : lines) {
                String trimmedLine = line.trim();
                if (trimmedLine.isEmpty()) { emptyLineCount++; continue; }

                // Title Detection
                if (emptyLineCount >= 2 && !trimmedLine.matches("^\\d+:\\d+.*")) { 
                    isReadingTitle = true; potentialTitle.setLength(0); potentialTitle.append(trimmedLine);
                    emptyLineCount = 0; continue; 
                }

                // First Verse (1:1) -> Finalize Title
                if (trimmedLine.matches("^1:1(?![0-9]).*")) {
                    if (isReadingTitle && potentialTitle.length() > 0) {
                        currentBook = cleanTitle(potentialTitle.toString().trim());
                    }
                    isReadingTitle = false; processLine(currentBook, trimmedLine); emptyLineCount = 0; continue;
                }

                // Regular Verse
                if (trimmedLine.matches("^\\d+:\\d+.*")) {
                    isReadingTitle = false; processLine(currentBook, trimmedLine); emptyLineCount = 0; continue;
                }

                // Accumulate Title or Process Text
                if (isReadingTitle) potentialTitle.append(" ").append(trimmedLine);
                else processLine(currentBook, trimmedLine);
                
                emptyLineCount = 0;
            }

            // --- REDUCE PHASE (OUTPUT IN SPECIFIC ORDER) ---
            
            // 3a. Most Frequent Alphabet Character in Each Book
            System.out.println("==================================================================");
            System.out.println("3a. Most Frequent Alphabet Character in Each Book");
            System.out.println("==================================================================");
            for (String book : bookCharCounts.keySet()) {
                if(bookCharCounts.get(book).size() < 5) continue; // Skip Headers
                System.out.print(String.format("%-20s : ", book));
                printWinner(bookCharCounts.get(book));
            }

            // 3b. Most Frequent Alphabet Character in the Bible
            System.out.println("\n==================================================================");
            System.out.println("3b. Most Frequent Alphabet Character in the Bible");
            System.out.println("==================================================================");
            System.out.print("Whole Bible          : ");
            printWinner(globalCharCounts);


            // 3c. Most Common Starting Letter of Words in Each Book
            System.out.println("\n==================================================================");
            System.out.println("3c. Most Common Starting Letter of Words in Each Book");
            System.out.println("==================================================================");
            for (String book : bookStartCounts.keySet()) {
                if(bookStartCounts.get(book).size() < 5) continue;
                System.out.print(String.format("%-20s : ", book));
                printWinner(bookStartCounts.get(book));
            }

            // 3d. Most Common Starting Letter of Words in the Bible
            System.out.println("\n==================================================================");
            System.out.println("3d. Most Common Starting Letter of Words in the Bible");
            System.out.println("==================================================================");
            System.out.print("Whole Bible          : ");
            printWinner(globalStartCounts);

        } catch (IOException e) { e.printStackTrace(); }
    }

    // --- PROCESSING LOGIC ---
    private static void processLine(String book, String text) {
        text = text.replaceFirst("^\\d+:\\d+", "").trim().toLowerCase();
        if (text.isEmpty()) return;

        // A. Character Frequency
        for (char c : text.toCharArray()) {
            if (Character.isLetter(c)) {
                globalCharCounts.put(c, globalCharCounts.getOrDefault(c, 0) + 1);
                bookCharCounts.putIfAbsent(book, new HashMap<>());
                bookCharCounts.get(book).put(c, bookCharCounts.get(book).getOrDefault(c, 0) + 1);
            }
        }

        // B. Starting Letter Frequency
        String[] tokens = text.split("\\s+");
        for (String token : tokens) {
            String clean = token.replaceAll("[^a-z]", "");
            if (!clean.isEmpty()) {
                char start = clean.charAt(0);
                globalStartCounts.put(start, globalStartCounts.getOrDefault(start, 0) + 1);
                bookStartCounts.putIfAbsent(book, new HashMap<>());
                bookStartCounts.get(book).put(start, bookStartCounts.get(book).getOrDefault(start, 0) + 1);
            }
        }
    }

    // --- PRINT HELPER ---
    private static void printWinner(Map<Character, Integer> map) {
        map.entrySet().stream()
           .max(Map.Entry.comparingByValue())
           .ifPresent(e -> System.out.println(e.getKey() + " (Count: " + e.getValue() + ")"));
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