import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Milestone1 {
    
    // LinkedHashMap preserves the insertion order (Genesis -> Revelation)
    static Map<String, Long> bookTotalWordCounts = new LinkedHashMap<>();
    static Map<String, Map<String, Integer>> bookWordFrequencies = new LinkedHashMap<>();
    static Map<String, Integer> globalWordFrequencies = new HashMap<>();

    public static void main(String[] args) {
        try {
            List<String> lines = Files.readAllLines(Paths.get("bible.txt"));
            
            String currentBook = "Header/Introduction";
            StringBuilder potentialTitle = new StringBuilder();
            boolean isReadingTitle = false;
            int emptyLineCount = 0;

            // --- MAP PHASE (PARSING) ---
            for (String line : lines) {
                String trimmedLine = line.trim();

                if (trimmedLine.isEmpty()) {
                    emptyLineCount++;
                    continue;
                }

                // Title Detection
                if (emptyLineCount >= 2 && !trimmedLine.matches("^\\d+:\\d+.*")) { 
                    isReadingTitle = true;
                    potentialTitle.setLength(0); 
                    potentialTitle.append(trimmedLine);
                    emptyLineCount = 0; 
                    continue; 
                }

                // Start of New Book (1:1)
                if (trimmedLine.matches("^1:1(?![0-9]).*")) {
                    if (isReadingTitle && potentialTitle.length() > 0) {
                        // Apply cleaning logic here
                        currentBook = cleanTitle(potentialTitle.toString().trim());
                    }
                    isReadingTitle = false; 
                    processLine(currentBook, trimmedLine);
                    emptyLineCount = 0;
                    continue;
                }

                // Regular Verse
                if (trimmedLine.matches("^\\d+:\\d+.*")) {
                    isReadingTitle = false; 
                    processLine(currentBook, trimmedLine);
                    emptyLineCount = 0;
                    continue;
                }

                // Text Line
                if (isReadingTitle) {
                    potentialTitle.append(" ").append(trimmedLine);
                } else {
                    processLine(currentBook, trimmedLine);
                }
                
                emptyLineCount = 0;
            }

            // --- REDUCE PHASE (OUTPUT) ---

            // MILESTONE 1A: Number of words per book
            System.out.println("================================================================================");
            System.out.println("MILESTONE 1a: Number of words per book");
            System.out.println("================================================================================");
            System.out.printf("%-25s | %s%n", "Book Name", "Word Count");
            System.out.println("--------------------------+------------------");
            
            for (String book : bookTotalWordCounts.keySet()) {
                if (bookTotalWordCounts.get(book) > 100) { 
                    System.out.printf("%-25s | %d%n", book, bookTotalWordCounts.get(book));
                }
            }
            System.out.println("\n");


// MILESTONE 1B: Top 5 words per book
            System.out.println("======================================================================================");
            System.out.println("MILESTONE 1b: Top 5 most common words per book");
            System.out.println("======================================================================================");
            
            for (String book : bookTotalWordCounts.keySet()) {
                if (bookTotalWordCounts.get(book) < 100) continue; 

                // 1. Print the Book Name first (ONCE)
                System.out.print(String.format("%-25s : ", book));

                Map<String, Integer> words = bookWordFrequencies.get(book);
                if (words != null) {
                    words.entrySet().stream()
                        .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue())) 
                        .limit(5)
                        // 2. Print only the word and count inside the loop
                        .forEach(e -> System.out.print(e.getKey() + "(" + e.getValue() + ")  "));
                }
                // 3. Move to the next line after the words are done
                System.out.println(); 
            }
            System.out.println("\n");


            // MILESTONE 1C: Top 5 most common words in the whole Bible
            System.out.println("================================================================================");
            System.out.println("MILESTONE 1c: Top 5 most common words in the whole Bible");
            System.out.println("================================================================================");
            
            globalWordFrequencies.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                .limit(5)
                .forEach(e -> System.out.println(e.getKey() + ": " + e.getValue()));

        } catch (IOException e) {
            System.out.println("Error: bible.txt not found.");
        }
    }

    private static void processLine(String book, String text) {
        text = text.replaceFirst("^\\d+:\\d+", "").trim();
        if (text.isEmpty()) return;
        
        String[] tokens = text.split("\\s+");
        long validWordCount = 0;

        for (String token : tokens) {
            if (token.matches(".*[a-zA-Z].*")) {
                validWordCount++;
                String cleanWord = token.replaceAll("[^a-zA-Z]", "").toLowerCase();

                if (!cleanWord.isEmpty()) {
                    globalWordFrequencies.put(cleanWord, globalWordFrequencies.getOrDefault(cleanWord, 0) + 1);
                    bookWordFrequencies.putIfAbsent(book, new HashMap<>());
                    bookWordFrequencies.get(book).put(cleanWord, bookWordFrequencies.get(book).getOrDefault(cleanWord, 0) + 1);
                }
            }
        }
        bookTotalWordCounts.put(book, bookTotalWordCounts.getOrDefault(book, 0L) + validWordCount);
    }

    // --- ROBUST CLEAN TITLE ---
    private static String cleanTitle(String rawTitle) {
        String lower = rawTitle.toLowerCase();
        // Check Revelation FIRST
        if (lower.contains("revelation")) return "Revelation";

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

        if (lower.contains("matthew")) return "Matthew";
        if (lower.contains("mark")) return "Mark";
        if (lower.contains("luke")) return "Luke";

        // John
        if (lower.contains("1 john") || (lower.contains("first") && lower.contains("john"))) return "1 John";
        if (lower.contains("2 john") || (lower.contains("second") && lower.contains("john"))) return "2 John";
        if (lower.contains("3 john") || (lower.contains("third") && lower.contains("john"))) return "3 John";
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

        return rawTitle.trim();
    }
}