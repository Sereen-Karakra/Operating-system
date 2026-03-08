import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * SalesAnalyzer - Operating Systems Project.
 * * This program analyzes sales data using three approaches:
 * 1. Naive (Serial)
 * 2. Multithreading (Java Threads)
 * 3. Multiprocessing (JVM Child Processes)
 * * It calculates execution time and speedup for performance comparison.
 */
public class SalesAnalyzer {

    // Path to the data folder containing the 20 CSV files
    private static final String DATA_FOLDER_PATH = "data";

    // List of file names to process
    private static final String[] FILE_NAMES = {
            "xaa.csv", "xab.csv", "xac.csv", "xad.csv", "xae.csv", "xaf.csv", "xag.csv", "xah.csv", "xai.csv", "xaj.csv",
            "xak.csv", "xal.csv", "xam.csv", "xan.csv", "xao.csv", "xap.csv", "xaq.csv", "xar.csv", "xas.csv", "xat.csv"
    };

    // Inner class to hold aggregated results (Count, Revenue, Profit)
    static class Result {
        long count = 0;
        double totalRevenue = 0;
        double totalProfit = 0;

        // Merges results from another Result object into this one
        public void add(Result other) {
            this.count += other.count;
            this.totalRevenue += other.totalRevenue;
            this.totalProfit += other.totalProfit;
        }

        @Override
        public String toString() {
            return String.format("Orders: %d | Revenue: %,.2f | Profit: %,.2f", count, totalRevenue, totalProfit);
        }
    }

    // =============================================================
    // MAIN ENTRY POINT
    // =============================================================
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

        // --- WORKER MODE CHECK ---
        // If arguments are present and first flag is "--worker", run as a child process.
        if (args.length > 0 && args[0].equals("--worker")) {
            runAsWorker(args);
            return;
        }

        // --- PARENT PROCESS (INTERACTIVE MODE) ---
        Scanner scanner = new Scanner(System.in);
        System.out.println("=== Sales Data Analyzer (OS Project) ===");

        // 1. Get User Selection
        System.out.println("Choose a filter criterion:");
        System.out.println("a. Region");
        System.out.println("b. Country");
        System.out.println("c. Item Type");
        System.out.println("d. Sales Channel");
        System.out.print("Enter choice (a-d): ");
        String choice = scanner.nextLine().trim();

        int columnIndex = -1;
        String criterionName = "";

        // Map choice to CSV column index
        switch (choice.toLowerCase()) {
            case "a": columnIndex = 0; criterionName = "Region"; break;
            case "b": columnIndex = 1; criterionName = "Country"; break;
            case "c": columnIndex = 2; criterionName = "Item Type"; break;
            case "d": columnIndex = 3; criterionName = "Sales Channel"; break;
            default:
                System.out.println("Invalid choice. Exiting.");
                return;
        }

        // 2. Get Target Value
        System.out.print("Enter specific " + criterionName + " (e.g., 'Asia', 'Online'): ");
        String targetValue = scanner.nextLine().trim();
        System.out.println("\nTargeting: " + criterionName + " = " + targetValue);

        // Array for the number of threads/processes to test
        int[] countsToTest = {2, 4, 8, 16, 20};

        // =============================================================
        // 1. NAIVE APPROACH (SERIAL)
        // =============================================================
        System.out.println("\n--- 1. Naive Approach (Serial) ---");

        long start = System.currentTimeMillis();
        Result naiveResult = solveNaive(columnIndex, targetValue);
        long end = System.currentTimeMillis();

        long naiveTime = end - start; // Store this to calculate speedup later

        System.out.println("Result: " + naiveResult);
        System.out.println("Time: " + naiveTime + " ms");

        // =============================================================
        // 2. MULTITHREADING APPROACH
        // =============================================================
        System.out.println("\n--- 2. Multithreading Approach ---");
        System.out.printf("%-10s %-15s %-10s%n", "Threads", "Time (ms)", "Speedup"); // Table Header
        System.out.println("----------------------------------------");

        for (int threads : countsToTest) {
            start = System.currentTimeMillis();
            Result threadResult = solveMultithreading(columnIndex, targetValue, threads);
            end = System.currentTimeMillis();

            long duration = end - start;
            // Speedup = Naive Time / Parallel Time
            double speedup = (double) naiveTime / duration;

            System.out.printf("%-10d %-15d %-10.2f%n", threads, duration, speedup);
        }

        // =============================================================
        // 3. MULTIPROCESSING APPROACH
        // =============================================================
        System.out.println("\n--- 3. Multiprocessing Approach ---");
        System.out.printf("%-10s %-15s %-10s%n", "Children", "Time (ms)", "Speedup"); // Table Header
        System.out.println("----------------------------------------");

        for (int children : countsToTest) {
            start = System.currentTimeMillis();
            Result procResult = solveMultiprocessing(columnIndex, targetValue, children);
            end = System.currentTimeMillis();

            long duration = end - start;
            double speedup = (double) naiveTime / duration;

            System.out.printf("%-10d %-15d %-10.2f%n", children, duration, speedup);
        }

        System.out.println("\nAnalysis Complete.");
    }

    // =============================================================
    // APPROACH 1: NAIVE (SERIAL)
    // Processes files one by one in the main thread.
    // =============================================================
    private static Result solveNaive(int colIndex, String target) {
        Result total = new Result();
        for (String fileName : FILE_NAMES) {
            Result fileRes = processFile(new File(DATA_FOLDER_PATH, fileName), colIndex, target);
            total.add(fileRes);
        }
        return total;
    }

    // =============================================================
    // APPROACH 2: MULTITHREADING
    // Uses ExecutorService to manage a pool of threads.
    // =============================================================
    private static Result solveMultithreading(int colIndex, String target, int numThreads) throws ExecutionException, InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<Result>> futures = new ArrayList<>();

        // Submit a task for each file
        for (String fileName : FILE_NAMES) {
            Callable<Result> task = () -> processFile(new File(DATA_FOLDER_PATH, fileName), colIndex, target);
            futures.add(executor.submit(task));
        }

        // Aggregate results
        Result total = new Result();
        for (Future<Result> f : futures) {
            total.add(f.get());
        }

        executor.shutdown();
        return total;
    }

    // =============================================================
    // APPROACH 3: MULTIPROCESSING
    // Uses ProcessBuilder to spawn separate JVM instances.
    // =============================================================
    private static Result solveMultiprocessing(int colIndex, String target, int numChildren) throws IOException, InterruptedException {
        // Distribute files among children (Round Robin)
        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < numChildren; i++) chunks.add(new ArrayList<>());
        for (int i = 0; i < FILE_NAMES.length; i++) {
            chunks.get(i % numChildren).add(FILE_NAMES[i]);
        }

        List<Process> processes = new ArrayList<>();
        Result totalResult = new Result();

        // Build and start processes
        for (int i = 0; i < numChildren; i++) {
            List<String> filesForChild = chunks.get(i);
            if (filesForChild.isEmpty()) continue;

            // Construct the command: java -cp ... SalesAnalyzer --worker ...
            String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
            String classpath = System.getProperty("java.class.path");
            String className = SalesAnalyzer.class.getName();

            List<String> command = new ArrayList<>();
            command.add(javaBin);
            command.add("-cp");
            command.add(classpath);
            command.add(className);
            command.add("--worker");
            command.add(String.valueOf(colIndex));
            command.add(target);
            command.addAll(filesForChild);

            ProcessBuilder builder = new ProcessBuilder(command);
            processes.add(builder.start());
        }

        // Read output from each child process
        for (Process p : processes) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String line = reader.readLine();
                if (line != null) {
                    String[] parts = line.split(",");
                    Result r = new Result();
                    r.count = Long.parseLong(parts[0]);
                    r.totalRevenue = Double.parseDouble(parts[1]);
                    r.totalProfit = Double.parseDouble(parts[2]);
                    totalResult.add(r);
                }
            }
            p.waitFor();
        }

        return totalResult;
    }

    // =============================================================
    // WORKER LOGIC (Child Process)
    // This code runs inside the separate JVM processes.
    // =============================================================
    private static void runAsWorker(String[] args) {
        try {
            // Parse arguments passed by the parent
            int colIndex = Integer.parseInt(args[1]);
            String target = args[2];
            Result workerTotal = new Result();

            // Process assigned files
            for (int i = 3; i < args.length; i++) {
                File f = new File(DATA_FOLDER_PATH, args[i]);
                workerTotal.add(processFile(f, colIndex, target));
            }

            // Print CSV-formatted result to Standard Output so parent can read it
            System.out.println(workerTotal.count + "," + workerTotal.totalRevenue + "," + workerTotal.totalProfit);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // =============================================================
    // CSV PARSING LOGIC
    // Reads a file line by line and filters based on criteria.
    // =============================================================
    private static Result processFile(File file, int colIndex, String target) {
        Result localRes = new Result();
        if (!file.exists()) return localRes;

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            boolean isHeader = true;
            while ((line = br.readLine()) != null) {
                if (isHeader) { isHeader = false; continue; } // Skip CSV header

                String[] parts = line.split(","); // Standard split (assuming clean data)

                if (parts.length > colIndex) {
                    // Case-insensitive comparison
                    if (parts[colIndex].equalsIgnoreCase(target)) {
                        localRes.count++;
                        try {
                            // Column 11 is Revenue, Column 13 is Profit
                            localRes.totalRevenue += Double.parseDouble(parts[11]);
                            localRes.totalProfit += Double.parseDouble(parts[13]);
                        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                            // Ignore malformed lines
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading file: " + file.getName());
        }
        return localRes;
    }
}