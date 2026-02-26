// Non-concurrent skiplist (Java) benchmark
//
// Measures:
// - Insert throughput
// - Lookup throughput

public class SkipListBenchmark {
    private static final int NUM_ELEMENTS = 100_000;

    private static long makeKey(int i) {
        return ((long) i * 17) + 255;
    }

    public static void main(String[] args) {
        System.out.println("=== Non-concurrent SkipList (Java) Benchmark ===");
        System.out.println();
        System.out.println("Elements: " + NUM_ELEMENTS);
        System.out.println();

        // =========================================================================
        // Benchmark 1: Sequential Insert
        // =========================================================================
        System.out.println("Running sequential insert benchmark...");
        long start = System.nanoTime();

        SkipList list = new SkipList();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            long key = makeKey(i);
            list.insert(key, ~key);
        }
        long duration = System.nanoTime() - start;
        double opsPerSec = (double) NUM_ELEMENTS / (duration / 1_000_000_000.0);
        System.out.printf("  sequential_insert: %d ops in %.3f ms | %.0f ops/s%n", 
            NUM_ELEMENTS, duration / 1_000_000.0, opsPerSec);

        // =========================================================================
        // Benchmark 2: Sequential Lookup (with value verification)
        // =========================================================================
        System.out.println("Running sequential lookup benchmark...");
        start = System.nanoTime();
        int found = 0;
        int verified = 0;
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            long key = makeKey(i);
            long val = list.search(key);
            if (val != -1) {
                found++;
                // Verify value is correct (should be ~key)
                if (val == (~key)) {
                    verified++;
                }
            }
        }
        duration = System.nanoTime() - start;
        opsPerSec = (double) NUM_ELEMENTS / (duration / 1_000_000_000.0);
        System.out.printf(" sequential_lookup: %d ops in %.3f ms | %.0f ops/s (%d found, %d verified)%n", 
            NUM_ELEMENTS, duration / 1_000_000.0, opsPerSec, found, verified);

        // =========================================================================
        // Benchmark 3: Random Lookup (with value verification)
        // =========================================================================
        System.out.println("Running random lookup benchmark...");
        java.util.Random rng = new java.util.Random(42);
        long[] keys = new long[NUM_ELEMENTS];
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            keys[i] = makeKey(rng.nextInt(NUM_ELEMENTS));
        }
        
        start = System.nanoTime();
        found = 0;
        verified = 0;
        for (long key : keys) {
            long val = list.search(key);
            if (val != -1) {
                found++;
                // Verify value is correct (should be ~key)
                if (val == (~key)) {
                    verified++;
                }
            }
        }
        duration = System.nanoTime() - start;
        opsPerSec = (double) NUM_ELEMENTS / (duration / 1_000_000_000.0);
        System.out.printf(" random_lookup: %d ops in %.3f ms | %.0f ops/s (%d found, %d verified)%n", 
            NUM_ELEMENTS, duration / 1_000_000.0, opsPerSec, found, verified);

        // =========================================================================
        // Benchmark 4: Insert + Remove
        // =========================================================================
        System.out.println("Running insert+remove benchmark...");
        list = new SkipList();
        start = System.nanoTime();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            long key = makeKey(i);
            list.insert(key, ~key);
        }
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            long key = makeKey(i);
            list.remove(key);
        }
        duration = System.nanoTime() - start;
        opsPerSec = (double) (2 * NUM_ELEMENTS) / (duration / 1_000_000_000.0);
        System.out.printf(" insert_remove: %d ops in %.3f ms | %.0f ops/s%n", 
            2 * NUM_ELEMENTS, duration / 1_000_000.0, opsPerSec);

        System.out.println();
        System.out.println("=== JSON Output ===");
        System.out.println("{");
        System.out.println("  \"engine\": \"skiplist_java_nonconcurrent\",");
        System.out.println("  \"elements\": " + NUM_ELEMENTS + ",");
        System.out.println("  \"results\": {");
        
        // Re-run to get JSON values
        list = new SkipList();
        start = System.nanoTime();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            long key = makeKey(i);
            list.insert(key, ~key);
        }
        duration = System.nanoTime() - start;
        double insertOps = (double) NUM_ELEMENTS / (duration / 1_000_000_000.0);
        
        start = System.nanoTime();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            long key = makeKey(i);
            list.search(key);
        }
        duration = System.nanoTime() - start;
        double lookupOps = (double) NUM_ELEMENTS / (duration / 1_000_000_000.0);
        
        System.out.printf("    \"sequential_insert\": %.0f,%n", insertOps);
        System.out.printf("    \"sequential_lookup\": %.0f,%n", lookupOps);
        System.out.println("    \"iteration\": 100000");
        System.out.println("  }");
        System.out.println("}");
    }
}
