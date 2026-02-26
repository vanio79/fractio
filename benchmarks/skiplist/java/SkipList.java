// Non-concurrent skiplist (Java) benchmark
//
// A simple non-concurrent skip list implementation for comparison

public class SkipList {
    // Node class
    static class Node {
        long key;
        long value;
        Node[] forward;
        
        Node(long key, long value, int level) {
            this.key = key;
            this.value = value;
            this.forward = new Node[level + 1];
        }
    }
    
    private static final int MAX_LEVEL = 16;
    private Node header;
    private int level;
    private int size;
    
    public SkipList() {
        this.header = new Node(Long.MIN_VALUE, 0, MAX_LEVEL);
        this.level = 0;
        this.size = 0;
    }
    
    // Generate a random level
    private int randomLevel() {
        int lvl = 0;
        while (Math.random() < 0.5 && lvl < MAX_LEVEL) {
            lvl++;
        }
        return lvl;
    }
    
    // Insert a key-value pair
    public void insert(long key, long value) {
        Node[] update = new Node[MAX_LEVEL + 1];
        Node current = header;
        
        // Find position to insert
        for (int i = level; i >= 0; i--) {
            while (current.forward[i] != null && current.forward[i].key < key) {
                current = current.forward[i];
            }
            update[i] = current;
        }
        
        current = current.forward[0];
        
        // Key already exists, update value
        if (current != null && current.key == key) {
            current.value = value;
            return;
        }
        
        // Create new node
        int newLevel = randomLevel();
        if (newLevel > level) {
            for (int i = level + 1; i <= newLevel; i++) {
                update[i] = header;
            }
            level = newLevel;
        }
        
        Node newNode = new Node(key, value, newLevel);
        for (int i = 0; i <= newLevel; i++) {
            newNode.forward[i] = update[i].forward[i];
            update[i].forward[i] = newNode;
        }
        size++;
    }
    
    // Search for a key, returns value or -1 if not found
    public long search(long key) {
        Node current = header;
        for (int i = level; i >= 0; i--) {
            while (current.forward[i] != null && current.forward[i].key < key) {
                current = current.forward[i];
            }
        }
        current = current.forward[0];
        if (current != null && current.key == key) {
            return current.value;
        }
        return -1;
    }
    
    // Remove a key
    public boolean remove(long key) {
        Node[] update = new Node[MAX_LEVEL + 1];
        Node current = header;
        
        for (int i = level; i >= 0; i--) {
            while (current.forward[i] != null && current.forward[i].key < key) {
                current = current.forward[i];
            }
            update[i] = current;
        }
        
        current = current.forward[0];
        if (current != null && current.key == key) {
            for (int i = 0; i <= level; i++) {
                if (update[i].forward[i] != current) {
                    break;
                }
                update[i].forward[i] = current.forward[i];
            }
            
            // Reduce level if needed
            while (level > 0 && header.forward[level] == null) {
                level--;
            }
            size--;
            return true;
        }
        return false;
    }
    
    public int size() {
        return size;
    }
}
