package simpledb.storage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class LRUCache<K, V> {
    // doubly linked list: maintain access order
    // hashmap: O(1) to access a node when get() or put()
    private final int capacity;
    private final HashMap<K, LRUNode<K,V>> map; 
    private LRUNode<K, V> head, tail;

    // Define a generic node
    private class LRUNode<K, V> {
        K key;
        V value;
        LRUNode<K, V> prev, next;

        LRUNode(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    // Contructor of LRU cache (doubly linked list)
    public LRUCache(int capacity) {
        this.capacity = capacity;
        this.map = new HashMap<>();
        this.head = new LRUNode<>(null, null);
        this.tail = new LRUNode<>(null, null);
        head.next = tail;
        tail.prev = head;
    }

    // Get the value associated with the key
    // If the key is not present, return null
    public synchronized V get(K key) {
        LRUNode<K,V> node = map.get(key);
        if (node == null) return null;
        moveToEnd(node);
        return node.value;
    }

    // Put a key-value pair into the cache
    // If the key already exists, update the value and move it to the end
    // If the key does not exist and the cache is full, evict the 
    // least recently used item
    public synchronized void put(K key, V value) {
        if (map.containsKey(key)) {
            LRUNode<K, V> node = map.get(key);
            node.value = value;
            moveToEnd(node);
        } else {
            if (map.size() >= capacity) {
                evict();
            }
            LRUNode<K, V> node = new LRUNode<>(key, value);
            map.put(key, node);
            insertToEnd(node);
        }
    }

    // Evict the least recently used item from the cache
    public synchronized void evict() {
        LRUNode<K, V> lru = head.next; // remove from head
        if (lru != tail) {
            removeFromList(lru);
            map.remove(lru.key);
        }
    }

    // Remove the key from the cache and return its value
    public synchronized V remove(K key) {
        LRUNode<K, V> node = map.remove(key);
        if (node == null) return null;
        removeFromList(node);
        return node.value;
    }

    // Check if the cache contains the key
    public synchronized boolean containsKey(K key) {
        return map.containsKey(key);
    }

    // Return the current size of the cache
    public synchronized int size() {
        return map.size();
    }

    public synchronized K getLRUKey() {
        if (head.next == tail) return null; // cache is empty
        return head.next.key; 
    }

    public synchronized V getLRUValue() {
        if (head.next == tail) return null; // cache is empty
        return head.next.value; 
    }

    private void moveToEnd(LRUNode<K,V> node) {
        removeFromList(node);
        insertToEnd(node);
    }

    private void removeFromList(LRUNode<K,V> node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private void insertToEnd(LRUNode<K,V> node) {
        LRUNode<K, V> oldPrev = tail.prev;
        oldPrev.next = node;
        node.prev = oldPrev;
        node.next = tail;
        tail.prev = node;
    }

    public Iterator<K> keyIterator() {
        return new LRUIterator();
    }

    protected class LRUIterator implements Iterator<K> {
        LRUNode<K, V> head;

        public synchronized boolean hasNext() {
            return head.next != null;
        }

        public synchronized K next() {
            if (head == null || head.next == null) {
                throw new NoSuchElementException("No more elements in the cache");
            }
            head = head.next;
            return head.key;
        }
    }
    
}
