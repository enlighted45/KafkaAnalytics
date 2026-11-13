package com.bank.txn.analytics.feed;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/** Thread-safe bounded list of the most recent items (newest first). */
public class RecentBuffer<T> {
    private final int capacity;
    private final Deque<T> items = new ArrayDeque<>();

    public RecentBuffer(int capacity) {
        this.capacity = capacity;
    }

    public synchronized void add(T item) {
        items.addFirst(item);
        while (items.size() > capacity) {
            items.removeLast();
        }
    }

    public synchronized List<T> snapshot() {
        return new ArrayList<>(items);
    }
}
