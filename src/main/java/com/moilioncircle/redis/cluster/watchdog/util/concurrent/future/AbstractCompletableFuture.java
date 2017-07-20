package com.moilioncircle.redis.cluster.watchdog.util.concurrent.future;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by Baoyi Chen on 2017/7/7.
 */
public abstract class AbstractCompletableFuture<T> implements CompletableFuture<T> {

    protected final List<FutureListener<T>> listeners = new CopyOnWriteArrayList<>();

    @Override
    public boolean addListener(FutureListener<T> listener) {
        boolean rs = listeners.add(listener);
        if (this.isDone() && !listeners.isEmpty()) {
            for (FutureListener<T> r : listeners) r.onComplete(this);
        }
        return rs;
    }

    @Override
    public boolean removeListener(FutureListener<T> listener) {
        return listeners.remove(listener);
    }

    @Override
    public boolean addListeners(List<FutureListener<T>> listeners) {
        boolean rs = this.listeners.addAll(listeners);
        if (this.isDone() && !this.listeners.isEmpty()) {
            for (FutureListener<T> r : this.listeners) r.onComplete(this);
        }
        return rs;
    }

    @Override
    public boolean removeListeners(List<FutureListener<T>> listeners) {
        return this.listeners.removeAll(listeners);
    }
}