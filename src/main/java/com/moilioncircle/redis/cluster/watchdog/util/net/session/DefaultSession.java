package com.moilioncircle.redis.cluster.watchdog.util.net.session;

import com.moilioncircle.redis.cluster.watchdog.util.concurrent.future.CompletableFuture;
import com.moilioncircle.redis.cluster.watchdog.util.concurrent.future.ListenableFuture;
import com.moilioncircle.redis.cluster.watchdog.util.net.ConnectionStatus;
import com.moilioncircle.redis.cluster.watchdog.util.net.exceptions.TransportException;
import com.moilioncircle.redis.cluster.watchdog.util.net.transport.Transport;

import java.net.InetSocketAddress;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class DefaultSession<T> implements Session<T> {

    protected final Transport<T> transport;

    public DefaultSession(Transport<T> transport) {
        this.transport = transport;
    }

    @Override
    public String getLocalAddress(String value) {
        if (value != null) return value;
        InetSocketAddress socketAddress = (InetSocketAddress) transport.getLocalAddress();
        return socketAddress.getAddress().getHostAddress();
    }

    @Override
    public String getRemoteAddress(String value) {
        if (value != null) return value;
        InetSocketAddress socketAddress = (InetSocketAddress) transport.getRemoteAddress();
        return socketAddress.getAddress().getHostAddress();
    }

    @Override
    public long getId() {
        return transport.getId();
    }

    @Override
    public ConnectionStatus getStatus() {
        return this.transport.getStatus();
    }

    @Override
    public CompletableFuture<Void> send(T message) {
        if (transport.getStatus() == ConnectionStatus.CONNECTED) {
            return transport.write(message, true);
        } else {
            CompletableFuture<Void> r = new ListenableFuture<>();
            r.failure(new TransportException("[cluster] connection disconnected: " + toString()));
            return r;
        }
    }

    @Override
    public CompletableFuture<Void> disconnect(Throwable cause) {
        return transport.disconnect(cause);
    }

    @Override
    public String toString() {
        return transport.toString();
    }

}