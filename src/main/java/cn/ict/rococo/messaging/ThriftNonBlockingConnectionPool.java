package cn.ict.rococo.messaging;

import java.io.IOException;

import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.apache.thrift.transport.TNonblockingSocket;

import cn.ict.rococo.Member;

public class ThriftNonBlockingConnectionPool extends BaseKeyedPoolableObjectFactory<Member,TNonblockingSocket> {

    @Override
    public TNonblockingSocket makeObject(Member member) throws Exception {
        return new CachedTNonBlockingSocket(member.getHostName(), member.getPort());
    }

    @Override
    public boolean validateObject(Member key, TNonblockingSocket obj) {
        long lastReturnTime = ((CachedTNonBlockingSocket) obj).getLastReturnTime();
        return lastReturnTime < 0 || System.currentTimeMillis() - lastReturnTime < 50000;
    }

    @Override
    public void destroyObject(Member key, TNonblockingSocket obj) throws Exception {
        obj.close();
    }

    @Override
    public void passivateObject(Member key, TNonblockingSocket obj) throws Exception {
        ((CachedTNonBlockingSocket) obj).setLastReturnTime(System.currentTimeMillis());
    }

    public static class CachedTNonBlockingSocket extends TNonblockingSocket {

        private long lastReturnTime = -1L;

        public CachedTNonBlockingSocket(String host, int port) throws IOException {
            super(host, port);
        }

        public long getLastReturnTime() {
            return lastReturnTime;
        }

        public void setLastReturnTime(long lastReturnTime) {
            this.lastReturnTime = lastReturnTime;
        }
    }
}
