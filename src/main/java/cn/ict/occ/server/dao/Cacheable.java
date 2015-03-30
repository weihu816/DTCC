package cn.ict.occ.server.dao;

public abstract class Cacheable {

    public abstract boolean isDirty();

    public abstract void setDirty(boolean dirty);

}
