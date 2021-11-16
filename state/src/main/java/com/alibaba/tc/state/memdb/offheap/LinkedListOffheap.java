package com.alibaba.tc.state.memdb.offheap;

import com.alibaba.sdb.spi.block.InternalUnsafe;

public class LinkedListOffheap<E extends Serializer<E> & ComparableOffheap> {
    private long head = 0L;
    private int size = 0;
    //一个next指针
    private final int PREFIX_BYTES = Long.BYTES;
    private final E prototype;

    public LinkedListOffheap(E prototype)
    {
        this.prototype = prototype;
    }

    private void deleteElement(long addr)
    {
        size--;
        prototype.free(addr, PREFIX_BYTES);
    }

    public int size()
    {
        return size;
    }

    public boolean isEmpty()
    {
        return 0 == size && 0L == head;
    }

    public boolean add(E e)
    {
        long addr = e.allocAndSerialize(PREFIX_BYTES);
        InternalUnsafe.putLong(addr, head);
        head = addr;
        size++;
        return true;
    }

    private long getNext(long addr)
    {
        return InternalUnsafe.getLong(addr);
    }

    private void putNext(long addr, long next)
    {
        InternalUnsafe.putLong(addr, next);
    }

    public boolean remove(E e)
    {
        long p = head;
        long pre = p;
        while (0L != p) {
            long next = getNext(p);
            if (0 == e.compareTo(p + PREFIX_BYTES)) {
                if (head == pre) {
                    head = next;
                }
                else {
                    putNext(pre, next);
                }
                deleteElement(p);
                return true;
            }
            pre = p;
            p = next;
        }
        return false;
    }
}
