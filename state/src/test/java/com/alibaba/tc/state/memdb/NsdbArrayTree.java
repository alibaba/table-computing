package com.alibaba.tc.state.memdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

import static java.lang.Math.max;

public class NsdbArrayTree<T>
{
    private static class Node<T>
    {
        ArrayList<Comparable<T>> elements = new ArrayList<>();
        ArrayList<Node> children = new ArrayList<>();
    }

    private final Node root = new Node();
    private int height = 0;

    public <T extends Comparable> void add(T element)
    {
        if (root.elements.isEmpty()) {
            root.elements.add(element);
            root.children.add(null);
            height = 1;
            return;
        }

        int height = 1;
        Node<T> node = root;
        while (node != null) {
            if (element.compareTo(node.elements.get(node.elements.size() - 1)) >= 0) {
                node.elements.add(element);
                node.children.add(null);
                return;
            }
            int index = Collections.binarySearch(node.elements, element);
            if (index >= 0) {
                return;
            }
            index = -index - 1;
            Node<T> tmp = node.children.get(index);
            if (null == tmp) {
                tmp = new Node<>();
                tmp.elements.add(element);
                tmp.children.add(null);
                node.children.set(index, tmp);
                this.height = max(height + 1, this.height);
                return;
            }
            else {
                height++;
                node = tmp;
            }
        }
    }

    public boolean contains(T element)
    {
        Node<T> node = root;
        while (node != null) {
            int index = Collections.binarySearch(node.elements, element);
            if (index >= 0) {
                return true;
            }
            index = -index - 1;
            node = node.children.get(index);
        }

        return false;
    }

    public int getHeight()
    {
        return height;
    }

    /**
     * 1亿个int
     *     array list add:  32秒
     *     skip list add:   434秒
     *     array tree add:  OOM
     *
     * 1千万个nextInt无bound随机int
     *     array tree add:      24.41秒
     *     array tree contains: 18.39秒
     *     skip list add:       31.08秒
     *     skip list contains:  21.40秒
     *     tree set add:        12.27秒
     *     tree set contains:   11.07秒
     *     hash set add:        5.74秒
     *     hash set contains:   1.09秒
     */
    public static void main(String[] args)
    {
        int N = 1_000_000_0;
        int[] rndNums = new int[N];
        for (int i = 0; i < N; i++) {
            rndNums[i] = new Random().nextInt(1_000_000_000);
            //bound 10, 100, 1000, 10000, 100000, 1_000_000, 10_000_000, 100_000_000, 1000_000_000对应的array tree的高度分别为：3, 8, 15, 20, 25, 30, 37, 38, 39
            //bound >= 1_000_000_0之后array tree性能略快于skip list，tree set在所有bound下性能都是最快的（应该跟纯随机，树的旋转发生的概率降低有关，另外skip list里的锁可能也会降低部分性能）
        }


        long start = System.nanoTime();
        NsdbArrayTree<Integer> arrayTree = new NsdbArrayTree<>();
        for (int i = 0; i < N; i++) {
            arrayTree.add(rndNums[i]);
        }
        long end = System.nanoTime();
        System.out.println("tree height: " + arrayTree.height);
        System.out.println("elapsed: " + ((end - start)/1_000_000_000.) + "s");

        start = System.nanoTime();
        for (int i = 0; i < N; i++) {
            arrayTree.contains(rndNums[i]);
        }
        end = System.nanoTime();
        System.out.println("elapsed: " + ((end - start)/1_000_000_000.) + "s");


        start = System.nanoTime();
        ConcurrentSkipListSet<Integer> skipListSet = new ConcurrentSkipListSet<>();
        for (int i = 0; i < N; i++) {
            skipListSet.add(rndNums[i]);
        }
        end = System.nanoTime();
        System.out.println("elapsed: " + ((end - start)/1_000_000_000.) + "s");

        start = System.nanoTime();
        for (int i = 0; i < N; i++) {
            skipListSet.contains(rndNums[i]);
        }
        end = System.nanoTime();
        System.out.println("elapsed: " + ((end - start)/1_000_000_000.) + "s");


        start = System.nanoTime();
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < N; i++) {
            treeSet.add(rndNums[i]);
        }
        end = System.nanoTime();
        System.out.println("elapsed: " + ((end - start)/1_000_000_000.) + "s");

        start = System.nanoTime();
        for (int i = 0; i < N; i++) {
            treeSet.contains(rndNums[i]);
        }
        end = System.nanoTime();
        System.out.println("elapsed: " + ((end - start)/1_000_000_000.) + "s");


        start = System.nanoTime();
        HashSet<Integer> hashSet = new HashSet<>();
        for (int i = 0; i < N; i++) {
            hashSet.add(rndNums[i]);
        }
        end = System.nanoTime();
        System.out.println("elapsed: " + ((end - start)/1_000_000_000.) + "s");

        start = System.nanoTime();
        for (int i = 0; i < N; i++) {
            hashSet.contains(rndNums[i]);
        }
        end = System.nanoTime();
        System.out.println("elapsed: " + ((end - start)/1_000_000_000.) + "s");
    }
}
