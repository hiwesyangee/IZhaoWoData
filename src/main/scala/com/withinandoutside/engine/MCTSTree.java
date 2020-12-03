package com.withinandoutside.engine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * WanWan MCTS Tree，蒙特卡罗搜索树结构，针对弯弯项目的数据结构变型。
 * 针对小市场非均衡状态下的策划师推荐。
 * by hiwes since 2020/11/18
 */
public class MCTSTree {

    private class Node {
        public Node head_link;
        public Info info;
        public Score score;
        public List<String> nodeRecomResult; // 通过
        public Node tail_link;

        // 新建根节点————构造函数。多种
        public Node() {
        }

        public Node(Node head_link, Info info, Score score, List<String> nodeRecomResult, Node tail_link) {
            this.head_link = head_link;
            this.info = info;
            this.score = score;
            this.nodeRecomResult = nodeRecomResult;
            this.tail_link = tail_link;
        }

        public Node(Info info, Score score) {
            this.info = info;
            this.score = score;
        }

        public Node(Info info, Score score, List<String> nodeRecomResult) {
            this.info = info;
            this.score = score;
            this.nodeRecomResult = nodeRecomResult;
        }

        public Node(Info info, Score score, List<String> nodeRecomResult, Node tail_link) {
            this.info = info;
            this.score = score;
            this.nodeRecomResult = nodeRecomResult;
            this.tail_link = tail_link;
        }

        public Node(Node head_link, Info info, Score score, List<String> nodeRecomResult) {
            this.head_link = head_link;
            this.info = info;
            this.score = score;
            this.nodeRecomResult = nodeRecomResult;
        }
    }

    private Node root; // 成员变量，根节点。【设置根节点为空，属性为空，但tail_link指向新节点】
    private int size; // 成员变量，元素个数

    // 构造函数，【method1:建立根节点方法】
    public MCTSTree() {
        root = new Node();
        size = 0;
    }

    // 构造函数，带根节点的MCTS树构建
    public MCTSTree(Node root, int size) {
        this.root = root;
        this.size = size;
    }

    // 查询MCTS树的节点个数
    public int getSize() {
        return size;
    }

    // 返回MCTS树是否为空
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * 新增节点
     */
    public void add(Info info, Score score, List<String> recList) {
        if (isEmpty()) root = new Node(info, score, recList);
        else {
            Node prev = root;
            while (null != prev.tail_link) {
                prev = prev.tail_link;
            }
            prev.tail_link = new Node(prev, info, score, recList);
        }
        size++;
    }

    /**
     * 获取MCTS树整体推荐结果。
     *
     * @return
     */
    public Map<Integer, List<String>> getRecomResultMap() {
        if (isEmpty()) return new HashMap<>();
        else { // 不是空，size就是层数
            Map<Integer, List<String>> map = new HashMap<>();
            Node prev = root;
            int dept = 0;
            while (null != prev.tail_link) {
                map.put(dept, prev.nodeRecomResult);
                prev = prev.tail_link;
                dept += 1;
            }
            return map;
        }
    }

    // todo 释放MCTS树对象
    public void releaseMCTSTree() {
        try {
            if (getSize() != 0) {
                root = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//
//    /**
//     * 测试用。
//     *
//     * @param args
//     */
//    public static void main(String[] args) {
//        MCTSTree mt = new MCTSTree();
//        System.out.println(mt.size);
//        Node ro = mt.root;
//        System.out.println(ro.head_link + "==" + ro.b + "==" + ro.score + "==" + ro.value + "==" + ro.tail_link);
//
//        double[] a = {-1d, 0d, 1d};
//        double[] b = {-0.5d, 0d, 0.5d};
//        int[] a_qual = {1, 2, 3};
//        int[] b_equal = {1, 2, 3};
//
//        B bbb = new B("hotel_id_test", 3, "sm_id_test", 3);
//        Score score = new Score(a, b, a_qual, b_equal);
//
////        mt.add(bbb, score, 30f);
////        ro = mt.root;
////        System.out.println("增加第一个节点1】 " + ro.head_link + "==" + ro.b + "==" + ro.score + "==" + ro.value + "==" + ro.tail_link);
////        System.out.println(mt.getSize());
//////
////        mt.add(bbb, score, 40f);
////        System.out.println("==============");
////        System.out.println("增加第二个节点，看第一个节点】 " + ro.head_link + "==" + ro.b + "==" + ro.score + "==" + ro.value + "==" + ro.tail_link);
////        ro = ro.tail_link;
////        System.out.println("增加第二个节点，看第二个节点】 " + ro.head_link + "==" + ro.b + "==" + ro.score + "==" + ro.value + "==" + ro.tail_link);
////        System.out.println(mt.getSize());
////
////        mt.add(bbb, score, 50f);
////        System.out.println("==============");
////        System.out.println("增加第三个节点，看第二个节点】 " + ro.head_link + "==" + ro.b + "==" + ro.score + "==" + ro.value + "==" + ro.tail_link);
////        ro = ro.tail_link;
////        System.out.println("增加第三个节点，看第三个节点】 " + ro.head_link + "==" + ro.b + "==" + ro.score + "==" + ro.value + "==" + ro.tail_link);
////        System.out.println(mt.getSize());
//
//        for (int i = 1; i <= 100; i++) {
//            float x = (float) (5f * i);
//            mt.add(bbb, score, x);
//        }
//
//        for (int i = 1; i <= 1000; i++) {
//            float x = (float) (0.5f * i);
//            mt.add(bbb, score, x);
//        }
//
//        System.out.println(mt.getSize());
//
//        long a1 = System.nanoTime();
//        Node v = mt.findUnBalanceBestNode();
//        System.out.println("最优节点】 " + v.head_link + "==" + v.b + "==" + v.score + "==" + v.value + "==" + v.tail_link);
//        long a2 = System.nanoTime();
//        System.out.println("time = " + (a2 - a1) + " ns.");
//    }
}
