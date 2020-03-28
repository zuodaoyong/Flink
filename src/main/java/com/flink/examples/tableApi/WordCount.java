package com.flink.examples.tableApi;

public class WordCount{
    public String word;
    public Long counts;

    public WordCount(){}
    public WordCount(String word, Long counts) {
        this.word = word;
        this.counts = counts;
    }

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", counts=" + counts +
                '}';
    }
}
