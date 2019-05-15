package com.flink.examples.batch;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import com.flink.examples.common.FileUtils;
/**
 * 获取集合中的前N个元素
 */
public class FirstNApplication {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment environment=ExecutionEnvironment.getExecutionEnvironment();
		List<Tuple2<Integer, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(2,"zs"));
        data.add(new Tuple2<>(4,"ls"));
        data.add(new Tuple2<>(3,"ww"));
        data.add(new Tuple2<>(1,"xw"));
        data.add(new Tuple2<>(1,"aw"));
        data.add(new Tuple2<>(1,"mw"));
        DataSource<Tuple2<Integer,String>> dataSource = environment.fromCollection(data);
        FileUtils.deleteFile("E://temp//flink//a.txt");
        FileUtils.deleteDirectory("E://temp//flink//a.txt");
        //获取前3条数据，按照数据插入的顺序
        /*GroupReduceOperator<Tuple2<Integer, String>, Tuple2<Integer, String>> first = dataSource.first(3);
        first.print();*/
        //根据数据中的第一列进行分组，获取每组的前2个元素
        //GroupReduceOperator<Tuple2<Integer,String>,Tuple2<Integer,String>> first2 = dataSource.groupBy(0).first(2);
        //根据数据中的第一列分组，再根据第二列进行组内排序[升序]，获取每组的前2个元素
        //GroupReduceOperator<Tuple2<Integer, String>, Tuple2<Integer, String>> first3 = dataSource.groupBy(0).sortGroup(1, Order.ASCENDING).first(2);
        //不分组，全局排序获取集合中的前3个元素，针对第一个元素升序，第二个元素倒序
        GroupReduceOperator<Tuple2<Integer, String>, Tuple2<Integer, String>> first4 = dataSource.sortPartition(0, Order.ASCENDING).sortPartition(1, Order.DESCENDING).first(3);
        first4.print();
        first4.writeAsText("E://temp//flink//a.txt");
        environment.execute(FirstNApplication.class.getSimpleName());
	}
}
