/*
 * Copyright 2013 Klarna AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.klarna.hiverunner;

import com.klarna.hiverunner.annotations.HiveProperties;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(StandaloneHiveRunner.class)
public class PartitionSupportTest {

    @HiveProperties
    public Map<String, String> hiveProperties = MapUtils.putAll(new HashMap(), new String[]{
            "table.name", "foo_bar",
            "HDFS_ROOT_FOO", "${hiveconf:hadoop.tmp.dir}"
    });

    @HiveSQL(files = "partitionSupportTest/hql_example.sql")
    public HiveShell hiveShell;


    /**
     * Hive 10 does not seem to support repair of partitions the same way as later versions.
     * Here, we just load the data into the table instead.
     */
    @Before
    public void createPartitions() throws IOException {

        File foo_tmp_folder = hiveShell.getBaseDir().newFolder("foo_tmp");

        File foo_2013_12 = new File(foo_tmp_folder, "2013_12.csv");
        File foo_2014_03 = new File(foo_tmp_folder, "2014_3.csv");
        File foo_2014_07 = new File(foo_tmp_folder, "2014_7.csv");

        FileUtils.write(foo_2013_12, "A,B,C");
        FileUtils.write(foo_2014_03, "e,f,g");
        FileUtils.write(foo_2014_07, "1,2,3");

        hiveShell.execute("LOAD DATA LOCAL INPATH '" + foo_2013_12.getAbsolutePath() + "' " +
                "INTO TABLE foo_bar PARTITION(year='2013', month='12')");

        hiveShell.execute("LOAD DATA LOCAL INPATH '" + foo_2014_03.getAbsolutePath() + "' " +
                "INTO TABLE foo_bar PARTITION(year='2014', month='3')");

        hiveShell.execute("LOAD DATA LOCAL INPATH '" + foo_2014_07.getAbsolutePath() + "' " +
                "INTO TABLE foo_bar PARTITION(year='2014', month='7')");
    }


    @Test
    public void testSelectStar() throws TException, IOException {


        List<String> expected = Arrays.asList("A\tB\tC\t2013\t12", "e\tf\tg\t2014\t3", "1\t2\t3\t2014\t7");
        List<String> actual = hiveShell.executeQuery("select * from foo_bar");

        Collections.sort(expected);
        Collections.sort(actual);

        Assert.assertEquals(expected, actual);


    }


    @Test
    public void testSelectMax() throws TException, IOException {
        // Notice that we have to cast the partition value to an int although it is actually defined as an int.
        Assert.assertEquals(Arrays.asList("12"), hiveShell.executeQuery("select max(cast(month as int)) from foo_bar"));
        Assert.assertEquals(Arrays.asList("2013"),
                hiveShell.executeQuery("select min(cast(year as int)) from foo_bar"));
    }

    @Test
    public void testShowPartitions() {
        List<String> expected = Arrays.asList("year=2013/month=12", "year=2014/month=3", "year=2014/month=7");
        List<String> actual = hiveShell.executeQuery("show partitions foo_bar");

        Collections.sort(expected);
        Collections.sort(actual);

        Assert.assertEquals(expected, actual);

    }
}
