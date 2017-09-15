/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.hive.BackgroundHiveSplitLoader.fillInEmptyBuckets;
import static java.util.Collections.emptySet;
import static org.testng.Assert.assertEquals;

public class TestBackgroundHiveSplitLoader
{
    @Test
    public void testFillInAllEmptyBuckets()
    {
        assertEquals(
                fillInEmptyBuckets(ImmutableList.of(), 5),
                ImmutableList.of(emptySet(), emptySet(), emptySet(), emptySet(), emptySet()));
    }

    @Test
    public void testFillInInitialEmptyBucket()
    {
        List<Set<LocatedFileStatus>> initialBucketList = createBucketList("000001_1", "000002_1", "000003_1", "000004_1");
        ImmutableList.Builder<Set<LocatedFileStatus>> expectedBucketList = ImmutableList.builder();
        expectedBucketList.add(emptySet());
        expectedBucketList.addAll(initialBucketList);
        assertEquals(
                fillInEmptyBuckets(initialBucketList, 5),
                expectedBucketList.build());
    }

    @Test
    public void testFillInVariousEmptyBuckets()
    {
        List<Set<LocatedFileStatus>> initialBucketList = createBucketList("000000_1", "000003_1");
        ImmutableList.Builder<Set<LocatedFileStatus>> expectedBucketList = ImmutableList.builder();
        expectedBucketList.add(initialBucketList.get(0));
        expectedBucketList.add(emptySet());
        expectedBucketList.add(emptySet());
        expectedBucketList.add(initialBucketList.get(1));
        expectedBucketList.add(emptySet());
        assertEquals(
                fillInEmptyBuckets(initialBucketList, 5),
                expectedBucketList.build());
    }

    private List<Set<LocatedFileStatus>> createBucketList(String... bucketIds)
    {
        ImmutableList.Builder<Set<LocatedFileStatus>> bucketListBuilder = ImmutableList.builder();
        for (String bucketId : bucketIds) {
            bucketListBuilder.add(ImmutableSet.of(getLocatedFileStatus(bucketId)));
        }
        return bucketListBuilder.build();
    }

    private LocatedFileStatus getLocatedFileStatus(String bucketId)
    {
        return new LocatedFileStatus(0, false, 0, 0L, 0L, 0L, FsPermission.createImmutable((short) 777), "owner", "group", null, new Path("/user/hive/warehouse/" + bucketId), null);
    }
}
