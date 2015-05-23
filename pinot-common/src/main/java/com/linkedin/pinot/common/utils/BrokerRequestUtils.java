/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.request.FilterQueryMap;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.request.BrokerRequest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


@Deprecated
public class BrokerRequestUtils {

  @Deprecated
  public static String buildRealtimeResourceNameForResource(String hybridResource) {
    return hybridResource + CommonConstants.Broker.DataResource.REALTIME_RESOURCE_SUFFIX;
  }

  @Deprecated
  public static String buildOfflineResourceNameForResource(String hybridResource) {
    return hybridResource + CommonConstants.Broker.DataResource.OFFLINE_RESOURCE_SUFFIX;
  }

  @Deprecated
  public static TableType getResourceTypeFromResourceName(String resourceName) {
    if (resourceName.endsWith(CommonConstants.Broker.DataResource.REALTIME_RESOURCE_SUFFIX)) {
      return TableType.REALTIME;
    }
    return TableType.OFFLINE;
  }

  @Deprecated
  public static String getRealtimeResourceNameForResource(String resourceName) {
    if (resourceName.endsWith(CommonConstants.Broker.DataResource.REALTIME_RESOURCE_SUFFIX)) {
      return resourceName;
    } else {
      return BrokerRequestUtils.buildRealtimeResourceNameForResource(resourceName);
    }
  }

  @Deprecated
  public static String getOfflineResourceNameForResource(String resourceName) {
    if (resourceName.endsWith(CommonConstants.Broker.DataResource.OFFLINE_RESOURCE_SUFFIX)) {
      return resourceName;
    } else {
      return BrokerRequestUtils.buildOfflineResourceNameForResource(resourceName);
    }
  }

  @Deprecated
  public static String getHybridResourceName(String resourceName) {
    if (resourceName.endsWith(CommonConstants.Broker.DataResource.REALTIME_RESOURCE_SUFFIX)) {
      return resourceName.substring(0, resourceName.length()
          - CommonConstants.Broker.DataResource.REALTIME_RESOURCE_SUFFIX.length());
    }
    if (resourceName.endsWith(CommonConstants.Broker.DataResource.OFFLINE_RESOURCE_SUFFIX)) {
      return resourceName.substring(0,
          resourceName.length() - CommonConstants.Broker.DataResource.OFFLINE_RESOURCE_SUFFIX.length());
    }
    return resourceName;
  }

  public static boolean areEquivalent(BrokerRequest left, BrokerRequest right) {
    boolean basicFieldsAreEquivalent = EqualityUtils.isEqual(left.getQueryType(), right.getQueryType()) &&
            EqualityUtils.isEqual(left.getQuerySource(), right.getQuerySource()) &&
            EqualityUtils.isEqual(left.getTimeInterval(), right.getTimeInterval()) &&
            EqualityUtils.isEqual(left.getDuration(), right.getDuration()) &&
            EqualityUtils.isEqual(left.getSelections(), right.getSelections()) &&
            EqualityUtils.isEqual(left.getBucketHashKey(), right.getBucketHashKey());

    boolean aggregationsAreEquivalent = true;

    List<AggregationInfo> leftAggregationsInfo = left.getAggregationsInfo();
    List<AggregationInfo> rightAggregationsInfo = right.getAggregationsInfo();
    if (!EqualityUtils.isEqual(leftAggregationsInfo, rightAggregationsInfo)) {
      if (leftAggregationsInfo == null || rightAggregationsInfo == null ||
          leftAggregationsInfo.size() != rightAggregationsInfo.size()) {
        aggregationsAreEquivalent = false;
      } else {
        int aggregationsInfoCount = leftAggregationsInfo.size();
        for (int i = 0; i < aggregationsInfoCount; i++) {
          AggregationInfo leftInfo = leftAggregationsInfo.get(i);
          AggregationInfo rightInfo = rightAggregationsInfo.get(i);

          // Check if the aggregationsInfo are the same or they're the count function
          if (EqualityUtils.isEqual(leftInfo, rightInfo)) {
            continue;
          } else {
            if ("count".equalsIgnoreCase(rightInfo.getAggregationType()) &&
                "count".equalsIgnoreCase(leftInfo.getAggregationType())
                ) {
              continue;
            } else {
              aggregationsAreEquivalent = false;
              break;
            }
          }
        }
      }
    }

    // Group by clauses might not be in the same order
    boolean groupByClauseIsEquivalent = EqualityUtils.isEqual(left.getGroupBy(), right.getGroupBy());

    if (!groupByClauseIsEquivalent) {
      groupByClauseIsEquivalent =
          (EqualityUtils.isEqualIgnoringOrder(left.getGroupBy().getColumns(), right.getGroupBy().getColumns()) &&
              EqualityUtils.isEqual(left.getGroupBy().getTopN(), right.getGroupBy().getTopN()));
    }

    boolean filtersAreEquivalent = EqualityUtils.isEqual(left.isSetFilterQuery(), right.isSetFilterQuery());

    if (left.isSetFilterQuery()) {
      int leftRootId = left.getFilterQuery().getId();
      int rightRootId = right.getFilterQuery().getId();
      filtersAreEquivalent = filterQueryIsEquivalent(
          Collections.singletonList(leftRootId),
          Collections.singletonList(rightRootId),
          left.getFilterSubQueryMap(),
          right.getFilterSubQueryMap()
      );
    }

    return basicFieldsAreEquivalent && aggregationsAreEquivalent && groupByClauseIsEquivalent && filtersAreEquivalent;
  }

  public static boolean filterQueryIsEquivalent(List<Integer> leftIds, List<Integer> rightIds,
      FilterQueryMap leftFilterQueries, FilterQueryMap rightFilterQueries) {
    if (leftIds.size() != rightIds.size()) {
      return false;
    }

    ArrayList<Integer> leftIdsCopy = new ArrayList<Integer>(leftIds);
    Iterator<Integer> leftIterator = leftIdsCopy.iterator();
    while (leftIterator.hasNext()) {
      Integer leftId = leftIterator.next();
      FilterQuery leftQuery = leftFilterQueries.getFilterQueryMap().get(leftId);

      Iterator<Integer> rightIterator = new ArrayList<Integer>(rightIds).iterator();
      while (rightIterator.hasNext()) {
        Integer rightId = rightIterator.next();
        FilterQuery rightQuery = rightFilterQueries.getFilterQueryMap().get(rightId);

        boolean operatorsAreEqual = EqualityUtils.isEqual(leftQuery.getOperator(), rightQuery.getOperator());
        boolean columnsAreEqual = EqualityUtils.isEqual(leftQuery.getColumn(), rightQuery.getColumn());
        boolean fieldsAreEqual = columnsAreEqual &&
            operatorsAreEqual &&
            EqualityUtils.isEqual(leftQuery.getValue(), rightQuery.getValue());

        // Compare sets if the op is IN
        if (operatorsAreEqual && columnsAreEqual && leftQuery.getOperator() == FilterOperator.IN) {
          Set<String> leftValues = new HashSet<String>(Arrays.asList(leftQuery.getValue().get(0).split("\t\t")));
          Set<String> rightValues = new HashSet<String>(Arrays.asList(rightQuery.getValue().get(0).split("\t\t")));
          fieldsAreEqual = leftValues.equals(rightValues);
          if (!fieldsAreEqual) {
            System.out.println("in clause not the same?");
            System.out.println("leftValues = " + leftValues);
            System.out.println("rightValues = " + rightValues);
          }
        }

        if (fieldsAreEqual) {
          if (filterQueryIsEquivalent(
              leftQuery.getNestedFilterQueryIds(),
              rightQuery.getNestedFilterQueryIds(),
              leftFilterQueries,
              rightFilterQueries
          )) {
            leftIterator.remove();
            rightIterator.remove();
          } else {
            return false;
          }
        }
      }
    }

    return leftIdsCopy.isEmpty();
  }
}
