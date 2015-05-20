package com.linkedin.pinot.common.utils;

import com.linkedin.pinot.common.request.BrokerRequest;


/**
 * FIXME Document me!
 *
 * @author jfim
 */
public class BrokerRequestComparisonUtils {
  public static boolean areEquivalent(BrokerRequest left, BrokerRequest right) {
    boolean basicFieldsAreEquivalent = EqualityUtils.isEqual(left.getQueryType(), right.getQueryType()) &&
        EqualityUtils.isEqual(left.getQuerySource(), right.getQuerySource()) &&
        EqualityUtils.isEqual(left.getTimeInterval(), right.getTimeInterval()) &&
        EqualityUtils.isEqual(left.getDuration(), right.getDuration()) &&
        EqualityUtils.isEqual(left.getAggregationsInfo(), right.getAggregationsInfo()) &&
        EqualityUtils.isEqual(left.getGroupBy(), right.getGroupBy()) &&
        EqualityUtils.isEqual(left.getSelections(), right.getSelections()) &&
        EqualityUtils.isEqual(left.getBucketHashKey(), right.getBucketHashKey());

    // TODO Compare filter query map

    return basicFieldsAreEquivalent;
  }
}
