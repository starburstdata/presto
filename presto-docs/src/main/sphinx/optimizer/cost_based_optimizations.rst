========================
Cost based optimizations
========================

Presto supports the following cost based optimizations:
 * Cost Based Join Enumeration
 * Cost Based Join Distribution Selection

---------------------------
Cost Based Join Enumeration
---------------------------

The order in which joins are executed in a
query can have a significant impact on the query's performance. The
aspect of join ordering that has the largest impact on performance is
the size of the data being processed and passed over the network. If a
join that produces a lot of data is performed early in the execution,
then subsequent stages will need to process large amounts of data for
longer than necessary, increasing the time and resources needed for
the query.

With cost based join enumeration, Presto uses
:doc:`/optimizer/statistics` provided by the connectors to estimate
the costs for different join orders and automatically pick the
join order with the lowest computed costs.

The join enumeration strategy is governed by
``optimizer.join-reordering-strategy`` set in ``config.properties``.  This serves as
the default for ``join_reordering_strategy`` session property.

The valid values are:
 * ``COST_BASED`` (default) - full automatic join enumeration enabled
 * ``ELIMINATE_CROSS_JOINS`` - eliminate unnecessary cross joins
 * ``NONE`` - purely syntactic join order

If using ``COST_BASED`` and statistics are not available or if for any other reason a cost
could not be computed, the ``ELIMINATE_CROSS_JOINS`` strategy is used
instead.

--------------------------------------
Cost Based Join Distribution Selection
--------------------------------------

Presto uses a hash based join algorithm. That implies that for each join
operator a hash table must be created from one join input (called build
side). The other input (probe side) is then iterated and for each row the hash table is queried to find matching rows.

There are two types of join distributions:
 * REPARTITIONED - each node participating in query builds a hash table from only fraction of data
 * REPLICATED - each node participating in query builds a hash table from all of data (data is replicated)

Each type have their trade offs. Repartitioned joins require
redistributing both tables using a hash of the join key. This can be
slower (sometimes substantially) than broadcast joins, but allows much
larger joins. In particular, broadcast joins will be faster if the
build side is much smaller than the probe side. However, broadcast joins
require that the tables on the build side of the join after filtering
fit in memory on each node, whereas distributed joins only need to fit
in distributed memory across all nodes.

With cost based join distribution selection, Presto automatically
chooses the type of distributed join to use for each join. During cost based join enumeration, Presto also considers
switching the probe side with build side.

The join distribution strategy is governed by
``join-distribution-type`` set in ``config.properties``. This serves as a default
for ``join_distribution_type`` session property.

The valid values are:
 * ``AUTOMATIC`` (default) - join distribution type is determined automatically for each join
 * ``REPLICATED`` - REPLICATED join distribution is used for all joins
 * ``REPARTITIONED`` - REPARTITIONED join distribution is used for all join

-------------------------
Connector Implementations
-------------------------

In order for the Presto optimizer to use the cost based strategies,
the connector implementation must provide statistics.  Currently the
only connector that supports statistics is the :doc:`/connector/hive`.
