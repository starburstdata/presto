===============
Release 0.195-e
===============

General changes
---------------

* Introduce cost-based plan optimizations.
* Replace ``distributed_join`` session property with ``join_distribution_type``.
* Replace ``reorder_joins`` session property with ``join_reordering_strategy``.
* Introduce distributed sort. It is now possible to turn on distributed sort via experimental ``distributed_sort`` session property
  (off by default). Distributed sort will help in cases when sorted data doesn't fit on single node.
  Performance improvement can be expected, but partially distributed data streams are still
  merged on a single node.
* Fix sort memory leak.
* Add support for ROLE management including CREATE ROLE, DROP ROLE, GRANT ROLE, REVOKE ROLE, SET ROLE,
  SHOW CURRENT ROLES, SHOW ROLES and SHOW ROLE GRANTS commands.
* Support prepared statements that are longer than 4K bytes.
* Support predicate pushdown for the `<column> IN <values list>` predicate
  where values in the values list require casting to match the type of column.
* Improve predicate inferring/pushdown between source and filtering subquery
  for queries like ``SELECT ... WHERE <symbol> IN <subquery>``.

Hive Connector changes
----------------------

* Allow partitions without files for bucketed tables (via ``hive.empty-bucketed-partitions.enabled``).
* Allow multiple files per bucket for bucketed tables (via ``hive.multi-file-bucketing.enabled``).
  There must be one or more files per bucket. File names must match the Hive naming convention.
* Allow reading incompletely bucketed tables with missing files (via ``hive.empty-bucketed-partitions.enabled``).
* Allow disabling ability to create external tables from Presto (via ``hive.non-managed-table-creates-enabled``).
* Implement ROLE management support.

JMX Connector changes
---------------------

* Add wildcard character `*` which allows to query several MBeans data with single query.

Security changes
----------------

* Secured internal cluster communication over HTTPS using Kerberos or LDAP authentication
* Add a Kerberos principal rule for exact match to File Based System Access Control

Data type changes
-----------------

* TIMESTAMP semantic changes (see :doc:`/language/time`)
* The ``current_time`` and ``localtime`` functions were fixed to return the correct value for non-UTC timezones.
* Make ``DECIMAL`` the default literal for non-integral numbers.
* Fix decimal precision of ``round(x, d)`` when ``x`` is a ``DECIMAL``.
* Fix returned value from ``round(x, d)`` when ``x`` is a ``DECIMAL`` with
  scale ``0`` and ``d`` is a negative integer. Previously, no rounding was done
  in this case.

===================
Release 0.195-e.0.4
===================

* Fix bug with internal cluster communication over HTTPS using LDAP when only `PASSWORD` authentication type is set.
