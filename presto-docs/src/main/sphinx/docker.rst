============================================
Running Presto sandbox in a Docker container
============================================

It is possible to run sandboxed Presto in a Docker container:

.. code-block:: none

    docker run -d -p 127.0.0.1:8080:8080 --name presto starburstdata/presto:0.195-e.0.2

Running Presto CLI client
^^^^^^^^^^^^^^^^^^^^^^^^^

When Presto Server is started you can start Presto CLI by running:

.. code-block:: none

    docker exec -it presto java -jar /presto-cli.jar

Presto cluster overview
^^^^^^^^^^^^^^^^^^^^^^^

Presto cluster overview webpage is available at `localhost:8080 <http://localhost:8080/>`_

Presto connectors
^^^^^^^^^^^^^^^^^

Presto docker container is started with following connectors:

* tpch
* tpcds
* memory
* blackhole
* jmx
* system

Oracle license
^^^^^^^^^^^^^^

By using this image, you accept the Oracle Binary Code License Agreement for Java SE available here: `Oracle license`_

.. _Oracle license: http://www.oracle.com/technetwork/java/javase/terms/license/index.html
