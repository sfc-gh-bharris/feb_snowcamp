use role accountadmin;
use database snowflake_sample_data;
use schema TPCH_SF100;
use warehouse perf_wh;

-- I'm going to turn off caching for this session. 
-- You would probably never do this in a production environment.
ALTER SESSION SET USE_CACHED_RESULT = FALSE;


-- we can see that the orders table has 150,000,000 rows and we'll join it to the customer data which has 15,000,000 rows
show tables;


-- This query is going to pull back a year's worth of orders and join to the customer. 
-- It will probably take about 10 seconds to run.
select o.o_orderkey, o.o_orderdate, c.c_custkey, c.c_name, c.c_phone from orders o
inner join customer c
on o.o_custkey = c.c_custkey
where o.o_orderdate > '1993-01-01'
and o.o_orderdate < '1994-01-01';

-- Behind the scenes, the snowflake optimizer is scanning the metadata and building out a query plan, then going to get the data from storage, joining as needed and then building a resultset. 
-- We can take a look at the query plan when this is completed.
-- When you click on the 'Query ID' you will see the detailed information about that query. 

-- This is a less optimized query that returns the same set of results
select * from customer c
left outer join orders o
on o.o_custkey = c.c_custkey
where LEFT(o.o_orderdate,4) = '1993';

-- You can see here that even though we're doing quite a few things that are less than ideal
-- (left join when you should use an inner join, narrowing by a field converted from a date to a string, and returning all the columns)
-- Your query still comes back in a fair amount of time. But that's because our dataset and query is still fairly small.
-- If we use the largest sample dataset we have, you can see how poor queries can have an impact.

use schema TPCH_SF1000;
-- we can see that the orders table has 1,500,000,000 rows and we'll join it to the customer data which has 150,000,000 rows. This is a 10x increase vs our first query
show tables;

-- we will run our "optimized query first".
-- this will probably take about 60 seconds to run.
select o.o_orderkey, o.o_orderdate, c.c_custkey, c.c_name, c.c_phone from orders o
inner join customer c
on o.o_custkey = c.c_custkey
where o.o_orderdate > '1993-01-01'
and o.o_orderdate < '1994-01-01';

-- lets go ahead and scale up our warehouse to see if we can cut the run time. Remember, each time we scale, our run time should decrease in a fairly linear fashion (to a point).
alter warehouse perf_wh set warehouse_size = 'LARGE';

-- we should see our time get cut by about 8x.
select o.o_orderkey, o.o_orderdate, c.c_custkey, c.c_name, c.c_phone from orders o
inner join customer c
on o.o_custkey = c.c_custkey
where o.o_orderdate > '1993-01-01'
and o.o_orderdate < '1994-01-01';

-- Lets open the query analyzer for this query and also run the less optimized version below.
select * from customer c
left outer join orders o
on o.o_custkey = c.c_custkey
where LEFT(o.o_orderdate,4) = '1993'
order by o.o_orderdate asc;

-- when this one is done, we can open the query analyzer and compare.

-- While we're waiting, what makes our first query more optimized than our second?
-- First, only return the columns you need, rather than all the columns available.
-- Column stores like Snowflake incur unnecessary I/O with more columns. Whenever possible limit SELECT on long strings or large variant columns
-- Do not use an order by unless it's required. Order by may use significant resources because you're essentially reading all of the returned data, sorting and rewriting it to the result.
-- Finally, a cast at runtime (for instance, taking the left 4 characters of a year) can have a big impact if it prevents the optimizer from narrowing the partitions that are scanned.

-- When we're looking at our query analysis, what should we look for to see if our query is optimized?

-- Partitions scanned vs Partitions total tells you how much data Snowflake needed to go through to find your results in the data.
-- You ideally want to scan fewer partitions than you have total, however, this is not something you will be able to control. If you're consistently seeing that you're scanning all of the partitions, you may want to reach out to your internal Snowflake team for help.

-- Bytes spilled to local storage - this tells you that we ran out of memory on our warehouse. This MAY mean that your data is just too large for the warehouse you specified, or it MAY mean that your query is not optimized. If you see consistently this, it's a good indication that you need to rethink your query and rethink the warehouse size you're using.

-- Bytes spilled to remote storage - this means that you ran out of SSD space on the warehouse and now you're using remote storage for your database operations. This is something you should try to avoid as much as possible. This is definitely something to avoid.

-- when we're done, lets scale back down our warehouse
alter warehouse perf_wh set warehouse_size = 'XSMALL';