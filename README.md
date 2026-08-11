docker run --name pg-shop3 -e POSTGRES_PASSWORD=* -e POSTGRES_DB=* -p 5433:5432 -d postgres:16.1
docker exec -it pg-shop3 psql -U postgres

shop=# SELECT * FROM customers WHERE name LIKE 'А%' ORDER BY email;
 customer_id |      name      |         email
-------------+----------------+------------------------
           5 | Агата Трушкина | atrushkina@example.com
           4 | Андрей Воронин | avoronin@example.com
(2 rows)

Time: 0.468 ms

shop=# SELECT c.name, c.email, o.order_id, o.created_at
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
WHERE c.customer_id=1
ORDER BY o.created_at DESC;
    name     |      email       | order_id |         created_at
-------------+------------------+----------+----------------------------
 Иван Петров | ivan@example.com |        1 | 2026-08-09 18:46:27.559377
 Иван Петров | ivan@example.com |        2 | 2026-08-09 18:46:27.559377
(2 rows)

Time: 0.814 ms

shop=# SELECT c.name, c.email, o.order_id, o.created_at
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
ORDER BY c.name;
      name      |         email          | order_id |         created_at
----------------+------------------------+----------+----------------------------
 Агата Трушкина | atrushkina@example.com |        5 | 2026-08-09 18:46:27.559377
 Андрей Воронин | avoronin@example.com   |        6 | 2026-08-09 18:46:27.559377
 Иван Петров    | ivan@example.com       |        1 | 2026-08-09 18:46:27.559377
 Иван Петров    | ivan@example.com       |        2 | 2026-08-09 18:46:27.559377
 Мария Сидорова | maria@example.com      |        3 | 2026-08-09 18:46:27.559377
 Пётр Кузнецов  | petr@example.com       |        4 | 2026-08-09 18:46:27.559377
(6 rows)

Time: 0.882 ms

shop=# SELECT * FROM products WHERE price > 2000 ORDER BY price DESC;
 product_id |  title  |  price
------------+---------+----------
          1 | Ноутбук | 89999.00
          3 | Монитор | 24999.00
          5 | Hub     | 12000.01
          4 | Роутер  |  7420.00
(4 rows)

Time: 0.768 ms
