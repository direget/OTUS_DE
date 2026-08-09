INSERT INTO customers (name, email) VALUES
    ('Иван Петров', 'ivan@example.com'),
    ('Мария Сидорова', 'maria@example.com'),
    ('Пётр Кузнецов', 'petr@example.com'),
    ('Андрей Воронин', 'avoronin@example.com'),
    ('Агата Трушкина', 'atrushkina@example.com');

INSERT INTO products (title, price) VALUES
    ('Ноутбук', 89999.00),
    ('Мышь', 1499.00),
    ('Монитор', 24999.00),
    ('Роутер', 7420.00),
    ('Hub', 12000.01);


INSERT INTO orders (customer_id) VALUES (1), (1), (2), (3), (5), (4);

SELECT * FROM products WHERE price > 2000 ORDER BY price DESC;

SELECT * FROM customers WHERE name LIKE 'А%' ORDER BY email;

SELECT c.name, c.email, o.order_id, o.created_at
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
WHERE c.customer_id=1
ORDER BY o.created_at DESC;

SELECT c.name, c.email, o.order_id, o.created_at
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
ORDER BY c.name;