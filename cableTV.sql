CREATE TABLE subscribers (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  address TEXT NOT NULL,
  phone TEXT NOT NULL UNIQUE,
  balance DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
  last_payment_date DATE,
  suspended BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE services (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  monthly_price DECIMAL(10, 2) NOT NULL DEFAULT 0.00
);

CREATE TABLE packages (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  monthly_price DECIMAL(10, 2) NOT NULL DEFAULT 0.00
);

CREATE TABLE movies (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  genre TEXT,
  release_date DATE,
  price DECIMAL(10, 2) NOT NULL DEFAULT 0.00
);

CREATE TABLE subscriptions (
  id SERIAL PRIMARY KEY,
  subscriber_id INTEGER NOT NULL REFERENCES subscribers(id),
  service_id INTEGER NOT NULL REFERENCES services(id),
  package_id INTEGER REFERENCES packages(id),
  start_date DATE NOT NULL,
  end_date DATE NOT NULL
);

CREATE TABLE movie_orders (
  id SERIAL PRIMARY KEY,
  subscriber_id INTEGER NOT NULL REFERENCES subscribers(id),
  movie_id INTEGER NOT NULL REFERENCES movies(id),
  order_date DATE NOT NULL,
  price DECIMAL(10, 2) NOT NULL DEFAULT 0.00
);

CREATE TABLE payments (
  id SERIAL PRIMARY KEY,
  subscriber_id INTEGER NOT NULL REFERENCES subscribers(id),
  amount DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
  payment_date DATE NOT NULL
);

--1
-- Вибірка із таблиці subscribers з сортуванням за ім'ям
SELECT * FROM subscribers ORDER BY name;

-- Вибірка із таблиці subscribers, де баланс > 1000 або suspended = true
SELECT * FROM subscribers WHERE balance > 1000 OR suspended = true;

-- Вибірка із таблиці subscribers, де адреса починається на "New York" і номер телефону не починається на "+1"
SELECT * FROM subscribers WHERE address LIKE 'New York%' AND phone NOT LIKE '+1%';


--2
-- Вибірка середньої місячної вартості послуг
SELECT AVG(monthly_price) AS avg_monthly_price FROM services;

-- Вибірка кількості фільмів в кожному жанрі
SELECT genre, COUNT(*) AS num_movies FROM movies GROUP BY genre;


--3
-- Вибірка інформації про підписки, де послуга має ціну менше 50 і або пакет не вказаний, або пакет має ціну менше 50
SELECT subscriptions.*, services.monthly_price AS service_price, packages.monthly_price AS package_price
FROM subscriptions
LEFT JOIN services ON subscriptions.service_id = services.id
LEFT JOIN packages ON subscriptions.package_id = packages.id
WHERE (services.monthly_price < 50 AND packages.id IS NOT NULL) OR packages.monthly_price < 50
ORDER BY start_date;

-- Вибірка інформації про замовлення фільмів, зроблених або підписниками з балансом більше 500, або замовлених вартістю більше 100
SELECT movie_orders.*, subscribers.balance, movies.price
FROM movie_orders
LEFT JOIN subscribers ON movie_orders.subscriber_id = subscribers.id
LEFT JOIN movies ON movie_orders.movie_id = movies.id
WHERE (subscribers.balance > 500 OR movies.price > 100)
ORDER BY order_date DESC;

--4
-- SELECT на базі кількох таблиць з типом поєднання Outer Join:
SELECT subscribers.name, services.name 
FROM subscribers 
LEFT OUTER JOIN subscriptions 
ON subscribers.id = subscriptions.subscriber_id 
LEFT OUTER JOIN services 
ON subscriptions.service_id = services.id;

--5 
-- SELECT з використанням операторів Like, Between, In, Exists, All, Any:
-- Like
SELECT * FROM movies WHERE title LIKE '%Memento%';

-- Between
SELECT * FROM movies WHERE release_date BETWEEN '2020-01-01' AND '2022-12-31';

-- In
SELECT * FROM movies WHERE genre IN ('comedy', 'drama');

-- Exists
SELECT * FROM subscribers 
WHERE EXISTS (SELECT * FROM subscriptions WHERE subscriber_id = subscribers.id);

-- All
SELECT * FROM subscribers WHERE balance >= ALL (SELECT monthly_price FROM services);

-- Any
SELECT * FROM subscribers WHERE suspended = ANY (SELECT suspended FROM subscriptions WHERE subscriber_id = subscribers.id);


--6
-- SELECT з використанням підсумовування та групування:
SELECT service_id, COUNT(*) AS subscribers_count, AVG(balance) AS avg_balance 
FROM subscriptions 
JOIN subscribers 
ON subscriptions.subscriber_id = subscribers.id 
GROUP BY service_id;


--7
-- SELECT з використанням під-запитів в частині Where:
SELECT * FROM subscribers 
WHERE balance > (SELECT AVG(balance) FROM subscribers);


--8
-- SELECT з використанням під-запитів в частині From:
SELECT * FROM 
(SELECT service_id, COUNT(*) AS subscribers_count FROM subscriptions GROUP BY service_id) AS subs_count 
JOIN services ON subs_count.service_id = services.id;


--9
-- iєрархічний SELECT-запит:
WITH RECURSIVE sub_packages AS (
  SELECT id, name, monthly_price FROM packages WHERE name = 'tv'
  UNION
  SELECT packages.id, packages.name, packages.monthly_price 
  FROM packages 
  JOIN sub_packages ON packages.id = sub_packages.id 
  WHERE packages.name LIKE 'tv'
) SELECT * FROM sub_packages;


--10
-- SELECT-запит типу CrossTab:
SELECT * FROM crosstab(
  'SELECT service_id, EXTRACT(MONTH FROM start_date) AS month, COUNT(*) AS subscribers_count 
   FROM subscriptions 
   WHERE EXTRACT(YEAR FROM start_date) = 2022 
   GROUP BY service_id, EXTRACT(MONTH FROM start_date) 
   ORDER BY service_id, EXTRACT(MONTH FROM start_date)',
  'SELECT generate_series(1, 12)'
) AS ct(service_id INTEGER, "Jan" INTEGER, "Feb" INTEGER, "Mar" INTEGER, "Apr" INTEGER, "May" INTEGER, 
      "Jun" INTEGER, "Jul" INTEGER, "Aug" INTEGER, "Sep" INTEGER, "Oct" INTEGER, "Nov" INTEGER, "Dec" INTEGER);


--11
-- UPDATE на базі однієї таблиці:
UPDATE subscribers SET balance = balance + 10.00 WHERE id = 123;


--12
-- UPDATE на базі кількох таблиць:
UPDATE subscribers 
SET balance = balance + packages.monthly_price 
FROM subscriptions 
JOIN packages ON subscriptions.package_id = packages.id 
WHERE subscribers.id = subscriptions.subscriber_id;


--13 
-- Append (INSERT) для додавання записів з явно вказаними значеннями.
--Додати підписника:
INSERT INTO subscribers (id, name, address, phone) VALUES (7, 'John Smith', '123 Main St', '555-1234');
--Додати послугу:
INSERT INTO services (id, name, description, monthly_price) VALUES (3, 'Basic Internet', 'Standard internet service', 29.99);
--Додати пакет:
INSERT INTO packages (id, name, description, monthly_price) VALUES (3, 'Premium TV', 'Includes premium TV channels', 49.99);


--14
-- Append (INSERT) для додавання записів з інших таблиць.
--Додати нову послугу з вибіркою з існуючого пакету:
INSERT INTO services (id, name, description)
SELECT 8, 'premium tv series', 60
FROM packages
WHERE name = 'tv';

--Додати нову підписку з вибором послуги та пакету від підписника з id = 2:
INSERT INTO subscriptions (subscriber_id, service_id, package_id, start_date, end_date)
SELECT 2, id, (SELECT id FROM packages WHERE name = 'Basic'), '2023-04-01', '2023-05-01'
FROM services
WHERE name = 'Basic Internet';


--15
-- DELETE для видалення всіх даних з таблиці.
--Видалити всіх підписників:
DELETE FROM subscribers;

--Видалити всі фільми:
DELETE FROM movies;


--16
-- DELETE для видалення вибраних записів таблиці.
--Видалити підписника з id = 1:
DELETE FROM subscribers WHERE id = 1;

--Видалити усі підписки на послугу з id = 2:
DELETE FROM subscriptions WHERE service_id = 2;


--3lab (To create a stored procedure to calculate the payment 
--for a given month and subscriber, you can use the following code:)
CREATE OR REPLACE PROCEDURE calculate_payment(
    subscriber_id INTEGER,
    month INTEGER,
    year INTEGER
) AS $$
DECLARE
    total DECIMAL(10, 2) := 0.00;
BEGIN
    SELECT SUM(monthly_price)
    INTO total
    FROM subscriptions s
    INNER JOIN services ON s.service_id = services.id
    WHERE s.subscriber_id = calculate_payment.subscriber_id
    AND start_date <= date_trunc('month', make_date(year, month, 1))
    AND (end_date IS NULL OR end_date >= date_trunc('month', make_date(year, month, 1)));

    IF total IS NOT NULL THEN
        UPDATE subscribers
        SET balance = balance + total
        WHERE id = subscriber_id;

        INSERT INTO payments(subscriber_id, amount, payment_date)
        VALUES (subscriber_id, total, make_date(year, month, 1));
    END IF;
END;
$$ LANGUAGE plpgsql;

CALL calculate_payment(7, 1, 2022);

--To create a stored procedure that calls the calculate_payment 
--function for all subscribers, you can use the following code:
CREATE OR REPLACE PROCEDURE calculate_payments_for_all(
    month INTEGER,
    year INTEGER
) AS $$
DECLARE
    subscriber RECORD;
BEGIN
    FOR subscriber IN SELECT * FROM subscribers
    LOOP
        CALL calculate_payment(subscriber.id, month, year);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CALL calculate_payments_for_all(7, 2022);

--1. У всіх таблицях створити поля UCR, DCR, ULC, DLC
-- subscribers
CREATE OR REPLACE FUNCTION update_subscriber_info() RETURNS TRIGGER AS $$
DECLARE
    now_date date := current_date;
BEGIN
    -- Check if subscriber has unpaid services for last 3 months
    IF EXISTS (
        SELECT 1 FROM subscriptions s
        JOIN payments p ON p.subscriber_id = s.subscriber_id AND p.payment_date >= now_date - INTERVAL '3 months'
        WHERE s.subscriber_id = NEW.id AND s.end_date > now_date
        HAVING SUM(p.amount) < SUM(s.service.monthly_price + COALESCE(s.package.monthly_price, 0))
    ) THEN
        -- Set suspended flag to true and disallow new service subscriptions
        UPDATE subscribers SET suspended = true WHERE id = NEW.id;
        RAISE EXCEPTION 'Subscriber % is suspended due to unpaid services for last 3 months', NEW.id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER subscribers_created 
  BEFORE INSERT ON subscribers 
  FOR EACH ROW 
  EXECUTE FUNCTION update_subscriber_info();

-- services
CREATE OR REPLACE FUNCTION services_created()
RETURNS TRIGGER AS $$
BEGIN
    NEW.created_by = current_user;
    NEW.created_at = current_timestamp;
    NEW.modified_by = current_user;
    NEW.modified_at = current_timestamp;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER services_created
BEFORE INSERT ON services
FOR EACH ROW
EXECUTE FUNCTION services_created();

--packages
CREATE OR REPLACE FUNCTION packages_created()
RETURNS TRIGGER AS $$
BEGIN
    NEW.ucr = current_user; 
    NEW.dcr = current_timestamp; 
    NEW.ulc = current_user; 
    NEW.dlc = current_timestamp;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER packages_created 
  BEFORE INSERT ON packages 
  FOR EACH ROW 
  EXECUTE FUNCTION packages_created();

CREATE OR REPLACE FUNCTION packages_updated()
RETURNS TRIGGER AS $$
BEGIN 
    NEW.ulc = current_user; 
    NEW.dlc = current_timestamp; 
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER packages_updated 
  BEFORE UPDATE ON packages 
  FOR EACH ROW 
  EXECUTE FUNCTION packages_updated();

--movies
CREATE OR REPLACE FUNCTION movies_created()
RETURNS TRIGGER AS $$
BEGIN 
    NEW.ucr = current_user; 
    NEW.dcr = current_timestamp; 
    NEW.ulc = current_user; 
    NEW.dlc = current_timestamp;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER movies_created 
  BEFORE INSERT ON movies 
  FOR EACH ROW 
  EXECUTE FUNCTION movies_created();

CREATE OR REPLACE FUNCTION movies_updated()
RETURNS TRIGGER AS $$
BEGIN 
    NEW.ulc = current_user; 
    NEW.dlc = current_timestamp; 
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER movies_updated 
  BEFORE UPDATE ON movies 
  FOR EACH ROW 
  EXECUTE FUNCTION movies_updated();

--subscriptions
CREATE OR REPLACE FUNCTION subscriptions_created()
RETURNS TRIGGER AS $$
BEGIN 
    NEW.ucr = current_user; 
    NEW.dcr = current_timestamp; 
    NEW.ulc = current_user; 
    NEW.dlc = current_timestamp;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER subscriptions_created 
  BEFORE INSERT ON subscriptions 
  FOR EACH ROW 
  EXECUTE FUNCTION subscriptions_created();

CREATE OR REPLACE FUNCTION subscriptions_updated()
RETURNS TRIGGER AS $$
BEGIN 
    NEW.ulc = current_user; 
    NEW.dlc = current_timestamp; 
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER subscriptions_updated 
  BEFORE UPDATE ON subscriptions 
  FOR EACH ROW 
  EXECUTE FUNCTION subscriptions_updated();

--movie_orders
CREATE OR REPLACE FUNCTION movie_orders_created()
RETURNS TRIGGER AS $$
BEGIN 
    NEW.ucr = current_user; 
    NEW.dcr = current_timestamp; 
    NEW.ulc = current_user; 
    NEW.dlc = current_timestamp;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER movie_orders_created 
  BEFORE INSERT ON movie_orders 
  FOR EACH ROW 
  EXECUTE FUNCTION movie_orders_created();

CREATE OR REPLACE FUNCTION movie_orders_updated()
RETURNS TRIGGER AS $$
BEGIN 
    NEW.ulc = current_user; 
    NEW.dlc = current_timestamp; 
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER movie_orders_updated 
  BEFORE UPDATE ON movie_orders 
  FOR EACH ROW 
  EXECUTE FUNCTION movie_orders_updated();

--payments
CREATE OR REPLACE FUNCTION payments_created()
RETURNS TRIGGER AS $$
BEGIN 
    NEW.ucr = current_user; 
    NEW.dcr = current_timestamp; 
    NEW.ulc = current_user; 
    NEW.dlc = current_timestamp;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

ALTER TABLE payments ADD COLUMN ucr TEXT;

CREATE OR REPLACE TRIGGER payments_created 
  BEFORE INSERT ON payments 
  FOR EACH ROW 
  EXECUTE FUNCTION payments_created();

CREATE OR REPLACE FUNCTION payments_updated()
RETURNS TRIGGER AS $$
BEGIN 
    NEW.ulc = current_user; 
    NEW.dlc = current_timestamp; 
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER payments_updated 
  BEFORE UPDATE ON payments
  FOR EACH ROW 
  EXECUTE FUNCTION payments_updated();
ERROR:  record "new" has no field "dcr"
CONTEXT:  PL/pgSQL assignment "NEW.dcr = current_timestamp"

--2.Створити сурогатний ключ для деякої таблиці, 
--та написати тригер для обов’язкового заповнення цього поля послідовними значеннями.
CREATE OR REPLACE FUNCTION set_subscription_id()
RETURNS TRIGGER AS $$
DECLARE
  last_id INTEGER;
BEGIN
  SELECT MAX(subscription_id) INTO last_id FROM subscriptions;
  IF last_id IS NULL THEN
    last_id := 0;
  END IF;
  NEW.subscription_id = last_id + 1;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_subscription_id_trigger
BEFORE INSERT ON subscriptions
FOR EACH ROW
WHEN (NEW.subscription_id IS NULL)
EXECUTE FUNCTION set_subscription_id();
--Цей тригер буде виконуватися перед кожним INSERT в таблицю "subscriptions" 
--і буде встановлювати значення поля "subscription_id" для нового запису 
--на основі останнього доданого значення, збільшеного на 1. Якщо таблиця ще порожня, 
--то значення буде встановлено на 1.

--3
--Перший тригер для перевірки наявності боргу
CREATE OR REPLACE FUNCTION check_subscriber_balance() RETURNS TRIGGER AS $$
DECLARE
  last_payment_date DATE;
  amount_owed DECIMAL(10, 2);
BEGIN
  SELECT MAX(payment_date) INTO last_payment_date FROM payments WHERE subscriber_id = NEW.subscriber_id;
  IF last_payment_date IS NULL OR last_payment_date < (CURRENT_DATE - INTERVAL '3 months') THEN
    SELECT SUM(amount) INTO amount_owed FROM payments WHERE subscriber_id = NEW.subscriber_id;
    IF amount_owed IS NULL THEN
      amount_owed := 0;
    END IF;
    IF amount_owed > 0 THEN
      UPDATE subscribers SET suspended = true WHERE id = NEW.subscriber_id;
      RAISE EXCEPTION 'Subscriber with id % has unpaid balance of %. New service subscriptions are not allowed.', NEW.subscriber_id, amount_owed;
    END IF;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_subscriber_balance_trigger
BEFORE INSERT ON subscriptions
FOR EACH ROW
EXECUTE FUNCTION check_subscriber_balance();


--Другий тригер для перевірки віку абонента при замовленні кінофільмів для дорослих 
CREATE OR REPLACE FUNCTION check_subscriber_age() RETURNS TRIGGER AS $$
DECLARE
  subscriber_age INTEGER;
BEGIN
  SELECT DATE_PART('year', age(NOW(), subscribers.date_of_birth)) INTO subscriber_age FROM subscribers WHERE id = NEW.subscriber_id;
  IF subscriber_age < 18 AND EXISTS (SELECT 1 FROM movies WHERE id = NEW.movie_id AND genre = 'adult') THEN
    RAISE EXCEPTION 'Subscriber with id % is not allowed to order adult movies.', NEW.subscriber_id;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_subscriber_age_trigger
BEFORE INSERT ON movie_orders
FOR EACH ROW
EXECUTE FUNCTION check_subscriber_age();

--lab5
--Адміністратор БД: 
GRANT ALL PRIVILEGES ON postgres.* TO 'postgres'@'localhost';
--Клієнт: 
GRANT SELECT, INSERT, UPDATE ON postgres.payments TO 'martyyy'@'localhost';
--Продавець: 
GRANT SELECT, INSERT, UPDATE, DELETE ON postgres.services TO 'martin'@'localhost';
--Менеджер з доставки: 
GRANT SELECT, UPDATE ON postgres.movie_orders TO 'ecommerce'@'localhost';


--Роль "Адміністратор": 
GRANT ALL PRIVILEGES ON postgres.* TO 'admin_role'@'localhost';
--Роль "Клієнт": 
GRANT SELECT, INSERT, UPDATE ON postgres.table_name TO 'moderator_role'@'localhost';
--Роль "Продавець": 
GRANT SELECT, INSERT, UPDATE, DELETE ON postgres.table_name TO 'seller_role'@'localhost';
--Роль "Менеджер з доставки": 
GRANT SELECT, UPDATE ON postgres.table_name TO 'role_name'@'localhost';


-- Призначення ролей "користувач" та "автор" користувачу з id=3
GRANT ecommerce TO martyyy;
GRANT moderator_role TO martin;

-- Відкликання привілею UPDATE у ролі "модератор"
REVOKE UPDATE ON services FROM moderator_role;

-- Перевірка привілеїв користувача user3
SELECT has_table_privilege('martin', 'services', 'SELECT'); -- повинен повернути true
SELECT has_table_privilege('moderator_role', 'services', 'INSERT'); -- повинен повернути true

-- Видалення ролі "автор"
DROP ROLE IF EXISTS author_role;
