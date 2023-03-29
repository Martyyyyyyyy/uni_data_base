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
