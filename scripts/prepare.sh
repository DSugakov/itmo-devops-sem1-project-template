#!/usr/bin/env bash
set -e

echo ">>> Установка Go-зависимостей..."
go mod tidy

echo ">>> Ожидание доступности PostgreSQL..."
until pg_isready -h localhost -p 5432 -U validator; do
  sleep 1
done
echo "PostgreSQL доступен."

echo ">>> Удаление старой таблицы (если требуется)..."
psql "host=localhost port=5432 dbname=project-sem-1 user=validator password=val1dat0r" -c "DROP TABLE IF EXISTS prices;"

echo ">>> Создание новой таблицы..."
psql "host=localhost port=5432 dbname=project-sem-1 user=validator password=val1dat0r" <<EOF
CREATE TABLE prices (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    price NUMERIC NOT NULL,
    create_date DATE NOT NULL
);
EOF

echo ">>> Скрипт подготовки успешно выполнен!"