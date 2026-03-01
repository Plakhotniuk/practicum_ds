#!/bin/bash

# Выбор имени файла, в который будет производиться запись
log_file="log.txt"

# Тело функции
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$log_file"
} 

if [ ! -f customers_processed.csv ]; then
    echo "Файл customers_processed.csv не найден."
    exit 1
fi

if [ $# -eq 0 ]; then
    echo "Укажите хотя бы один фильтр, например: ./script_customers.sh Москва"
    exit 1
fi


for filter in "$@"; do
    echo "Фильтр: $filter"
    count=$(grep -c "$filter" customers_processed.csv)
    if [ "$count" -gt 0 ]; then
        grep "$filter" customers_processed.csv > "filtered_${filter}.csv"
        echo "Найдено $count строк.  Сохранено в файл filtered_${filter}.csv"
    else
        echo "Совпадений не найдено. Пропущено."
    fi
done


