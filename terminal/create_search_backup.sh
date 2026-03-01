   #!/bin/bash
   
   # Создание нового файла
   touch customers_script.csv
   
   # Запись заголовков и данных
   echo "Name,Age,City" > customers_script.csv
   echo "Станислав,31,Москва" >> customers_script.csv
   echo "Полина,28,Москва" >> customers_script.csv
   echo "Кирилл,40,Санкт-Петербург" >> customers_script.csv
   echo "Максим,21,Казань" >> customers_script.csv
   
   # Просмотр содержимого
   cat customers_script.csv
   
   # Поиск строк с «Москва»
   grep "Москва" customers_script.csv
   
   # Подсчёт символов
   wc -m customers_script.csv
   
   # Создание резервной копии
   cp customers_script.csv backup_customers_script.csv
   
   # Добавление нового клиента
   echo "Георгий,30,Москва" >> customers_script.csv
   
   # Переименование старой копии
   mv backup_customers_script.csv archived_customers_script.csv
   
   # Удаление устаревшего файла
   rm archived_customers_script.csv


