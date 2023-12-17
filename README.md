# Лабораторная работа №3
## Петрова Мария 22ПИ2
### Ссылка на репозиторий: 
- https://github.com/MINTCanella/BenchmarkSQL.git
## Замеры и графики
### - Результаты измерения времени для файла 209МБ
![Таблица со временем tiny файла](https://github.com/MINTCanella/BenchmarkSQL/blob/main/graphs/tinycsv.png)
![График к таблице tiny файла](https://github.com/MINTCanella/BenchmarkSQL/blob/main/graphs/tinycsvgraph.png)
### - Результаты измерения времени для файла 2ГБ
![Таблица со временем big файла](https://github.com/MINTCanella/BenchmarkSQL/blob/main/graphs/bigcsv.png)
![График к таблице big файла](https://github.com/MINTCanella/BenchmarkSQL/blob/main/graphs/bigcsvgraph.png)
## Впечатления и выводы 
### 1. Psycopg2
Требует подключения к Постгресу, что может быть не очень удобным, а главное, из-за этого снижается производительность. 
Скорость выполнения запросов по сравнению с остальными четырьмя библиотеками довольно низкая и было тяжело загружать файл, но следует отметить, что используется чистый SQL, что может быть как плюсом, так и минусом. 
### 2. SQLite
На маленьком файле показала скорость ниже, чем у psycopg2, но на большом заняла твердую середину. 
При том из-за использоваия другой СУБД некоторые функции имели отличия от функций Постгреса, что важно было учитывать в работе, но на общие впечатления это почти не повлияло. 
### 3. DuckDB
Очень выделяется среди других скоростью выполнения запросов - она невероятно большая. Это достигается путем векторизации запросов. При том довольно легко грузить файлы и работа идет через SQL запросы.
Самая приятная библиотека не только для многократных тестов, но и в целом для работы в общем. 
### 4. Pandas
При помощи этой библиотеки легко работать с csv файлами и после DuckDB у нее самая высокая скорость. 
Запросы выполняются в python манере и ради них может быть необходимо изучить дополнительную информацию, и в целом они получаются более сложночитаемыми, чем все остальные, но, скорее всего, это дело привычки.
### 5. SQLAlchemy 
По скорости выполнения запросов держится примерно наравне с psycopg2, потому что так же требует подключение к внешнему процессору, что очень замедляет библиотеку.
Примечательно то, что библиотека совместима со многими СУБД и ее код универсален для них, хотя это не имело значения в этой лабораторной работе. 
