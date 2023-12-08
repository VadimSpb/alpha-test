# Загрузка прогноза погоды 

##  Выгрузка данных из API Яндекс.Погоды

### Задание
Используя API Яндекс.Погоды, необходимо выгрузить прогнозные данные за 7 дней для Москвы, Казани, Санкт-Петербурга, Тулы и Новосибирска. В случае, если API отдает пустые значения за день, то их необходимо удалить.
Информация должна быть представлена по часам с расширенным набором полей по осадкам.
Полученный json необходимо преобразовать в csv

**Примечание**

*Так как  получить токен от Яндекс.Погода мне как физлицу не получилось:*
![Снимок экрана 2023-12-07 в 13.49.00.png](images%2F%D0%A1%D0%BD%D0%B8%D0%BC%D0%BE%D0%BA%20%D1%8D%D0%BA%D1%80%D0%B0%D0%BD%D0%B0%202023-12-07%20%D0%B2%2013.49.00.png)
*я использовал сервис [www.openweathermap.org](www.openweathermap.org). Этот сервис предоставляет бесплатно предсказания погоды, в том числе почасовые предсказания на ближайшие 48 часов. Подробнее о сервисе [тут](https://openweathermap.org/api/one-call-3).*

### Решение.
#### Загрузи репозиторий

```sh
$ git clone https://github.com/VadimSpb/alpha-test.git
$ cd alpha-test
```

#### Запусти docker-контейнеров

```sh
$ docker compose up airflow-init
$ docker compose up
```
####  Настрой Airflow
1. Введи в строке поиска браузера адрес web-сервера airflow 
`http://localhost:8081/home`
2. Введи логин `airflow` и пароль `airflow`
3. Создай переменную openweathermap_access_token в Admin-> Variables (подробнее о получении токена [тут](https://home.openweathermap.org/api_keys))
4. Создай соединение postgres_local. Для использования postgres, установленного в группе контейнеров, введи:
```    
Connection Type : Postgres
Host            : postgres
Schema          : postgres
Login           : airflow
Password        : airflow
Port            : 5432
``` 
5. Cоздай переменную 'bronze_tier_path' в Admin-> Variables с путем до директории загрузки сырых данных
6. Cоздай переменную 'silver_tier_path' в Admin-> Variables с путем до директории хранения обработанных данных
7. Создай таблицу [weather_forecasts](#DDL-таблицы-слоя-источника) для загрузки сырых данных 
####  Запусти DAG 

Перейди во вкладку DAGs и нажми кнопку "run DAG"

#### Какие существуют возможные пути ускорения получения данных по API и их преобразования? 
Зависит от правил предоставления данных по api. Если есть возможность параллельных запросов - можно запустить сбор данных параллельно по каждому городу.

#### Возможно ли эти способы использовать в Airflow?
Да. Текущая реализация использует минимальные настройки. Можно увеличить число исполняемых параллельно задач в настройках DAG и настройках Airflow


### DDL таблицы слоя источника
```sql
CREATE TABLE public.weather_forecasts (
    city VARCHAR(255),
    date DATE,
    hour INT,
    temperature_c FLOAT,
    pressure_mm FLOAT,
    is_rainy INT
);
CREATE INDEX idx_weather_data_city ON public.weather_forecasts(city);
CREATE INDEX idx_weather_data_date ON public.weather_forecasts(date);
```
### DDL витрины начала дождя 
**Задача:** Используя таблицу с сырыми данными, необходимо собрать витрину, где для каждого города и дня будут указаны часы начала дождя. Условимся, что дождь может начаться только 1 раз за день в любом из городов.
```sql
CREATE VIEW rain_start_time AS
SELECT
    city,
    date,
    MIN(hour) as rain_start_hour
FROM weather_forecasts
WHERE is_rainy = 1
GROUP BY city, date;
```

### DDL скользящее среднее по температуре и по давлению
**Задача:** Необходимо создать витрину, где для каждого города, дня и часа будет рассчитано скользящее среднее по температуре и по давлению.
```sql
CREATE VIEW hourly_avg_weather AS
SELECT
    city,
    date,
    hour,
    AVG(temperature_c) 
      OVER (PARTITION BY city, date 
            ORDER BY hour 
            ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
            ) as avg_temperature_c,
    AVG(pressure_mm) 
      OVER (PARTITION BY city, date 
            ORDER BY hour 
            ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
            ) as avg_pressure_mm
FROM
    weather_forecasts;
```
*Примечание: Так как не указан период для скользящего, я выбрал среднесуточные показания*

