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
####  Запусти DAG 

Перейди во вкладку DAGs и нажми кнопку "run DAG"

