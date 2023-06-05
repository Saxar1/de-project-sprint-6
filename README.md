# Спринт 6 #

* Скопируйте проект в директорию:
```shell script
git clone https://github.com/practicum-de/s6-lessons.git
```
* Перейдите в директорию c проектом:
```shell script
cd s6-lessons
```
* Создайте [виртуальное окружение](https://docs.python.org/3/library/venv.html) и активируйте его:
```shell script
python3 -m venv venv
```

* Активируйте его:
```shell script
source venv/bin/activate
```
или в Windowns
```shell script
source venv/Scripts/activate
```

* Обновите pip до последней версии:
```shell script
pip install --upgrade pip
```
* Установите зависимости:
```shell script
pip install -r requirements.txt
```
Запустите docker-compose:
`docker compose up -d`

Если у Вас не установлен python 3.8 то самое время сделать это. 

Airflow доступен по адресу http://localhost:3000/airflow
Metabse - http://localhost:3333/

Если в Metabase на шаге выбора БД отсутствует опция Vertica, проверьте логи на вкладке "Разрешение проблем". Скорее всего вы найдете сообщение "java.lang.AssertionError: Assert failed: Metabase does not have permissions to write to plugins directory /plugins". В таком случае в папке с репозиторием (в которой должна находиться папка plugins) выполните команду `chmod -R 777 plugins` после чего перезапустите контейнер с Metabase - `docker restart s6-lessons_metabase_1`.

В DBeaver рекомендуется использовать драйвер 11.1, если подключение не устанавливается то 12.0.4.

Перед запуском тестов убедитесь что запущен контейнер.
