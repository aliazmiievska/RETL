--
--

# Common libraries

--

### 1. mysql.connector

Принципи
робота через connection → cursor → execute → commit → close
бажано тримати pool, а не перестворювати конекшн кожен раз
обов’язково робити commit після INSERT/UPDATE/DELETE
використовується prepared statements через (%s) — це захист від SQL injection

Важливі методи
conn = mysql.connector.connect(...)
cursor = conn.cursor()
cursor.execute(query, params)  # params = tuple
cursor.fetchall()
conn.commit()

Особливості
cursor.execute() не повертає дані, fetchall() — повертає
dictcursor треба явно вказувати
погано працює з bulk insert → краще .executemany()

--

### 2. logging

Принципи
логування має рівні: DEBUG < INFO < WARNING < ERROR < CRITICAL (якщо відбувається щось з вищого, нижні ігноруються)
форматування через logging.basicConfig(...) - базові дані
краще логувати в файл, а не в консоль

Методи
logging.info()
logging.debug()
logging.warning()
logging.error(exc_info=True)

Особливості
exc_info=True логне stacktrace (тіпа де саме сталась помилка)
можна робити RotatingFileHandler (щоб файл не розростався безкінечно)
ETL без логів = смерть на проді

--

### 3. datetime

Принципи
має date, time, datetime, timedelta
now() і utcnow() — не те саме!
Python не зберігає timezone по дефолту

Методи
datetime.now()
datetime.utcnow()
datetime.strptime() - з рядка в час
datetime.strftime() - з часу в рядок

Особливості
.strftime() для форматування → "2024-03-18"
.strptime() для парсингу
timedelta(days=...) для різниць

--

### 4. pathlib.Path

Принципи
працює як обʼєкт, а не як строка
/ → join шляхів

Методи
p = Path("src") / "data"
p.exists() - перевіряє чи існує шлях
p.mkdir(parents=True, exist_ok=True) - створює шлях
p.read_text()
p.write_text()  

Особливості
cross-platform (Windows/Linux)
краще ніж os.path (читабельністю і кросплатформом)

--

### 5. yaml

Принципи
yaml = більш читабельний конфіг ніж json
може мати типи: строки, карти, списки

Методи
yaml.safe_load(open("config.yaml"))
yaml.safe_dump(data)

Особливості
safe_load → безпечний
load → може виконувати python-обʼєкти (не юзати з інтернетом)
yaml не любить таби — тільки пробіли

--
--

## Unique for extract.py

--

### 1. hashlib

Принципи
дає детерміновані хеші
хеш ≠ крипто; хеш → для унікальності/ідентифікації

Методи
hashlib.md5(data.encode()).hexdigest()

Особливості
md5 швидкий → для dedup
sha256 → міцніший, але довший
хешування tuple → спочатку треба конвертувати в строку

-- 

### 2. re

Принципи
regex - регулярний вираз

Методи
re.findall(pattern, text) → список всіх збігів
re.search(pattern, text) → перший збіг
re.match() → збіг, що тільки на початку рядка
re.sub(pattern, repl, text) → заміна збігів за шаблоном

групи: (…) доступні через .group(1) - група імен, група дат..

--

### 3. dateutil.parser

інтелектуальний варіант datetime.strptime
(автоматично розуміє формати)

Методи
date_parser.parse("3 days ago")
date_parser.parse("2024-05-11T12:44")

Особливості
може ламатися на неоднозначних датах (03/04/24 → US vs EU)
може обробляти слова (“yesterday”, “next month”)

--

### 4. relativedelta

Принципи
працює як timedelta, але по-людськи:
delta = relativedelta(d2, d1)
delta.years, delta.months, delta.days - а у звичайній тільки хвилини, дні

Особливості
Може додавати місяці (timedelta не може!)
now + relativedelta(months=+1)

--

### 5. Parsera

Тут загальна логіка для “parser-обʼєктів”:

Типові принципи
має метод parse()
приймає сирий HTML/JSON
повертає нормалізовані поля
може містити правила очищення

короче зручна штука для скрапінгу

--

### 6. os

Принципи
інтерфейс до OS

Методи
os.listdir()
os.getenv()
os.makedirs()
os.remove()

Особливості
для env змінних → os.getenv("DB_PASSWORD")
для створення директорій краще Path.mkdir()

--
--

## Unique for transform.py

--

### 1. rapidfuzz (fuzz)

Принципи
бібліотека для fuzzy string matching (неповного або “розмитого” співпадіння рядків)
швидша альтернативна бібліотека для fuzzywuzzy (на C++/Python)
основна ідея: обчислити схожість рядків у відсотках
працює на алгоритмах Levenshtein distance, але оптимізована під швидкість

Важливі методи
from rapidfuzz import fuzz
fuzz.ratio("apple", "aple")         # відсоток схожості
fuzz.partial_ratio("apple", "green apple")  # часткове співпадіння
fuzz.token_sort_ratio("apple pie", "pie apple")  # ігнорує порядок слів
fuzz.token_set_ratio("apple pie", "apple pie recipe")  # враховує часткові набори слів

Особливості
швидка, на великі датасети → працює оптимально
робить string deduplication, fuzzy join, matching
можна комбінувати з pandas для роботи з колонками
результат завжди 0–100 (%)
не робить “корекцію” рядка, лише оцінює схожість

--
--

## Unique for load.py

--

### 1. sys

sys — це як пульт управління для твого Python-скрипта


Аргументи командного рядка (sys.argv)
уяви, що ти запускаєш скрипт і можеш йому передати “папірець з інструкцією”:
python script.py hello world

тоді в Python:
import sys
print(sys.argv)
    тіпа ['script.py', 'hello', 'world']

перший елемент — це завжди ім’я скрипта
інші елементи — інструкції, які ти передав


Версія Python (sys.version)
можна швидко глянути, яка у тебе “машина” під капотом
print(sys.version)  # наприклад "3.11.2"


Завершити програму (sys.exit())
хочеш сказати скрипту: “стоп, все, кінець”

можеш вказати код завершення:
sys.exit(0)   # все ок
sys.exit(1)   # була помилка


Шляхи до модулів (sys.path)

Python шукає модулі, де підказує sys.path
уяви як список полиць у гаражі, де він дивиться, чи є там потрібна “банка” з кодом


Стандартні потоки (sys.stdin/stdout/stderr) 
але вони нахуй нікому в ооп не всрались