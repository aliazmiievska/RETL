**retl/** (помилки логувати)
-
-
-
-
**├── src/**
-
-
**│   ├──  extract.py {**
-
код створює raw.sql з таблицею Extract, елемент з extract_id:1.., source_id:1-end (після останнього сорсу закінчується), brand_id:1-end (після останнього бренду переходить на новий сорс), extract_datetime:current (коли вперше, далі просто створює нові елементи)

Extracts
	 extract_id INT
	 extract_fk_source -> [Sources: source_id, source_desc]
	 extract_brand -> [Brands: brand_id, brand_desc]
	 extract_datetime DATETIME
	 extract_status ENUM('pending', 'success', 'failed') {
		 source → brand → products → reviews → raw.sql write → success
		}

тоді заходить на вказаний сорс 
вводить в пошук вказаний бренд і виконує пошук. тоді з отриманої сторінки за допомогою парсери (на безкоштовній ллм апі) дістає назви всіх товарів, що містять хоча б один відгук і створює temporary.scv такого плану

| product_name                                          | product_url          | product_reviews_count |
| ----------------------------------------------------- | -------------------- | --------------------- |
| Jean Paul Gaultier Scandal Pour Homme - Туалетна вода | /ua/product/995244/  | 2                     |
| Angel Schlesser Femme Naturelle                       | /ua/product/1410453/ | 5                     |
(це приклад виконаного запиту на parsera.org)

тоді код відбирає scv за product_reviews_count >= 1

потім по ключових словах (визначених вручну) йде первірка, чи це товар бренду санвіта (тобто чи це серветки, бо там за пошуком можуть бути і парфуми і шо хоч) {
**Rule set:**
- must contain 1 keyword from brand dictionary
- must NOT contain слова-шум (як "parfum", "eau", "ml", "для жінок")
}
і створює таблицю в raw.sql

Product_RAW 
	pr_id INT - auto
	extract_fk_pr INT
	pr_name TEXT
	pr_review_count INT
	pr_first_seen DATETIME
	pr_url_full TEXT (добавити домен до /ua/product/1410453/, який скрапить парсера)

тоді по кожному продукту проходиться по юрл і збирає відгуки в temporary.scv (він перезаписується, бо сенс зберігати) типу

|review_text|review_date|
|---|---|
|Дуже подобатися такі серветки, вже не перший раз купую|06 серпня 2022|
|Хорошие салфетки, со своей функцией справляются, не сухие.|13 січня 2021|
(теж приклад запиту на parsera.org)
і тоді створює таблицю

Review_RAW
	rr_id INT - autoincrement
	pr_fk_rr INT
	rr_text TEXT
	rr_date_normalized DATE (нормалізувати) {
		в makeup/epicenter/etc дати прилітають локалізовані:
		`"06 серпня 2022" "13 січня 2021" "2 дня назад" "Вчера" "Today"`
		Тобі треба:
		1. привести до `DATE`
		2. час не треба (в відгуках його нема)
		Універсальний формат → `YYYY-MM-DD`
		}
	rr_hash TEXT UNIQUE (створити хеш) {
		rr_text = "Дуже подоба..."
		rr_date = "2022-08-06"
		зробити одну строку:
		"Дуже подоба...|2022-08-06"
		прогнати через хеш-функцію (MD5) → отримуєм фіксовану строку (типу 32 символи), яка унікальна для такого комбо.

якщо зайти не вдалось, або будь-яка інша помилка, через яку не стався success, то rollback -> cleanup -> статус failed
cleanup має видаляти тільки raw таблиці **по цьому extract_id**, він не чіпає Brands/Sources, Core, Review_Core і т.д. 
спочатку Reviews, потім Products, бо там foreign keys
наступний екстракт елемент -> наступне айді, повторів немає

RAW stage completed → extract_status = success
CORE stage starts
-
**}**
-
-
**│   ├── transform.py {**
-
в нас є екстракт айді1. в ньому, допустим, 25 продуктів_ров, в них по 2 рев'ю_ров.

треба створити продукт_кор айді1, тобто

Product_CORE (кожен продукт унікальний)
	pc_id INT - autoincrement
	pc_desc TEXT
	pc_brand
	pc_fk_category INT -> [Categories: category_id, category_name]

бо його ше поки нема, не було екстрактів

значить дескріпшн код бере з pr_name
бренд бере з extract_brand
категорію підбирає ллм по назві з набору категорій (створюється вручну)

якщо це екстракт айді2, то є ризик, що один з товарів вже буде в корі. назву нового товару спочатку aepps має звірити з кожною назвою в корі і сказати "однаковий"/"неоднаковий". і якщо всі неоднакові, тоді добавляється новий товар в кор зі своєю назвою.

from rapidfuzz import fuzz
score = fuzz.token_sort_ratio(a, b)
print(score)

а далі на ті 1-2, що лишаться "однаковий (або схожий 0.9+", ллм, щоб зменшити кількість токенів

тепер по "однаковий". якщо товар вже є в корі, тоді новий не створюється, а нові відгуки добавляються до його наявних відгуків
тепер по відгуках

Review_CORE
	rc_id INT - ауто
	pc_fk_rc INT
	rc_text TEXT - з review_raw
	rc_source - з extract_source
	rc_date DATE - з review_raw
	rc_sentiment ENUM("negative", "neutral", "positive") - це визначає ллм з тексту
	rc_importance ENUM("high", "low") - також визначає ллм з тексту відгуку
	rc_hash TEXT - з rr_hash
-
**}**
-
-
**│   └── load.py** - тут product_core i review_core завантажуються на mysql сервер, який я підключаю далі сама до павер біай
-
-
-
**├── config/**
-
-
**│   └── api_keys.yaml**
-
-
-
**├── data/** (при повторному запуску дані не перезаписуються)
-
-
**│   ├── raw.sql {** 
-
Extracts
	 extract_id INT - autoincrement
	 extract_fk_source -> [Sources: source_id, source_desc]
	 extract_brand -> [Brands: brand_id, brand_desc]
	 extract_datetime DATETIME
	 extract_status ENUM('pending', 'success', 'failed')
↓
Product_RAW 
	pr_id INT - auto
	extract_fk_pr INT
	pr_name TEXT
	pr_review_count INT
	pr_first_seen DATETIME
	pr_url_full TEXT (добавити домен до /ua/product/1410453/, який скрапить парсера)
↓
Review_RAW
	rr_id INT - autoincrement
	pr_fk_rr INT
	rr_text TEXT
	rr_date DATE
	rr_hash TEXT
-
**}**
-
-
**│   └── core.sql  {**
-
Product_CORE (кожен продукт унікальний)
	pc_id INT - autoincrement
	pc_desc TEXT
	pc_brand
	pc_fk_category INT -> [Categories: category_id, category_name]
↓
Review_CORE
	rc_id INT - autoincrement
	pc_fk_rc INT
	rc_text TEXT
	rc_source INT (інша таблиця)
	rc_date DATE
	rc_sentiment ENUM("negative", "neutral", "positive")
	rc_importance ENUM("high", "low")
	rc_hash TEXT
-
**}**
-
-
**└── run_retl.py**        ← запускає все разом через cron щосуботи о 8:00
-
-
-
-
end 