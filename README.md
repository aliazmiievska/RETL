
!!! добавити в секрет апі ключі перед комітом


# RETL - Review ETL Pipeline for Sanvita Products

Автоматизований ETL pipeline для збору та аналізу відгуків на продукцію Санвіта з різних інтернет-магазинів.

🎯 Ключові особливості:
✅ LLM аналіз - Claude API для sentiment, важливості та категорій
✅ Дедуплікація - MD5 hash для відгуків, rapidfuzz + LLM для продуктів
✅ Нормалізація дат - українські дати ("06 серпня 2022") → YYYY-MM-DD
✅ Cleanup - автоматичне видалення при помилках
✅ Логування - детальні логи у файли
✅ Cron ready - готовий до запуску щосуботи о 8:00
✅ Power BI ready - production MySQL для підключення


## 📁 Структура проекту

```
retl/
├── src/
│   ├── extract.py      # RAW extraction stage
│   ├── transform.py    # CORE transformation stage
│   └── load.py         # Production load stage
├── data/               # Автоматично створюється
├── logs/               # Автоматично створюється
├── config.yaml         # Configuration file
├── requirements.txt
├── run_retl.py         # Main pipeline runner
└── README.md
```

## 🚀 Встановлення

### 1. Клонувати проект

```bash
git clone <your-repo>
cd retl
```

### 2. Створити віртуальне середовище

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# або
venv\Scripts\activate     # Windows
```

### 3. Встановити залежності

```bash
python -m pip install -r requirements.txt
python -m playwright install

```

### 4. Налаштувати MySQL

Створити базу даних:

```mysql
CREATE DATABASE retl_database CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE retl_production CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

+ таблиця категорії
+ таблиця бренди
хоча вони є в конфізі..
```

### 5. Налаштувати конфігурацію

Відредагувати `config.yaml`:

```yaml
mysql:
  host: "localhost"
  user: "your_username"
  password: "your_password"
  database: "retl_database"

mysql_target:
  host: "your_production_server"
  user: "your_username"
  password: "your_password"
  database: "retl_production"

anthropic:
  api_key: "your_anthropic_api_key"
```

**Отримати Anthropic API key:** https://console.anthropic.com/

### 6. Налаштувати Parsera

Parsera використовує безкоштовний LLM API для парсингу.

Для роботи з Claude (рекомендовано) встановіть змінну середовища:

```bash
export ANTHROPIC_API_KEY="your_anthropic_api_key"
```

## 📊 Структура бази даних

### RAW Tables (data/raw.sql)

**Extracts** - історія всіх запусків
- `extract_id` - унікальний ID запуску
- `extract_fk_source` - джерело (makeup, epicentr, etc.)
- `extract_brand` - бренд (Санвіта)
- `extract_datetime` - час запуску
- `extract_status` - статус: pending/success/failed

**Product_RAW** - сирі дані про продукти
- `pr_id` - ID продукту
- `extract_fk_pr` - зв'язок з extract
- `pr_name` - назва продукту
- `pr_review_count` - кількість відгуків
- `pr_url_full` - повний URL

**Review_RAW** - сирі відгуки
- `rr_id` - ID відгуку
- `pr_fk_rr` - зв'язок з продуктом
- `rr_text` - текст відгуку
- `rr_date` - нормалізована дата (YYYY-MM-DD)
- `rr_hash` - MD5 хеш для дедуплікації

### CORE Tables (production)

**Product_CORE** - унікальні продукти
- `pc_id` - ID продукту
- `pc_desc` - опис
- `pc_brand` - бренд
- `pc_fk_category` - категорія (визначається LLM)

**Review_CORE** - унікальні відгуки з аналізом
- `rc_id` - ID відгуку
- `pc_fk_rc` - зв'язок з продуктом
- `rc_text` - текст
- `rc_source` - джерело
- `rc_date` - дата
- `rc_sentiment` - negative/neutral/positive (аналіз LLM)
- `rc_importance` - high/low (аналіз LLM)
- `rc_hash` - хеш для дедуплікації

## 🔄 Використання

### Ручний запуск

```bash
python run_retl.py
```

### Автоматичний запуск (cron)

Додати в crontab (щосуботи о 8:00):

```bash
crontab -e
```

Додати рядок:

```
0 8 * * 6 cd /path/to/retl && /path/to/venv/bin/python run_retl.py >> logs/cron.log 2>&1
```

### Запуск окремих стадій

**Тільки extraction:**

```python
from src.extract import Extractor

extractor = Extractor()
extractor.run_extraction(
    source_url="https://makeup.com.ua/ua/search/?q=санвіта",
    source_desc="makeup.com.ua",
    brand_name="санвіта",
    brand_desc="Санвіта",
    base_domain="https://makeup.com.ua"
)
```

**Тільки transformation:**

```python
from src.transform import Transformer

transformer = Transformer()
transformer.transform_all_successful_extracts()
```

**Тільки load:**

```python
from src.load import Loader

loader = Loader()
loader.run_load()
```

## 🎯 Як працює pipeline

### Stage 1: Extract (RAW)

1. Створює новий запис в `Extracts` (status: pending)
2. Parsera скрапить сторінку пошуку → отримує список продуктів
3. Фільтрація по ключових словах (серветки, санвіта) + виключення шуму (parfum, eau)
4. Зберігає валідні продукти в `Product_RAW`
5. Для кожного продукту скрапить сторінку → отримує відгуки
6. Нормалізує дати ("06 серпня 2022" → "2022-08-06")
7. Створює MD5 хеш для кожного відгуку (text + date)
8. Зберігає в `Review_RAW`
9. При успіху: status → success, при помилці: cleanup + status → failed

### Stage 2: Transform (CORE)

1. Отримує всі продукти з успішних extracts
2. Для кожного продукту:
   - Перевіряє схожість з існуючими (rapidfuzz + LLM)
   - Якщо новий → LLM визначає категорію → створює в `Product_CORE`
   - Якщо існує → використовує існуючий `pc_id`
3. Для кожного відгуку:
   - Перевіряє по hash (дедуплікація)
   - LLM аналізує sentiment (negative/neutral/positive)
   - LLM визначає importance (high/low)
   - Зберігає в `Review_CORE`

### Stage 3: Load (Production)

1. Підключається до production MySQL
2. Завантажує Categories, Sources
3. Завантажує Product_CORE (UPSERT)
4. Завантажує Review_CORE (INSERT IGNORE по hash)

## 🛠 Моніторинг

Логи зберігаються в `logs/`:

```bash
tail -f logs/retl_20250113_080000.log
```

Перевірка статусу останніх extracts:

```sql
SELECT 
    e.extract_id,
    s.source_desc,
    b.brand_desc,
    e.extract_datetime,
    e.extract_status,
    COUNT(DISTINCT pr.pr_id) as products_count,
    COUNT(rr.rr_id) as reviews_count
FROM Extracts e
LEFT JOIN Sources s ON e.extract_fk_source = s.source_id
LEFT JOIN Brands b ON e.extract_brand = b.brand_id
LEFT JOIN Product_RAW pr ON pr.extract_fk_pr = e.extract_id
LEFT JOIN Review_RAW rr ON rr.pr_fk_rr = pr.pr_id
GROUP BY e.extract_id
ORDER BY e.extract_datetime DESC
LIMIT 10;
```

## 🔧 Налаштування категорій

Додати нові категорії в `config.yaml`:

```yaml
categories:
  - "Серветки косметичні"
  - "Серветки сухі"
  - "Серветки вологі"
  - "Серветки універсальні"
  - "Серветки для дітей"
  - "Ваша нова категорія"
```

## 🌐 Додавання нових джерел

Додати в `config.yaml`:

```yaml
sources:
  - name: "rozetka.com.ua"
    url: "https://rozetka.com.ua/ua/search/?text=naturelle"
    domain: "https://rozetka.com.ua"
```

## ⚠️ Troubleshooting

**Помилка: "No products found"**
- Перевірте URL джерела
- Перевірте чи працює Parsera
- Можливо сайт змінив структуру

**Помилка: "Anthropic API error"**
- Перевірте API key
- Перевірте ліміти на акаунті

**Помилка: "MySQL connection error"**
- Перевірте credentials в config
- Перевірте чи запущений MySQL сервер

**Cleanup не спрацював**
- Перевірте foreign keys в БД
- Видалити вручну: спочатку Review_RAW, потім Product_RAW

## 📈 Power BI Integration

Після успішного load підключіть Power BI до production MySQL:

1. Get Data → MySQL database
2. Server: `your_production_server`
3. Database: `retl_production`
4. Import tables: `Product_CORE`, `Review_CORE`, `Categories`, `Sources`

Рекомендовані міри:

```dax
Total Reviews = COUNT(Review_CORE[rc_id])
Positive Reviews % = 
    DIVIDE(
        CALCULATE(COUNT(Review_CORE[rc_id]), Review_CORE[rc_sentiment] = "positive"),
        COUNT(Review_CORE[rc_id])
    )
```

## 📝 License

no

## 👤 Author

Alia Zmiievska