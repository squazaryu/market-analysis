# ⚡ Быстрый запуск дашборда

## Для нового ПК (менее 2 минут)

### 1️⃣ Клонируйте репозиторий
```bash
git clone <your-repo-url>
cd market-analysis
```

### 2️⃣ Установите Python зависимости
```bash
pip install flask pandas numpy plotly requests
```
*или*
```bash
pip install -r requirements.txt
```

### 3️⃣ Запустите автоматическую настройку
```bash
python3 setup_dashboard.py
```

### 4️⃣ Запустите дашборд
```bash
python3 simple_dashboard.py
```

### 5️⃣ Откройте в браузере
```
http://localhost:5004
```

---

## ✅ Что должно получиться

После выполнения команд вы увидите:

```
🚀 АВТОМАТИЧЕСКАЯ НАСТРОЙКА ДАШБОРДА
============================================================
🔍 Проверка зависимостей...
✅ flask установлен
✅ pandas установлен
✅ numpy установлен
✅ plotly установлен
✅ requests установлен

📁 Создание директорий...
✅ Создана директория investfunds_cache
✅ Создана директория cache
✅ Создана директория logs

📊 Создание минимальных файлов данных...
✅ Создан enhanced_etf_data_20250827_105019.csv
✅ Создан simplified_bpif_structure_20250827_105516.csv
✅ Создан simplified_level1_summary_20250827_105516.csv
✅ Создан simplified_level2_summary_20250827_105516.csv
✅ Создан simplified_geography_summary_20250827_105516.csv
✅ Создан real_temporal_analysis.json

🎉 НАСТРОЙКА ЗАВЕРШЕНА!
```

---

## ❗ Если что-то пошло не так

### Проблема: "ModuleNotFoundError"
**Решение:**
```bash
pip install flask pandas numpy plotly requests beautifulsoup4
```

### Проблема: "Permission denied"
**Решение для macOS/Linux:**
```bash
chmod +x setup_dashboard.py
python3 setup_dashboard.py
```

### Проблема: "Файлы данных не найдены"
**Решение:**
```bash
python3 setup_dashboard.py  # Пересоздать файлы
```

### Проблема: "Порт 5004 занят"
**Решение:** Измените порт в `simple_dashboard.py`:
```python
app.run(host='0.0.0.0', port=5005, debug=True)  # Любой свободный порт
```

---

## 📋 Что создается автоматически

### Файлы данных:
- ✅ `enhanced_etf_data_20250827_105019.csv` (основные данные ETF)
- ✅ `simplified_bpif_structure_*.csv` (структура классификации)
- ✅ `real_temporal_analysis.json` (данные временного анализа)

### Директории:
- ✅ `investfunds_cache/` (кэш данных)
- ✅ `cache/` (временные файлы)  
- ✅ `logs/` (логи системы)

---

## 🔄 Обновление данных

Для получения актуальных данных с MOEX:
```bash
python3 create_real_temporal_analysis.py
```

---

## 🎯 Что делает дашборд

- 📊 **Анализ доходности** ETF фондов
- 📈 **Временные графики** по периодам
- 🏆 **Рейтинги фондов** по различным метрикам  
- 🔍 **Детальная аналитика** по каждому фонду
- 📋 **Интерактивные таблицы** с сортировкой
- 🎨 **Красивые графики** Plotly

---

## ⚙️ Системные требования

- Python 3.8+
- 2 ГБ RAM
- 500 МБ свободного места
- Интернет-соединение (для обновления данных)