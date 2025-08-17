# 🔧 Диагностика и исправление проблем дашборда

## 🚨 Проблемы и решения

### 1. **Кнопки не работают**

#### Возможные причины:
- JavaScript ошибки в консоли браузера
- Неправильные onclick обработчики
- Конфликты после автоисправления кода

#### Решение:
```bash
# Запустите минимальный тестовый дашборд
python3 minimal_dashboard.py
```

Откройте http://localhost:5002 и протестируйте все кнопки.

### 2. **Ошибка загрузки ETF**

#### Проверьте наличие данных:
```bash
ls -la enhanced_etf_data_*.csv full_moex_etf_data_*.csv
```

#### Если файлов нет, создайте их:
```bash
python3 run_advanced_etf_analytics.py
```

#### Проверьте загрузку данных:
```bash
python3 -c "
from web_dashboard import dashboard
print(f'Данные загружены: {dashboard.enhanced_df is not None}')
if dashboard.enhanced_df is not None:
    print(f'Количество ETF: {len(dashboard.enhanced_df)}')
"
```

### 3. **JavaScript ошибки**

#### Откройте консоль браузера (F12) и проверьте:
- Нет ли ошибок в консоли
- Загружаются ли все скрипты (Bootstrap, Plotly, FontAwesome)
- Выполняются ли AJAX запросы к API

#### Типичные ошибки:
```javascript
// ❌ Неправильно
function myFunction() {
    event.target.classList.add('active'); // event не определен
}

// ✅ Правильно  
function myFunction(element) {
    element.classList.add('active');
}
```

## 🧪 Тестовые дашборды

### Минимальный тест (порт 5002)
```bash
python3 minimal_dashboard.py
```
- ✅ Простые кнопки
- ✅ Переключение состояний
- ✅ API запросы
- ✅ Загрузка данных

### Простой дашборд (порт 5001)
```bash
python3 simple_dashboard.py
```
- ✅ Статистика ETF
- ✅ График риск-доходность
- ✅ Таблица с поиском
- ✅ Уведомления

### Полный дашборд (порт 5000)
```bash
python3 start_web_dashboard.py
```
- ✅ Все функции
- ✅ Интерактивные графики
- ✅ Фильтрация и сортировка
- ✅ Экспорт данных

## 🔍 Пошаговая диагностика

### Шаг 1: Проверка данных
```bash
# Проверяем файлы данных
ls -la *etf_data*.csv | head -3

# Проверяем загрузку в Python
python3 -c "
import pandas as pd
from pathlib import Path
files = list(Path('.').glob('*etf_data*.csv'))
if files:
    df = pd.read_csv(max(files, key=lambda x: x.stat().st_mtime))
    print(f'✅ Данные: {len(df)} записей')
else:
    print('❌ Файлы данных не найдены')
"
```

### Шаг 2: Тест минимального дашборда
```bash
python3 minimal_dashboard.py
```
Откройте http://localhost:5002 и проверьте:
- [ ] Кнопки "Тест 1, 2, 3" работают
- [ ] Переключение режимов работает
- [ ] API загружает статистику
- [ ] Таблица отображается

### Шаг 3: Проверка браузера
1. Откройте F12 (Developer Tools)
2. Перейдите на вкладку Console
3. Обновите страницу
4. Проверьте наличие ошибок

### Шаг 4: Тест API endpoints
```bash
# Тест в отдельном терминале
curl http://localhost:5002/api/stats
curl http://localhost:5002/api/table
```

## 🛠️ Быстрые исправления

### Исправление onclick обработчиков
```html
<!-- ❌ Не работает -->
<button onclick="myFunction('param')">Кнопка</button>

<!-- ✅ Работает -->
<button onclick="myFunction('param', this)">Кнопка</button>
```

### Исправление JavaScript функций
```javascript
// ❌ Не работает
function myFunction(param) {
    event.target.classList.add('active');
}

// ✅ Работает
function myFunction(param, element) {
    element.classList.add('active');
}
```

### Добавление обработки ошибок
```javascript
async function loadData() {
    try {
        const response = await fetch('/api/data');
        const data = await response.json();
        // Обработка данных
    } catch (error) {
        console.error('Ошибка:', error);
        showNotification('Ошибка загрузки данных', 'error');
    }
}
```

## 📋 Чек-лист проверки

### Перед запуском:
- [ ] Файлы данных ETF существуют
- [ ] Python зависимости установлены (Flask, pandas, plotly)
- [ ] Порт свободен (5000, 5001, или 5002)

### После запуска:
- [ ] Сервер запустился без ошибок
- [ ] Страница загружается в браузере
- [ ] Нет ошибок в консоли браузера
- [ ] API endpoints отвечают (проверить в Network tab)

### Тест функциональности:
- [ ] Кнопки реагируют на клики
- [ ] Переключение состояний работает
- [ ] Поиск в таблице функционирует
- [ ] Графики отображаются
- [ ] Уведомления появляются

## 🚀 Рекомендации

### Для разработки:
1. Начните с минимального дашборда
2. Постепенно добавляйте функции
3. Тестируйте каждое изменение
4. Используйте консоль браузера для отладки

### Для продакшена:
1. Отключите debug режим Flask
2. Используйте production WSGI сервер
3. Добавьте логирование ошибок
4. Настройте мониторинг

## 📞 Если ничего не помогает

1. **Перезапустите все:**
   ```bash
   # Остановите все Python процессы
   pkill -f python3
   
   # Запустите минимальный тест
   python3 minimal_dashboard.py
   ```

2. **Проверьте системные требования:**
   - Python 3.7+
   - Flask, pandas, plotly установлены
   - Современный браузер (Chrome, Firefox, Safari)

3. **Создайте новый виртуальный environment:**
   ```bash
   python3 -m venv test_env
   source test_env/bin/activate
   pip install flask pandas plotly
   python3 minimal_dashboard.py
   ```

---

**Помните: минимальный дашборд должен работать всегда! 🧪✅**