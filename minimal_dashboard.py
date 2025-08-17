#!/usr/bin/env python3
"""
Минимальный рабочий дашборд для тестирования кнопок
"""

from flask import Flask, render_template_string, jsonify
import pandas as pd
from pathlib import Path

app = Flask(__name__)

# Загружаем данные при старте
def get_etf_data():
    try:
        data_files = list(Path('.').glob('enhanced_etf_data_*.csv'))
        if not data_files:
            data_files = list(Path('.').glob('full_moex_etf_data_*.csv'))
        
        if data_files:
            latest_data = max(data_files, key=lambda x: x.stat().st_mtime)
            df = pd.read_csv(latest_data)
            
            # Добавляем sharpe_ratio если его нет
            if 'sharpe_ratio' not in df.columns:
                df['sharpe_ratio'] = (df['annual_return'] - 15) / df['volatility']
            
            return df
        return None
    except:
        return None

etf_df = get_etf_data()

HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>🧪 Тест кнопок ETF дашборда</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%); padding: 20px; }
        .test-card { background: white; border-radius: 15px; padding: 20px; margin: 20px 0; box-shadow: 0 4px 15px rgba(0,0,0,0.1); }
        .btn-test { margin: 5px; }
        .status { padding: 10px; margin: 10px 0; border-radius: 5px; }
        .status-success { background: #d4edda; color: #155724; }
        .status-error { background: #f8d7da; color: #721c24; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🧪 Тест функций ETF дашборда</h1>
        
        <div class="test-card">
            <h3>1. Тест базовых кнопок</h3>
            <button class="btn btn-primary btn-test" onclick="testBasic('Кнопка 1')">Тест 1</button>
            <button class="btn btn-success btn-test" onclick="testBasic('Кнопка 2')">Тест 2</button>
            <button class="btn btn-warning btn-test" onclick="testBasic('Кнопка 3')">Тест 3</button>
            <div id="basic-status" class="status">Нажмите любую кнопку</div>
        </div>

        <div class="test-card">
            <h3>2. Тест переключения состояний</h3>
            <div class="btn-group" role="group">
                <button class="btn btn-outline-primary active" onclick="toggleButton(this, 'Режим 1')">Режим 1</button>
                <button class="btn btn-outline-primary" onclick="toggleButton(this, 'Режим 2')">Режим 2</button>
                <button class="btn btn-outline-primary" onclick="toggleButton(this, 'Режим 3')">Режим 3</button>
            </div>
            <div id="toggle-status" class="status">Выберите режим</div>
        </div>

        <div class="test-card">
            <h3>3. Тест API запросов</h3>
            <button class="btn btn-info btn-test" onclick="testAPI()">Загрузить статистику</button>
            <button class="btn btn-secondary btn-test" onclick="testTable()">Загрузить таблицу</button>
            <div id="api-status" class="status">Нажмите кнопку для теста API</div>
        </div>

        <div class="test-card">
            <h3>4. Результаты тестов</h3>
            <div id="results">
                <p>Статистика ETF: <span id="etf-count">Загрузка...</span></p>
                <div id="etf-table"></div>
            </div>
        </div>
    </div>

    <script>
        // Тест базовых кнопок
        function testBasic(buttonName) {
            const status = document.getElementById('basic-status');
            status.innerHTML = `✅ ${buttonName} работает! Время: ${new Date().toLocaleTimeString()}`;
            status.className = 'status status-success';
            console.log(`Кнопка ${buttonName} нажата`);
        }

        // Тест переключения
        function toggleButton(element, mode) {
            // Убираем active у всех кнопок в группе
            const buttons = element.parentElement.querySelectorAll('.btn');
            buttons.forEach(btn => btn.classList.remove('active'));
            
            // Добавляем active к нажатой кнопке
            element.classList.add('active');
            
            const status = document.getElementById('toggle-status');
            status.innerHTML = `🔄 Активен: ${mode}`;
            status.className = 'status status-success';
            console.log(`Переключено на ${mode}`);
        }

        // Тест API
        async function testAPI() {
            const status = document.getElementById('api-status');
            status.innerHTML = '⏳ Загрузка статистики...';
            status.className = 'status';
            
            try {
                const response = await fetch('/api/stats');
                const data = await response.json();
                
                status.innerHTML = `✅ API работает! Загружено ${data.total} ETF`;
                status.className = 'status status-success';
                
                document.getElementById('etf-count').textContent = `${data.total} ETF найдено`;
            } catch (error) {
                status.innerHTML = `❌ Ошибка API: ${error.message}`;
                status.className = 'status status-error';
            }
        }

        // Тест таблицы
        async function testTable() {
            const status = document.getElementById('api-status');
            status.innerHTML = '⏳ Загрузка таблицы...';
            
            try {
                const response = await fetch('/api/table');
                const data = await response.json();
                
                let tableHTML = '<table class="table table-sm"><thead><tr><th>Тикер</th><th>Доходность</th><th>Риск</th></tr></thead><tbody>';
                
                data.slice(0, 5).forEach(etf => {
                    tableHTML += `<tr>
                        <td><strong>${etf.ticker}</strong></td>
                        <td>${etf.annual_return.toFixed(1)}%</td>
                        <td>${etf.volatility.toFixed(1)}%</td>
                    </tr>`;
                });
                
                tableHTML += '</tbody></table>';
                document.getElementById('etf-table').innerHTML = tableHTML;
                
                status.innerHTML = `✅ Таблица загружена! Показано ${Math.min(5, data.length)} записей`;
                status.className = 'status status-success';
            } catch (error) {
                status.innerHTML = `❌ Ошибка загрузки таблицы: ${error.message}`;
                status.className = 'status status-error';
            }
        }

        // Автозапуск при загрузке
        document.addEventListener('DOMContentLoaded', function() {
            console.log('🚀 Тестовый дашборд загружен');
            testAPI(); // Автоматически загружаем статистику
        });
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML)

@app.route('/api/stats')
def api_stats():
    if etf_df is None:
        return jsonify({'error': 'Данные не найдены', 'total': 0})
    
    return jsonify({
        'total': len(etf_df),
        'avg_return': round(etf_df['annual_return'].mean(), 1),
        'avg_volatility': round(etf_df['volatility'].mean(), 1)
    })

@app.route('/api/table')
def api_table():
    if etf_df is None:
        return jsonify([])
    
    # Возвращаем топ-10 ETF
    top_etfs = etf_df.nlargest(10, 'avg_daily_volume')
    
    result = []
    for _, row in top_etfs.iterrows():
        result.append({
            'ticker': row['ticker'],
            'annual_return': float(row['annual_return']),
            'volatility': float(row['volatility']),
            'sharpe_ratio': float(row.get('sharpe_ratio', 0))
        })
    
    return jsonify(result)

if __name__ == '__main__':
    print("🧪 Запуск минимального тестового дашборда...")
    
    if etf_df is not None:
        print(f"✅ Загружено {len(etf_df)} ETF")
    else:
        print("⚠️  Данные ETF не найдены, но дашборд запустится для тестирования")
    
    print("🌐 Откройте: http://localhost:5002")
    print("⏹️  Остановка: Ctrl+C")
    
    app.run(debug=True, host='0.0.0.0', port=5002)