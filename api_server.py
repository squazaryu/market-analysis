#!/usr/bin/env python3
"""
Простой API сервер для дашборда БПИФ
Только JSON API, без HTML рендеринга
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import numpy as np
import json
from datetime import datetime
from pathlib import Path

app = Flask(__name__)
CORS(app)  # Разрешаем запросы с других доменов

# Функция для конвертации numpy/pandas типов в JSON-совместимые
def convert_to_json_serializable(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.Series):
        return obj.tolist()
    elif isinstance(obj, pd.DataFrame):
        return obj.to_dict('records')
    elif isinstance(obj, dict):
        return {key: convert_to_json_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_json_serializable(item) for item in obj]
    elif pd.isna(obj):
        return None
    else:
        return obj

# Глобальные данные
etf_data = None

def load_latest_data():
    """Загружает последние данные ETF"""
    global etf_data
    
    try:
        # Ищем последний файл с данными
        data_files = list(Path('.').glob('enhanced_etf_data_*.csv'))
        if not data_files:
            return False
        
        latest_file = max(data_files, key=lambda x: x.stat().st_mtime)
        etf_data = pd.read_csv(latest_file)
        
        # Добавляем базовые метрики если их нет
        if 'sharpe_ratio' not in etf_data.columns:
            risk_free_rate = 15.0
            etf_data['sharpe_ratio'] = (etf_data['annual_return'] - risk_free_rate) / etf_data['volatility']
        
        print(f"✅ Загружено {len(etf_data)} ETF из {latest_file}")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка загрузки данных: {e}")
        return False

@app.route('/api/stats')
def api_stats():
    """API получения статистики"""
    if etf_data is None or len(etf_data) == 0:
        return jsonify({'error': 'Данные не загружены'}), 404
    
    try:
        stats = {
            'total_etfs': len(etf_data),
            'total_nav': float(etf_data['nav_billions'].sum()),
            'avg_return': float(etf_data['annual_return'].mean()),
            'avg_volatility': float(etf_data['volatility'].mean()),
            'top_return': float(etf_data['annual_return'].max()),
            'lowest_return': float(etf_data['annual_return'].min()),
            'last_updated': datetime.now().strftime('%d.%m.%Y, %H:%M:%S')
        }
        
        return jsonify(convert_to_json_serializable(stats))
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/table')
def api_table():
    """API получения данных таблицы"""
    if etf_data is None or len(etf_data) == 0:
        return jsonify({'error': 'Данные не загружены'}), 404
    
    try:
        limit = int(request.args.get('limit', 20))
        sort_by = request.args.get('sort_by', 'nav_billions')
        sort_order = request.args.get('sort_order', 'desc')
        
        # Сортировка
        ascending = sort_order.lower() == 'asc'
        df_sorted = etf_data.sort_values(sort_by, ascending=ascending)
        
        # Лимит
        df_limited = df_sorted.head(limit) if limit > 0 else df_sorted
        
        result = {
            'data': convert_to_json_serializable(df_limited.to_dict('records')),
            'total': len(etf_data),
            'showing': len(df_limited),
            'sort_by': sort_by,
            'sort_order': sort_order
        }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/chart')
def api_chart():
    """API получения данных для графика"""
    if etf_data is None or len(etf_data) == 0:
        return jsonify({'error': 'Данные не загружены'}), 404
    
    try:
        # Создаем данные для scatter plot
        scatter_data = {
            'x': convert_to_json_serializable(etf_data['volatility']),
            'y': convert_to_json_serializable(etf_data['annual_return']),
            'text': convert_to_json_serializable(etf_data['ticker']),
            'mode': 'markers',
            'type': 'scatter',
            'marker': {
                'size': 8,
                'color': convert_to_json_serializable(etf_data['nav_billions']),
                'colorscale': 'Viridis',
                'showscale': True,
                'colorbar': {'title': 'СЧА (млрд ₽)'}
            },
            'name': 'БПИФ'
        }
        
        layout = {
            'title': 'Соотношение риск-доходность БПИФ',
            'xaxis': {'title': 'Волатильность (%)'},
            'yaxis': {'title': 'Годовая доходность (%)'},
            'hovertemplate': '<b>%{text}</b><br>Доходность: %{y:.1f}%<br>Волатильность: %{x:.1f}%<extra></extra>'
        }
        
        return jsonify({
            'data': [scatter_data],
            'layout': layout
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/force-refresh')
def force_refresh():
    """Принудительное обновление данных"""
    try:
        # Перезагружаем данные
        success = load_latest_data()
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Данные успешно обновлены',
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_funds': len(etf_data) if etf_data is not None else 0
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Не удалось обновить данные'
            }), 500
            
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Ошибка обновления: {str(e)}'
        }), 500

@app.route('/api/archive-summary')
def archive_summary():
    """Сводка архивных данных"""
    try:
        from investfunds_parser import InvestFundsParser
        
        parser = InvestFundsParser()
        summary = parser.get_fund_history_summary()
        
        return jsonify({
            'status': 'success',
            'archive_summary': summary,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Ошибка получения сводки архива: {str(e)}'
        }), 500

@app.route('/api/scheduler/start')
def start_scheduler():
    """Запуск планировщика"""
    try:
        from data_scheduler import DataScheduler
        
        # Создаем и запускаем планировщик
        scheduler = DataScheduler()
        scheduler.setup_schedule()
        scheduler.start_background()
        
        return jsonify({
            'status': 'success',
            'message': 'Планировщик запущен',
            'schedule': {
                'investfunds': 'Ежедневно в 10:00',
                'moex': 'Еженедельно в пятницу 09:00'
            }
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Ошибка запуска планировщика: {str(e)}'
        }), 500

@app.route('/')
def health_check():
    """Проверка работоспособности API"""
    return jsonify({
        'status': 'ok',
        'message': 'API сервер работает',
        'endpoints': [
            '/api/stats',
            '/api/table',
            '/api/chart',
            '/api/force-refresh',
            '/api/archive-summary',
            '/api/scheduler/start'
        ],
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

if __name__ == '__main__':
    print("🚀 Запуск API сервера для дашборда БПИФ...")
    
    # Загружаем данные при старте
    if load_latest_data():
        print("✅ Данные загружены успешно")
    else:
        print("⚠️ Данные не загружены, API будет возвращать ошибки")
    
    print("🌐 API доступен по адресу: http://localhost:5005")
    print("📄 HTML дашборд: standalone_dashboard.html")
    print("⏹️ Для остановки нажмите Ctrl+C")
    
    app.run(debug=True, host='0.0.0.0', port=5005)