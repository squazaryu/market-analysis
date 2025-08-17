#!/usr/bin/env python3
"""
Быстрое исправление временных фильтров
"""

from flask import Flask, render_template_string, jsonify, request
import pandas as pd
import plotly.graph_objects as go
import json
from pathlib import Path
from historical_data_manager import HistoricalDataManager
from capital_flow_analyzer import CapitalFlowAnalyzer

app = Flask(__name__)

# Загружаем данные
data_files = list(Path('.').glob('enhanced_etf_data_*.csv'))
if not data_files:
    data_files = list(Path('.').glob('full_moex_etf_data_*.csv'))

if data_files:
    etf_data = pd.read_csv(max(data_files, key=lambda x: x.stat().st_mtime))
    print(f"✅ Загружено {len(etf_data)} ETF")
else:
    etf_data = None
    print("❌ Данные не найдены")

# Инициализируем менеджер
historical_manager = HistoricalDataManager()

@app.route('/api/time-periods')
def api_time_periods():
    """API для получения доступных временных периодов"""
    try:
        periods = historical_manager.get_available_periods()
        return jsonify({'periods': periods})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/capital-flows')
def api_capital_flows():
    """API анализа перетоков капитала с поддержкой временных фильтров"""
    if etf_data is None:
        return jsonify({'error': 'Данные не загружены'})
    
    try:
        # Получаем параметр периода
        period_index = int(request.args.get('period', 0))
        
        # Загружаем данные для указанного периода
        period_data = historical_manager.get_data_for_period(period_index)
        analyzer = CapitalFlowAnalyzer(period_data, historical_manager)
        sector_flows = analyzer.calculate_sector_flows()
        
        # Получаем информацию о периоде
        period_info = historical_manager.get_period_metadata(period_index)
        
        # Создаем график потоков капитала
        sectors = sector_flows.index.tolist()
        volumes = [float(v) for v in sector_flows['volume_share'].tolist()]  # Конвертируем в float
        
        fig_data = [{
            'x': sectors,
            'y': volumes,
            'type': 'bar',
            'name': 'Доля объема (%)',
            'marker': {'color': 'lightblue'},
            'text': [f"{v:.1f}%" for v in volumes],
            'textposition': 'outside'
        }]
        
        layout = {
            'title': f'💰 Потоки капитала по секторам<br><sub>{period_info["label"]}</sub>',
            'xaxis': {'title': 'Сектор', 'tickangle': -45},
            'yaxis': {'title': 'Доля объема торгов (%)'},
            'height': 500,
            'margin': {'b': 120}
        }
        
        return jsonify({
            'data': fig_data, 
            'layout': layout,
            'period_info': period_info
        })
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/test')
def test():
    """Тестовая страница"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Тест временных фильтров</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    </head>
    <body>
        <h1>Тест временных фильтров</h1>
        
        <div>
            <label>Выберите период:</label>
            <select id="period-select" onchange="updateChart()">
                <option value="0">Загрузка...</option>
            </select>
        </div>
        
        <div id="chart" style="height: 500px;"></div>
        
        <script>
            let currentPeriod = 0;
            
            async function loadPeriods() {
                try {
                    const response = await fetch('/api/time-periods');
                    const data = await response.json();
                    
                    const select = document.getElementById('period-select');
                    select.innerHTML = '';
                    
                    data.periods.forEach(period => {
                        const option = document.createElement('option');
                        option.value = period.value;
                        option.textContent = period.label;
                        select.appendChild(option);
                    });
                    
                    updateChart();
                } catch (error) {
                    console.error('Ошибка загрузки периодов:', error);
                }
            }
            
            async function updateChart() {
                const period = document.getElementById('period-select').value;
                
                try {
                    const response = await fetch(`/api/capital-flows?period=${period}`);
                    const data = await response.json();
                    
                    if (data.data && data.layout) {
                        Plotly.newPlot('chart', data.data, data.layout);
                        console.log('График обновлен для периода:', period);
                    } else if (data.error) {
                        document.getElementById('chart').innerHTML = `<div style="color: red;">Ошибка: ${data.error}</div>`;
                    }
                } catch (error) {
                    console.error('Ошибка загрузки графика:', error);
                    document.getElementById('chart').innerHTML = `<div style="color: red;">Ошибка загрузки</div>`;
                }
            }
            
            // Инициализация
            loadPeriods();
        </script>
    </body>
    </html>
    """

if __name__ == '__main__':
    print("🚀 Запуск тестового сервера...")
    app.run(debug=True, port=5006)