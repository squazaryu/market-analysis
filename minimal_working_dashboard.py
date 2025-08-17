#!/usr/bin/env python3
"""
Минимальный рабочий дашборд без лишних функций
"""

from flask import Flask, render_template_string, jsonify
import pandas as pd
import plotly.graph_objects as go
import json
from pathlib import Path
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

@app.route('/')
def index():
    return """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <title>📊 Минимальный ETF Дашборд</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .chart { height: 500px; margin: 20px 0; border: 1px solid #ddd; }
        h1 { color: #333; }
    </style>
</head>
<body>
    <h1>📊 Минимальный ETF Дашборд</h1>
    
    <h2>График риск-доходность</h2>
    <div id="risk-return-plot" class="chart"></div>
    
    <h2>Потоки капитала</h2>
    <div id="capital-flows-plot" class="chart"></div>
    
    <script>
        console.log('🚀 Загружаем дашборд...');
        
        // График риск-доходность
        fetch('/api/chart')
          .then(response => response.json())
          .then(data => {
            if (data.data && data.layout) {
              Plotly.newPlot('risk-return-plot', data.data, data.layout, {responsive: true});
              console.log('✅ График риск-доходность загружен');
            }
          })
          .catch(error => console.error('Ошибка:', error));
        
        // Потоки капитала
        fetch('/api/capital-flows')
          .then(response => response.json())
          .then(data => {
            if (data.data && data.layout) {
              Plotly.newPlot('capital-flows-plot', data.data, data.layout, {responsive: true});
              console.log('✅ Потоки капитала загружены');
            }
          })
          .catch(error => console.error('Ошибка:', error));
    </script>
</body>
</html>
    """

@app.route('/api/chart')
def api_chart():
    """API графика риск-доходность"""
    if etf_data is None:
        return jsonify({'error': 'Данные не загружены'})
    
    try:
        # Создаем scatter plot
        fig_data = [{
            'x': etf_data['volatility'].tolist(),
            'y': etf_data['annual_return'].tolist(),
            'text': etf_data['ticker'].tolist(),
            'mode': 'markers',
            'type': 'scatter',
            'marker': {
                'size': 8,
                'color': etf_data['annual_return'].tolist(),
                'colorscale': 'RdYlGn',
                'showscale': True
            }
        }]
        
        layout = {
            'title': 'Риск vs Доходность',
            'xaxis': {'title': 'Волатильность (%)'},
            'yaxis': {'title': 'Годовая доходность (%)'},
            'hovermode': 'closest'
        }
        
        return jsonify({'data': fig_data, 'layout': layout})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/capital-flows')
def api_capital_flows():
    """API потоков капитала"""
    if etf_data is None:
        return jsonify({'error': 'Данные не загружены'})
    
    try:
        analyzer = CapitalFlowAnalyzer(etf_data.copy())
        sector_flows = analyzer.calculate_sector_flows()
        
        sectors = sector_flows.index.tolist()
        volumes = [float(v) for v in sector_flows['volume_share'].tolist()]
        
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
            'title': '💰 Потоки капитала по секторам',
            'xaxis': {'title': 'Сектор', 'tickangle': -45},
            'yaxis': {'title': 'Доля объема торгов (%)'},
            'height': 500,
            'margin': {'b': 120}
        }
        
        return jsonify({'data': fig_data, 'layout': layout})
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    print("🚀 Запуск минимального дашборда...")
    app.run(debug=False, host='0.0.0.0', port=5006)