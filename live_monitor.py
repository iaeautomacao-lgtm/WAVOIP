import sys
import os
import json
import time
import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

VAPI_KEY = "332987f4-f832-4542-9fd0-76de02bde971"
VAPI_HEADERS = {"Authorization": f"Bearer {VAPI_KEY}"}

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>WAVOIP Live Monitor - Painel ao Vivo</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-dark: #0f172a;
            --card-bg: #1e293b;
            --border-color: #334155;
            --text-primary: #f8fafc;
            --text-secondary: #94a3b8;
            --accent-orange: #ff5706;
            --accent-green: #10b981;
            --accent-red: #ef4444;
            --accent-blue: #3b82f6;
            --accent-yellow: #f59e0b;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
            font-family: 'Inter', -apple-system, sans-serif;
        }

        body {
            background-color: var(--bg-dark);
            color: var(--text-primary);
            padding: 24px;
            min-height: 100vh;
        }

        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding-bottom: 24px;
            border-bottom: 1px solid var(--border-color);
            margin-bottom: 24px;
        }

        .brand {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .brand-logo {
            width: 40px;
            height: 40px;
            background: linear-gradient(135deg, var(--accent-orange), #ea580c);
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 800;
            font-size: 20px;
            color: white;
            box-shadow: 0 4px 12px rgba(255, 87, 6, 0.3);
        }

        .brand-title {
            font-size: 22px;
            font-weight: 700;
            letter-spacing: -0.5px;
        }

        .brand-subtitle {
            font-size: 13px;
            color: var(--text-secondary);
        }

        .live-badge {
            display: flex;
            align-items: center;
            gap: 8px;
            background: rgba(16, 185, 129, 0.1);
            border: 1px solid rgba(16, 185, 129, 0.3);
            padding: 6px 14px;
            border-radius: 20px;
            color: var(--accent-green);
            font-size: 13px;
            font-weight: 600;
        }

        .pulse-dot {
            width: 8px;
            height: 8px;
            background-color: var(--accent-green);
            border-radius: 50%;
            animation: pulse 1.5s infinite;
        }

        @keyframes pulse {
            0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
            70% { transform: scale(1); box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); }
            100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 28px;
        }

        .stat-card {
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 18px;
        }

        .stat-label {
            font-size: 13px;
            color: var(--text-secondary);
            margin-bottom: 6px;
            font-weight: 500;
        }

        .stat-value {
            font-size: 28px;
            font-weight: 700;
        }

        .logs-container {
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 14px;
            overflow: hidden;
        }

        .logs-header {
            padding: 16px 20px;
            border-bottom: 1px solid var(--border-color);
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: rgba(15, 23, 42, 0.4);
        }

        .logs-title {
            font-weight: 600;
            font-size: 15px;
        }

        .call-list {
            display: flex;
            flex-direction: column;
            divide-y: 1px solid var(--border-color);
        }

        .call-item {
            padding: 16px 20px;
            border-bottom: 1px solid var(--border-color);
            transition: background-color 0.2s;
        }

        .call-item:hover {
            background-color: rgba(255, 255, 255, 0.02);
        }

        .call-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .customer-info {
            display: flex;
            flex-direction: column;
            gap: 4px;
        }

        .customer-name {
            font-weight: 600;
            font-size: 15px;
        }

        .customer-phone {
            font-size: 13px;
            color: var(--text-secondary);
            font-family: monospace;
        }

        .status-pill {
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .status-ended-success {
            background: rgba(16, 185, 129, 0.15);
            color: var(--accent-green);
            border: 1px solid rgba(16, 185, 129, 0.3);
        }

        .status-ringing {
            background: rgba(59, 130, 246, 0.15);
            color: var(--accent-blue);
            border: 1px solid rgba(59, 130, 246, 0.3);
            animation: pulse-blue 1s infinite alternate;
        }

        .status-no-answer {
            background: rgba(245, 158, 11, 0.15);
            color: var(--accent-yellow);
            border: 1px solid rgba(245, 158, 11, 0.3);
        }

        .status-failed {
            background: rgba(239, 68, 68, 0.15);
            color: var(--accent-red);
            border: 1px solid rgba(239, 68, 68, 0.3);
        }

        .call-meta {
            font-size: 12px;
            color: var(--text-secondary);
            text-align: right;
        }

        .transcript-box {
            margin-top: 12px;
            background: #0f172a;
            border-radius: 8px;
            padding: 12px;
            font-size: 13px;
            line-height: 1.5;
            color: #cbd5e1;
            border-left: 3px solid var(--accent-orange);
        }

        .empty-state {
            padding: 40px;
            text-align: center;
            color: var(--text-secondary);
        }
    </style>
</head>
<body>
    <div class="header">
        <div class="brand">
            <div class="brand-logo">W</div>
            <div>
                <div class="brand-title">WAVOIP Monitor</div>
                <div class="brand-subtitle">Monitoramento de Ligações ao Vivo</div>
            </div>
        </div>
        <div class="live-badge">
            <div class="pulse-dot"></div>
            MONITORANDO EM TEMPO REAL
        </div>
    </div>

    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-label">Total de Discagens</div>
            <div class="stat-value" id="stat-total">0</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Atendidas / Conectadas</div>
            <div class="stat-value" style="color: var(--accent-green)" id="stat-answered">0</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Tocando / Em Andamento</div>
            <div class="stat-value" style="color: var(--accent-blue)" id="stat-active">0</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Não Atendidas / Recusadas</div>
            <div class="stat-value" style="color: var(--accent-yellow)" id="stat-failed">0</div>
        </div>
    </div>

    <div class="logs-container">
        <div class="logs-header">
            <div class="logs-title">Transmissão de Logs de Ligações</div>
            <div style="font-size: 12px; color: var(--text-secondary);" id="last-update">Atualizando...</div>
        </div>
        <div id="call-list" class="call-list">
            <div class="empty-state">Buscando chamadas recentes...</div>
        </div>
    </div>

    <script>
        async function fetchLogs() {
            try {
                const res = await fetch('/api/calls');
                const data = await res.json();
                
                if (!data.ok) return;

                // Update Stats
                document.getElementById('stat-total').innerText = data.stats.total;
                document.getElementById('stat-answered').innerText = data.stats.answered;
                document.getElementById('stat-active').innerText = data.stats.active;
                document.getElementById('stat-failed').innerText = data.stats.failed;
                document.getElementById('last-update').innerText = 'Última atualização: ' + new Date().toLocaleTimeString();

                // Render Calls
                const list = document.getElementById('call-list');
                if (data.calls.length === 0) {
                    list.innerHTML = '<div class="empty-state">Nenhuma ligação registrada até o momento.</div>';
                    return;
                }

                list.innerHTML = data.calls.map(c => {
                    let pillClass = 'status-failed';
                    let statusLabel = c.ended_reason || c.status;

                    if (c.is_answered) {
                        pillClass = 'status-ended-success';
                        statusLabel = 'ATENDIDA / CONECTADA ✅';
                    } else if (c.status === 'ringing' || c.status === 'in-progress') {
                        pillClass = 'status-ringing';
                        statusLabel = 'TOCANDO / FALANDO 📞';
                    } else if (c.ended_reason === 'customer-did-not-answer') {
                        pillClass = 'status-no-answer';
                        statusLabel = 'NÃO ATENDEU 🔔';
                    } else if (c.ended_reason && (c.ended_reason.includes('480') || c.ended_reason.includes('busy'))) {
                        pillClass = 'status-failed';
                        statusLabel = 'OCUPADO / RECUSADO';
                    }

                    const timeStr = c.created_at ? new Date(c.created_at).toLocaleTimeString('pt-BR') : '';
                    const durationStr = c.duration ? ` (${c.duration}s)` : '';

                    return `
                        <div class="call-item">
                            <div class="call-row">
                                <div class="customer-info">
                                    <div class="customer-name">${c.customer_name || 'Devedor'}</div>
                                    <div class="customer-phone">${c.customer_phone || ''}</div>
                                </div>
                                <span class="status-pill ${pillClass}">${statusLabel}${durationStr}</span>
                                <div class="call-meta">
                                    <div><b>${timeStr}</b></div>
                                    <div>ID: ${c.id ? c.id.substring(0, 8) + '...' : ''}</div>
                                </div>
                            </div>
                            ${c.transcript ? `<div class="transcript-box"><b>Transcrição Júlia:</b><br>${c.transcript}</div>` : ''}
                        </div>
                    `;
                }).join('');

            } catch (err) {
                console.error('Erro ao atualizar logs:', err);
            }
        }

        setInterval(fetchLogs, 2000);
        fetchLogs();
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/calls')
def api_calls():
    try:
        # Fetch from Vapi API
        r_vapi = requests.get('https://api.vapi.ai/call?limit=30', headers=VAPI_HEADERS, timeout=8)
        calls_vapi = r_vapi.json() if r_vapi.ok else []

        # Fetch from cPanel API
        r_cpanel = requests.get('https://wavoip.grupoddm.ia.br/api/debug-import', timeout=8)
        calls_cpanel = r_cpanel.json().get('calls', []) if r_cpanel.ok else []

        cpanel_map = {c.get('vapi_call_id'): c for c in calls_cpanel if c.get('vapi_call_id')}

        calls = []
        stats = {
            'total': len(calls_vapi),
            'answered': 0,
            'active': 0,
            'failed': 0
        }

        for c in calls_vapi:
            v_id = c.get('id')
            status = c.get('status')
            reason = c.get('endedReason') or ''
            
            cpanel_data = cpanel_map.get(v_id, {})
            duration = cpanel_data.get('duration') or 0
            answered_db = cpanel_data.get('answered') == 1

            # Extract transcript if available
            transcript = cpanel_data.get('transcript') or ""
            if not transcript:
                msgs = c.get('messages', [])
                for m in msgs:
                    if m.get('role') in ('user', 'bot'):
                        content = m.get('content') or m.get('message')
                        if content:
                            role_label = "Júlia" if m.get('role') == 'bot' else "Devedor"
                            transcript += f"<b>{role_label}:</b> {content}<br>"

            is_answered = (
                answered_db or
                duration > 0 or
                reason in ('customer-ended-call', 'assistant-ended-call', 'silence-timed-out', 'voicemail', 'max-duration-exceeded') or
                bool(transcript.strip())
            )

            if status in ('ringing', 'in-progress', 'queued'):
                stats['active'] += 1
            elif is_answered:
                stats['answered'] += 1
            else:
                stats['failed'] += 1

            calls.append({
                'id': v_id,
                'customer_name': c.get('customer', {}).get('name') or cpanel_data.get('name') or 'Cliente',
                'customer_phone': c.get('customer', {}).get('number') or cpanel_data.get('phone') or '',
                'status': status,
                'ended_reason': reason,
                'is_answered': is_answered,
                'duration': duration,
                'created_at': c.get('createdAt') or cpanel_data.get('created_at'),
                'transcript': transcript
            })

        return jsonify({'ok': True, 'stats': stats, 'calls': calls})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

if __name__ == '__main__':
    print("==================================================")
    print("WAVOIP LIVE MONITOR RODANDO NA PORTA 5000!")
    print("ACESSE NO NAVEGADOR: http://localhost:5000")
    print("==================================================")
    app.run(host='0.0.0.0', port=5000, debug=False)
