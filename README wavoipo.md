# CallOps — DDM

> Plataforma de discagem automática via WhatsApp integrada com Vapi AI, Wavoip e Supabase.

---

## Visão Geral

O CallOps é um dashboard de call center inteligente que permite:

- Monitorar o status das linhas WhatsApp (Wavoip) em tempo real
- Importar planilhas de contatos e verificar débitos automaticamente
- Disparar campanhas de ligação em massa via agente de voz AI (Júlia)
- Failover automático entre linhas — se uma cair, a próxima assume
- Fila assíncrona com Celery + Redis para alto volume

---

## Stack

| Camada | Tecnologia |
|---|---|
| Backend | Python + Flask |
| Fila | Celery + Redis |
| Banco de dados | Supabase (PostgreSQL) |
| Agente de voz | Vapi AI |
| Linhas WhatsApp | Wavoip (SIP Trunk) |
| Deploy | Railway |
| Servidor | Gunicorn |

---

## Arquitetura

```
Usuário
  ↓
Dashboard Flask (UI + API)
  ↓
Redis (fila de jobs)
  ↓
Celery Worker (processa 1 por vez)
  ↓
Vapi AI (agente Júlia)
  ↓
Wavoip SIP Trunk
  ↓
WhatsApp do devedor
  ↓
Supabase (salva resultado)
```

---

## Capacidade

| Configuração | Disparos/hora | Ligações/dia (10h) |
|---|---|---|
| concurrency=1 | ~1.800 | ~18.000 |
| concurrency=3 | ~5.400 | ~54.000 |
| concurrency=5 | ~9.000 | ~90.000 |

> **Importante:** O WhatsApp não permite múltiplas chamadas simultâneas no mesmo número.
> Com 5 dispositivos Wavoip, o máximo real é 5 ligações simultâneas.
> Para aumentar o volume, adicione mais dispositivos Wavoip.

---

## Failover Automático

O sistema verifica as linhas disponíveis **a cada ligação** usando round-robin com fallback:

```
Ligação 1 → Cruzeiro
Ligação 2 → Dispositivo 1
Ligação 3 → Dispositivo 2
...
Cruzeiro cai → próxima ligação vai pro Dispositivo 1
Todos caem   → worker pausa e verifica a cada 30s por até 30min
Linha volta  → worker retoma fila automaticamente
```

Ordem de prioridade das linhas (configurável em `tasks.py`):
1. Cruzeiro
2. Dispositivo 1
3. Dispositivo 2
4. Dispositivo 3
5. Dispositivo 4

---

## Estrutura do Projeto

```
wavoip-dashboard/
├── app.py              # Flask — dashboard + API REST
├── tasks.py            # Celery — tarefas assíncronas de ligação
├── requirements.txt    # Dependências Python
├── Procfile            # Processos Railway (web + worker)
├── railway.toml        # Configuração de deploy Railway
├── .env.example        # Template de variáveis de ambiente
├── .gitignore
└── templates/
    └── index.html      # Dashboard (HTML + CSS + JS)
```

---

## Variáveis de Ambiente

Copie `.env.example` para `.env` e preencha:

```env
# Wavoip — credenciais da conta
WAVOIP_EMAIL=seu@email.com
WAVOIP_PASSWORD=suasenha

# Vapi — agente de voz
VAPI_API_KEY=sua_private_key
VAPI_ASSISTANT_ID=id_da_julia

# Vapi — Phone Number IDs dos SIP trunks (um por dispositivo Wavoip)
VAPI_PHONE_NUMBER_ID_1=id_cruzeiro
VAPI_PHONE_NUMBER_ID_2=id_dispositivo1
VAPI_PHONE_NUMBER_ID_3=id_dispositivo2
VAPI_PHONE_NUMBER_ID_4=id_dispositivo3
VAPI_PHONE_NUMBER_ID_5=id_dispositivo4

# Supabase
SUPABASE_URL=https://xxxx.supabase.co
SUPABASE_KEY=sua_service_role_key

# Redis (gerado automaticamente pelo Railway)
REDIS_URL=redis://...
```

> Os `VAPI_PHONE_NUMBER_ID_*` são os IDs dos Phone Numbers cadastrados no painel do Vapi,
> cada um vinculado ao SIP trunk do respectivo dispositivo Wavoip.

---

## Como Mapear os Dispositivos Wavoip

Cada dispositivo Wavoip tem um **token único** que serve como username SIP.
Esse token precisa ser mapeado para o `phoneNumberId` correspondente no Vapi.

| Dispositivo | Token Wavoip (username SIP) | Variável |
|---|---|---|
| Cruzeiro | `88b232ad-5c1d-404f-8652-f5399e6a6f51` | `VAPI_PHONE_NUMBER_ID_1` |
| Dispositivo 1 | `ed49616d-fb19-46df-96b8-decab4cde3cf` | `VAPI_PHONE_NUMBER_ID_2` |
| Dispositivo 2 | `c8af8686-ca83-4eef-b757-f53940426011` | `VAPI_PHONE_NUMBER_ID_3` |
| Dispositivo 3 | `49d7328f-13ab-469f-b8a9-b7eb2b713f43` | `VAPI_PHONE_NUMBER_ID_4` |
| Dispositivo 4 | `8d739b23-77ed-4eb7-b807-e81270eb4ddb` | `VAPI_PHONE_NUMBER_ID_5` |

Para cadastrar um SIP trunk no Vapi:
1. Acesse **Integrations → SIP Trunk** no painel do Vapi
2. Crie um novo trunk com:
   - **Gateway:** `sipv2.wavoip.com:5060`
   - **Username:** token do dispositivo Wavoip
   - **Password:** senha SIP do dispositivo Wavoip
3. Copie o `phoneNumberId` gerado e coloque na variável correspondente

---

## Banco de Dados (Supabase)

Execute o arquivo `schema.sql` no SQL Editor do Supabase para criar as tabelas:

```sql
-- Contatos com débito
CREATE TABLE contacts (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name        text NOT NULL,
  cpf         text,
  cpf_norm    text,
  institution text,
  phones      jsonb DEFAULT '[]',
  has_debt    boolean DEFAULT false,
  debt_amount numeric(12,2),
  created_at  timestamptz DEFAULT now()
);

-- Histórico de chamadas
CREATE TABLE calls (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  contact_id      uuid REFERENCES contacts(id),
  contact_name    text,
  customer_number text,
  vapi_call_id    text,
  status          text DEFAULT 'queued',
  duration        int,
  created_at      timestamptz DEFAULT now()
);

-- Controle de campanhas
CREATE TABLE campaign_calls (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  campaign_id  text NOT NULL,
  cpf          text,
  phone        text,
  status       text DEFAULT 'pendente',
  vapi_call_id text,
  error        text,
  created_at   timestamptz DEFAULT now()
);
```

---

## Rodando Localmente

```bash
# 1. Instalar dependências
pip install -r requirements.txt

# 2. Configurar variáveis
cp .env.example .env
# editar .env com suas credenciais

# 3. Subir Redis local (via Docker)
docker run -d -p 6379:6379 redis:alpine

# 4. Rodar Flask
py -3.14 app.py

# 5. Rodar worker Celery (outro terminal)
celery -A tasks.celery worker --loglevel=info --concurrency=1
```

Acessa em `http://localhost:5000`

---

## Deploy no Railway

1. Crie um novo projeto no [Railway](https://railway.app)
2. Adicione o plugin **Redis** ao projeto
3. Conecte o repositório GitHub
4. Configure as variáveis de ambiente (copie do `.env.example`)
5. O `REDIS_URL` é gerado automaticamente pelo Railway
6. O Railway detecta o `Procfile` e sobe automaticamente:
   - **web** — Flask com Gunicorn
   - **worker** — Celery worker

---

## API Endpoints

| Método | Endpoint | Descrição |
|---|---|---|
| `GET` | `/api/lines` | Status das linhas Wavoip |
| `GET` | `/api/contacts` | Lista contatos com paginação e busca |
| `GET` | `/api/calls` | Histórico de chamadas |
| `POST` | `/api/call` | Dispara uma ligação manual |
| `POST` | `/api/import/preview` | Preview de planilha com verificação de débito |
| `POST` | `/api/import/call-batch` | Dispara lote (síncrono, uso local) |
| `POST` | `/api/campaign/start` | Inicia campanha via fila Celery (produção) |
| `GET` | `/api/campaign/<id>/status` | Progresso da campanha |

---

## Importação de Planilha

O dashboard aceita arquivos `.xlsx` e `.csv` com as seguintes colunas:

| Coluna | Obrigatório | Aliases aceitos |
|---|---|---|
| CPF | ✅ | `cpf`, `CPF`, `Cpf` |
| Nome | ✅ | `nome`, `name`, `Nome` |
| Telefone | ❌ | `telefone`, `tel`, `fone`, `celular`, `phone` |

O sistema normaliza o CPF automaticamente (remove pontos e traços) e verifica
no Supabase se o contato tem débito registrado.

---

## Fluxo de Campanha

```
1. Importar planilha (.xlsx ou .csv)
2. Sistema verifica débito por CPF no Supabase
3. Preview mostra: total / com débito / sem débito
4. Clicar "Ligar para todos com débito"
5. Jobs enfileirados no Redis
6. Worker Celery processa 1 por vez
7. Cada ligação:
   a. Verifica linhas saudáveis (Wavoip API)
   b. Seleciona linha via round-robin
   c. Dispara via Vapi (agente Júlia)
   d. Se linha falhar → tenta próxima automaticamente
   e. Se todas caírem → pausa 30s e tenta novamente
   f. Salva resultado no Supabase
```

---

## Licença

Uso interno — Grupo DDM © 2026
