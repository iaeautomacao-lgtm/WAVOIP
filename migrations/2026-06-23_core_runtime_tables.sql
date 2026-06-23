-- Runtime tables required by the Flask/Celery application.
-- Safe to run more than once in Supabase SQL Editor.

CREATE TABLE IF NOT EXISTS campaigns (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  status text NOT NULL DEFAULT 'rascunho',
  total integer NOT NULL DEFAULT 0,
  finished integer NOT NULL DEFAULT 0,
  fired integer NOT NULL DEFAULT 0,
  line_tokens jsonb NOT NULL DEFAULT '[]'::jsonb,
  sip_group_id uuid,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS campaign_calls (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  campaign_id uuid REFERENCES campaigns(id) ON DELETE CASCADE,
  cpf text,
  phone text,
  name text,
  status text NOT NULL DEFAULT 'pendente',
  order_idx integer NOT NULL DEFAULT 0,
  debito_data jsonb DEFAULT '{}'::jsonb,
  vapi_call_id text,
  duration integer,
  error text,
  answered boolean NOT NULL DEFAULT false,
  watchdog_retries integer NOT NULL DEFAULT 0,
  line_token text,
  line_name text,
  phone_number_id text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS import_jobs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  status text NOT NULL DEFAULT 'queued',
  total integer NOT NULL DEFAULT 0,
  processed integer NOT NULL DEFAULT 0,
  with_debt integer NOT NULL DEFAULT 0,
  celery_task_id text,
  result jsonb,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS acordos_formalizados (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  cpf text,
  nome text,
  email text,
  instituicao text,
  valor text,
  forma_pagamento text,
  link_boleto text,
  link_pix text,
  linha_dig text,
  vencimento text,
  nr_acordo text,
  email_enviado boolean NOT NULL DEFAULT false,
  vapi_call_id text,
  campaign_call_id uuid REFERENCES campaign_calls(id) ON DELETE SET NULL,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

ALTER TABLE campaigns
  ADD COLUMN IF NOT EXISTS finished integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS fired integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS line_tokens jsonb NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS sip_group_id uuid,
  ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();

ALTER TABLE campaign_calls
  ADD COLUMN IF NOT EXISTS answered boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS watchdog_retries integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS line_token text,
  ADD COLUMN IF NOT EXISTS line_name text,
  ADD COLUMN IF NOT EXISTS phone_number_id text,
  ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();

ALTER TABLE import_jobs
  ADD COLUMN IF NOT EXISTS celery_task_id text,
  ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_campaigns_status
  ON campaigns (status);

CREATE INDEX IF NOT EXISTS idx_campaign_calls_campaign_status_order
  ON campaign_calls (campaign_id, status, order_idx);

CREATE INDEX IF NOT EXISTS idx_campaign_calls_vapi_call_id
  ON campaign_calls (vapi_call_id);

CREATE INDEX IF NOT EXISTS idx_campaign_calls_updated_at
  ON campaign_calls (updated_at);

CREATE INDEX IF NOT EXISTS idx_campaign_calls_line_token
  ON campaign_calls (line_token);

CREATE INDEX IF NOT EXISTS idx_import_jobs_status
  ON import_jobs (status);

CREATE INDEX IF NOT EXISTS idx_acordos_campaign_call_id
  ON acordos_formalizados (campaign_call_id);
