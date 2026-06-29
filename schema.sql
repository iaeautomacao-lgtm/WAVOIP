-- =============================================================
-- CallOps — Schema completo
-- Rodar no Supabase SQL Editor do novo projeto
-- =============================================================

-- ── Contatos ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS contacts (
  id            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  name          text        NOT NULL,
  cpf           text,
  cpf_norm      text,                    -- CPF normalizado (só dígitos, 11 chars)
  institution   text,
  phones        jsonb       DEFAULT '[]', -- array de strings: ["5521999..."]
  has_debt      boolean     DEFAULT false,
  debt_amount   numeric(12,2),
  notes         text,
  created_at    timestamptz DEFAULT now(),
  updated_at    timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_contacts_cpf      ON contacts (cpf);
CREATE INDEX IF NOT EXISTS idx_contacts_cpf_norm ON contacts (cpf_norm);
CREATE INDEX IF NOT EXISTS idx_contacts_name     ON contacts USING gin (to_tsvector('portuguese', name));
CREATE INDEX IF NOT EXISTS idx_contacts_has_debt ON contacts (has_debt);

-- ── Chamadas ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS calls (
  id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  contact_id       uuid        REFERENCES contacts(id) ON DELETE SET NULL,
  contact_name     text,
  customer_number  text,
  vapi_call_id     text,
  status           text        DEFAULT 'queued', -- queued | in-progress | ended | failed
  duration         int,                          -- segundos
  line_id          text,                         -- device id da Wavoip usado
  phone_number_id  text,                         -- phoneNumberId do Vapi
  transcript       text,
  metadata         jsonb,
  created_at       timestamptz DEFAULT now(),
  updated_at       timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_calls_contact_id  ON calls (contact_id);
CREATE INDEX IF NOT EXISTS idx_calls_status      ON calls (status);
CREATE INDEX IF NOT EXISTS idx_calls_created_at  ON calls (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_calls_vapi_id     ON calls (vapi_call_id);

-- ── Trigger: updated_at automático ────────────────────────────
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_contacts_updated_at ON contacts;
CREATE TRIGGER trg_contacts_updated_at
  BEFORE UPDATE ON contacts
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_calls_updated_at ON calls;
CREATE TRIGGER trg_calls_updated_at
  BEFORE UPDATE ON calls
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ── RLS ───────────────────────────────────────────────────────
ALTER TABLE contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE calls    ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "service role full access" ON contacts;
CREATE POLICY "service role full access" ON contacts FOR ALL USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "service role full access" ON calls;
CREATE POLICY "service role full access" ON calls    FOR ALL USING (true) WITH CHECK (true);

-- Runtime: campanhas, importacao e acordos
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
  recording_url text,
  transcript text,
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

CREATE INDEX IF NOT EXISTS idx_campaigns_status ON campaigns (status);
CREATE INDEX IF NOT EXISTS idx_campaign_calls_campaign_status_order ON campaign_calls (campaign_id, status, order_idx);
CREATE INDEX IF NOT EXISTS idx_campaign_calls_vapi_call_id ON campaign_calls (vapi_call_id);
CREATE INDEX IF NOT EXISTS idx_campaign_calls_updated_at ON campaign_calls (updated_at);
CREATE INDEX IF NOT EXISTS idx_campaign_calls_line_token ON campaign_calls (line_token);
CREATE INDEX IF NOT EXISTS idx_import_jobs_status ON import_jobs (status);
CREATE INDEX IF NOT EXISTS idx_acordos_campaign_call_id ON acordos_formalizados (campaign_call_id);
