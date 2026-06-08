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

CREATE TRIGGER trg_contacts_updated_at
  BEFORE UPDATE ON contacts
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_calls_updated_at
  BEFORE UPDATE ON calls
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ── RLS ───────────────────────────────────────────────────────
ALTER TABLE contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE calls    ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service role full access" ON contacts FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service role full access" ON calls    FOR ALL USING (true) WITH CHECK (true);
