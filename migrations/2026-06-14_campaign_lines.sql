-- Vinculo de SIPs por campanha e auditoria da linha usada em cada chamada.
-- Rode no Supabase SQL Editor antes do deploy desta versao.

ALTER TABLE campaigns
  ADD COLUMN IF NOT EXISTS line_tokens jsonb NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE campaign_calls
  ADD COLUMN IF NOT EXISTS line_token text,
  ADD COLUMN IF NOT EXISTS line_name text,
  ADD COLUMN IF NOT EXISTS phone_number_id text;

CREATE INDEX IF NOT EXISTS idx_campaign_calls_line_token
  ON campaign_calls (line_token);
