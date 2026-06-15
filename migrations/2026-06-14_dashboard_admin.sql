-- Dashboard operacional: grupos de SIP, pausa manual e auditoria por linha.
-- Pode ser rodado mesmo se a migration campaign_lines ainda nao tiver sido aplicada.

ALTER TABLE campaigns
  ADD COLUMN IF NOT EXISTS line_tokens jsonb NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS sip_group_id uuid;

ALTER TABLE campaign_calls
  ADD COLUMN IF NOT EXISTS line_token text,
  ADD COLUMN IF NOT EXISTS line_name text,
  ADD COLUMN IF NOT EXISTS phone_number_id text,
  ADD COLUMN IF NOT EXISTS answered boolean NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_campaign_calls_line_token
  ON campaign_calls (line_token);

CREATE TABLE IF NOT EXISTS sip_groups (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL UNIQUE,
  line_tokens jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS line_overrides (
  line_token text PRIMARY KEY,
  paused boolean NOT NULL DEFAULT false,
  reason text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_campaigns_sip_group_id
  ON campaigns (sip_group_id);
