CREATE DATABASE IF NOT EXISTS wavoip
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE wavoip;

CREATE TABLE IF NOT EXISTS contacts (
  id char(36) PRIMARY KEY,
  name varchar(255) NOT NULL,
  cpf varchar(32),
  cpf_norm varchar(32),
  institution varchar(255),
  phones json,
  has_debt boolean DEFAULT false,
  debt_amount decimal(12,2),
  notes text,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_contacts_cpf (cpf),
  INDEX idx_contacts_cpf_norm (cpf_norm),
  INDEX idx_contacts_has_debt (has_debt),
  FULLTEXT INDEX idx_contacts_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS calls (
  id char(36) PRIMARY KEY,
  contact_id char(36),
  contact_name varchar(255),
  customer_number varchar(32),
  vapi_call_id varchar(128),
  status varchar(64) DEFAULT 'queued',
  duration int,
  line_id varchar(128),
  phone_number_id varchar(128),
  transcript longtext,
  metadata json,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_calls_contact_id (contact_id),
  INDEX idx_calls_status (status),
  INDEX idx_calls_created_at (created_at),
  INDEX idx_calls_vapi_id (vapi_call_id),
  CONSTRAINT fk_calls_contact_id FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS campaigns (
  id char(36) PRIMARY KEY,
  name varchar(255) NOT NULL,
  status varchar(64) NOT NULL DEFAULT 'rascunho',
  total int NOT NULL DEFAULT 0,
  finished int NOT NULL DEFAULT 0,
  fired int NOT NULL DEFAULT 0,
  line_tokens json,
  sip_group_id char(36),
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_campaigns_status (status),
  INDEX idx_campaigns_sip_group_id (sip_group_id),
  INDEX idx_campaigns_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS campaign_calls (
  id char(36) PRIMARY KEY,
  campaign_id char(36),
  cpf varchar(32),
  phone varchar(32),
  name varchar(255),
  status varchar(64) NOT NULL DEFAULT 'pendente',
  order_idx int NOT NULL DEFAULT 0,
  debito_data json,
  vapi_call_id varchar(128),
  duration int,
  error text,
  answered boolean NOT NULL DEFAULT false,
  watchdog_retries int NOT NULL DEFAULT 0,
  line_token varchar(128),
  line_name varchar(255),
  phone_number_id varchar(128),
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_campaign_calls_campaign_status_order (campaign_id, status, order_idx),
  INDEX idx_campaign_calls_vapi_call_id (vapi_call_id),
  INDEX idx_campaign_calls_updated_at (updated_at),
  INDEX idx_campaign_calls_line_token (line_token),
  CONSTRAINT fk_campaign_calls_campaign_id FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS import_jobs (
  id char(36) PRIMARY KEY,
  status varchar(64) NOT NULL DEFAULT 'queued',
  total int NOT NULL DEFAULT 0,
  processed int NOT NULL DEFAULT 0,
  with_debt int NOT NULL DEFAULT 0,
  celery_task_id varchar(128),
  result json,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_import_jobs_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS acordos_formalizados (
  id char(36) PRIMARY KEY,
  cpf varchar(32),
  nome varchar(255),
  email varchar(255),
  instituicao varchar(255),
  valor varchar(64),
  forma_pagamento varchar(64),
  link_boleto text,
  link_pix text,
  linha_dig text,
  vencimento varchar(64),
  nr_acordo varchar(128),
  email_enviado boolean NOT NULL DEFAULT false,
  vapi_call_id varchar(128),
  campaign_call_id char(36),
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_acordos_campaign_call_id (campaign_call_id),
  CONSTRAINT fk_acordos_campaign_call_id FOREIGN KEY (campaign_call_id) REFERENCES campaign_calls(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS sip_groups (
  id char(36) PRIMARY KEY,
  name varchar(255) NOT NULL UNIQUE,
  line_tokens json,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS line_overrides (
  line_token varchar(128) PRIMARY KEY,
  paused boolean NOT NULL DEFAULT false,
  reason text,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
