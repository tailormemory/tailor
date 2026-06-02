-- TAILOR — Memoization dei verdetti di fact supersession (fix #2).
-- Applied to db/facts.sqlite3.
--
-- fact_supersession.py giudica ~15k coppie/notte via LLM. Con corpus stabile
-- ri-giudica ogni notte le STESSE coppie con gli STESSI input → stesso verdetto.
-- Questa tabella memoizza i verdetti così che le coppie già giudicate (e i cui
-- due fatti non sono cambiati) vengano skippate, senza consumare budget LLM.
--
-- Chiave normalizzata: fact_id_a = id più basso, fact_id_b = id più alto.
-- Invalidazione: un verdetto cached è valido solo se ENTRAMBI i fatti hanno
-- created_at < judged_at (i fatti sono immutabili; created_at cambia solo se un
-- id viene rigenerato da un rebuild/re-extract — caso in cui vogliamo ri-giudicare).
-- judged_at è un full ISO timestamp (datetime.now().isoformat()), NON date-only,
-- per consentire il confronto lessicografico con facts.created_at.
--
-- Lo script fact_supersession.py crea questa tabella anche inline
-- (CREATE TABLE IF NOT EXISTS) per essere self-bootstrapping; questa migration
-- è la registrazione canonica dello schema.

CREATE TABLE IF NOT EXISTS judged_pairs (
    fact_id_a INTEGER NOT NULL,   -- id più basso (= old_f)
    fact_id_b INTEGER NOT NULL,   -- id più alto (= new_f)
    verdict   TEXT NOT NULL,      -- 'SUPERSEDES' | 'INDEPENDENT'
    judged_at TEXT NOT NULL,      -- datetime.now().isoformat(), full timestamp
    PRIMARY KEY (fact_id_a, fact_id_b)
);
