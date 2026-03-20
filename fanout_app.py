# Fan-out Query Generator — GEO Edition
# App Streamlit che porta la logica del notebook in un'interfaccia web moderna.
# Tutti i dati vengono tenuti in st.session_state; nessun file viene scritto su disco.

import streamlit as st
import anthropic
import requests
import base64
import json
import csv
import math
import re
import io
import copy
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Configurazione pagina ──────────────────────────────────────────────────────
st.set_page_config(
    page_title="Fan-out Query Generator — GEO Edition",
    page_icon="🧭",
    layout="wide",
)

# ── Inizializzazione session_state ────────────────────────────────────────────
# IMPORTANTE: inizializzare PRIMA di qualsiasi widget
_DEFAULTS = {
    "fanout_data": {},
    "paa_questions": [],
    "fanout_modules": {},
    "dist_stats": {},
    "drill_results": [],
    "geo_fanout_combined": "",
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ══════════════════════════════════════════════════════════════════════════════
# COSTANTI E SCHEMA
# ══════════════════════════════════════════════════════════════════════════════

FANOUT_TOOL_SCHEMA = {
    "name": "save_fanout_queries",
    "description": "Salva le sub-query fan-out generate per ogni tipo semantico",
    "input_schema": {
        "type": "object",
        "properties": {
            "equivalent":       {"type": "array", "items": {"type": "object", "properties": {"query": {"type": "string"}, "priority": {"type": "integer", "minimum": 1, "maximum": 5}, "coverage_gap": {"type": "boolean"}}, "required": ["query", "priority", "coverage_gap"]}},
            "follow_up":        {"type": "array", "items": {"type": "object", "properties": {"query": {"type": "string"}, "priority": {"type": "integer"}, "coverage_gap": {"type": "boolean"}}, "required": ["query", "priority", "coverage_gap"]}},
            "generalization":   {"type": "array", "items": {"type": "object", "properties": {"query": {"type": "string"}, "priority": {"type": "integer"}, "coverage_gap": {"type": "boolean"}}, "required": ["query", "priority", "coverage_gap"]}},
            "canonicalization": {"type": "array", "items": {"type": "object", "properties": {"query": {"type": "string"}, "priority": {"type": "integer"}, "coverage_gap": {"type": "boolean"}}, "required": ["query", "priority", "coverage_gap"]}},
            "entailment":       {"type": "array", "items": {"type": "object", "properties": {"query": {"type": "string"}, "priority": {"type": "integer"}, "coverage_gap": {"type": "boolean"}}, "required": ["query", "priority", "coverage_gap"]}},
            "specification":    {"type": "array", "items": {"type": "object", "properties": {"query": {"type": "string"}, "priority": {"type": "integer"}, "coverage_gap": {"type": "boolean"}}, "required": ["query", "priority", "coverage_gap"]}},
            "clarification":    {"type": "array", "items": {"type": "object", "properties": {"query": {"type": "string"}, "priority": {"type": "integer"}, "coverage_gap": {"type": "boolean"}}, "required": ["query", "priority", "coverage_gap"]}},
            "translation":      {"type": "array", "items": {"type": "object", "properties": {"query": {"type": "string"}, "priority": {"type": "integer"}, "coverage_gap": {"type": "boolean"}}, "required": ["query", "priority", "coverage_gap"]}},
            "content_gaps":     {"type": "array", "items": {"type": "object", "properties": {"url_slug": {"type": "string"}, "covers_type": {"type": "string"}, "query_target": {"type": "string"}, "content_element": {"type": "string"}}, "required": ["url_slug", "covers_type", "query_target", "content_element"]}},
        },
        "required": ["equivalent", "follow_up", "generalization", "canonicalization", "entailment", "specification", "clarification", "translation"],
    },
}

DRILL_TOOL_SCHEMA = {
    "name": "save_drill_queries",
    "description": "Salva le sub-query drill-down con rationale",
    "input_schema": {
        "type": "object",
        "properties": {
            "queries": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "query":        {"type": "string"},
                        "priority":     {"type": "integer", "minimum": 1, "maximum": 5},
                        "coverage_gap": {"type": "boolean"},
                        "rationale":    {"type": "string"},
                    },
                    "required": ["query", "priority", "coverage_gap", "rationale"],
                },
            }
        },
        "required": ["queries"],
    },
}

TYPE_META = {
    "equivalent":       {"label": "Equivalent",      "desc": "Riformulazioni alternative che esprimono la stessa query con parole diverse",                     "example": "'come funziona X' → 'come opera X'",                         "color": "#3498db", "emoji": "🔄"},
    "follow_up":        {"label": "Follow-up",        "desc": "Domande successive logiche che l'utente farà dopo aver letto la risposta iniziale",                "example": "'cos'è X' → 'quanto costa X', 'dove comprare X'",            "color": "#9b59b6", "emoji": "➡️"},
    "generalization":   {"label": "Generalization",   "desc": "Versioni più ampie e generiche che inquadrano la query in un contesto più largo",                 "example": "'X per PMI' → 'soluzioni per aziende', 'mercato di X'",     "color": "#1abc9c", "emoji": "🔭"},
    "canonicalization": {"label": "Canonicalization", "desc": "Forme standardizzate, best-practice o denominazioni ufficiali del topic",                         "example": "'come fare X' → 'best practice X', 'standard X'",           "color": "#e67e22", "emoji": "📐"},
    "entailment":       {"label": "Entailment",       "desc": "Domande logicamente implicate: se X è vero, l'AI cerca anche le conseguenze",                    "example": "'X è sicuro' → 'rischi di X', 'certificazioni per X'",      "color": "#e74c3c", "emoji": "🔗"},
    "specification":    {"label": "Specification",    "desc": "Versioni più strette e specifiche: brand, modello, caso d'uso, segmento",                        "example": "'X' → 'X per settore Y', 'X modello Z'",                    "color": "#27ae60", "emoji": "🎯"},
    "clarification":    {"label": "Clarification",    "desc": "Disambiguazione tra significati o contesti diversi dello stesso termine",                         "example": "'X' → 'differenza tra X e Y', 'X nel contesto di Z'",      "color": "#f39c12", "emoji": "❓"},
    "translation":      {"label": "Translation",      "desc": "Stessa query riformulata per pubblici, livelli di expertise o contesti diversi",                "example": "'X tecnico' → 'X per non esperti', 'X per professionisti'", "color": "#95a5a6", "emoji": "🌐"},
}

TYPES_A   = ["equivalent", "follow_up", "generalization", "canonicalization"]
TYPES_B   = ["entailment", "specification", "clarification", "translation"]
ALL_TYPES = TYPES_A + TYPES_B

INDUSTRY_BENCHMARKS = {
    "general":    {"label": "General",    "avg_subq": "12-15", "citation_rate": "~50%", "color": "#7f8c8d"},
    "ecommerce":  {"label": "E-commerce", "avg_subq": "18-22", "citation_rate": "61%",  "color": "#27ae60"},
    "finance":    {"label": "Finance",    "avg_subq": "16-20", "citation_rate": "52%",  "color": "#2980b9"},
    "b2b_saas":   {"label": "B2B SaaS",   "avg_subq": "14-18", "citation_rate": "54%",  "color": "#8e44ad"},
    "healthcare": {"label": "Healthcare", "avg_subq": "22-28", "citation_rate": "48%",  "color": "#e74c3c"},
    "education":  {"label": "Education",  "avg_subq": "12-16", "citation_rate": "58%",  "color": "#f39c12"},
}

_TYPE_ROLE = {
    "equivalent":       ("paragrafo definitorio alternativo",
                         "Apri con una frase definitoria. Usa sinonimi e parafrasi della keyword. Struttura: definizione → meccanismo → esempio concreto."),
    "follow_up":        ("sezione Q&A",
                         "Formato domanda-risposta esplicito. Ogni Q&A deve essere autoconsistente. Includi almeno 3 coppie Q&A. Usa <h3> per le domande."),
    "generalization":   ("sezione contestuale",
                         "Inquadra la keyword nel contesto più ampio. Apri con il contesto macro, poi restringi alla keyword. Cita trend o dati di settore."),
    "canonicalization": ("sezione best-practice",
                         "Usa lista numerata (1., 2., 3...). Ogni punto: azione concreta + motivazione + esempio. Chiudi con una raccomandazione operativa."),
    "entailment":       ("sezione implicazioni e rischi",
                         "Struttura: implicazione principale → conseguenze → come gestirle. Includi dati numerici o statistiche. Bilancia opportunità e rischi."),
    "specification":    ("sezione tecnica/comparativa",
                         "Usa una tabella comparativa se possibile (|Aspetto|Opzione A|Opzione B|). Altrimenti lista tecnica con specifiche precise. Includi numeri."),
    "clarification":    ("sezione disambiguazione",
                         "Apri con 'Non confondere X con Y'. Spiega le differenze chiave. Usa esempi concreti per ciascun caso d'uso. Conclude con la regola pratica."),
    "translation":      ("sezione adattata per pubblico",
                         "Adatta il registro linguistico al pubblico target. Se tecnico → semplifica con analogie. Se generico → approfondisci con dettagli. Evita jargon non necessario."),
}

# ══════════════════════════════════════════════════════════════════════════════
# FUNZIONI CORE (logica identica al notebook, senza parti Colab)
# ══════════════════════════════════════════════════════════════════════════════

def _build_prompt(keyword, lang, atom_limit, types_batch, include_plan=False, paa_seed=None):
    """Costruisce il prompt testuale per Claude."""
    types_block = "\n".join(
        f'- "{t}": {TYPE_META[t]["desc"]} | Esempio: {TYPE_META[t]["example"]}'
        for t in types_batch
    )
    paa_block = ""
    if paa_seed:
        paa_block = (
            "\n\nPEOPLE ALSO ASK (domande reali da Google — usale come punto di partenza "
            "e ispirazione per generare sub-query semanticamente allineate al comportamento reale di ricerca):\n"
            + "\n".join(f"- {q}" for q in paa_seed[:25])
            + "\n"
        )
    plan_instr = (
        '\nIncludi anche "content_gaps": lista di max 5 pagine/sezioni da creare '
        'per coprire i tipi con opportunità maggiori.'
        if include_plan else ""
    )
    plan_schema = (
        '\n  "content_gaps": ['
        '\n    {"url_slug": "slug", "covers_type": "tipo", '
        '"query_target": "query principale", '
        '"content_element": "definizione|lista|tabella|faq|comparativa|scheda-tecnica"}'
        '\n  ],'
        if include_plan else ""
    )
    types_schema = "\n  ".join(
        f'"{t}": [{{"query": "...", "priority": 1, "coverage_gap": true}}],'
        for t in types_batch
    )
    return (
        f"Sei un esperto GEO/SEO specializzato in AI Query Fan-out.\n\n"
        f"I sistemi AI (Google AI Mode, Perplexity, Claude) espandono ogni query "
        f"in sotto-query per TIPO DI TRASFORMAZIONE SEMANTICA, non per intent commerciale.\n\n"
        f"KEYWORD TARGET: {keyword}\n"
        f"LINGUA OUTPUT: {lang}\n"
        f"MAX SUB-QUERY PER TIPO: {atom_limit}\n"
        f"{paa_block}\n"
        f"TIPI DA GENERARE:\n{types_block}\n\n"
        f"Per ogni sub-query:\n"
        f'- "query": stringa concisa in lingua {lang}\n'
        f'- "priority": 1 (bassa) → 5 (alta): probabilità che AI Mode generi questa sotto-query\n'
        f'- "coverage_gap": true se questa sub-query è raramente coperta nei contenuti tipici '
        f'sulla keyword, false se è solitamente ben trattata\n'
        f'{plan_instr}\n\n'
        f"Rispondi SOLO con JSON valido, nessun backtick.\n"
        f"Struttura:\n{{\n  {types_schema}{plan_schema}\n}}"
    )


def _call_claude(keyword, lang, atom_limit, types_batch, include_plan, api_key, max_tokens=2000, paa_seed=None):
    """Chiama Claude usando tool_use per structured output."""
    client = anthropic.Anthropic(api_key=api_key.strip())
    tool_schema = copy.deepcopy(FANOUT_TOOL_SCHEMA)
    keep = set(types_batch)
    if include_plan:
        keep.add("content_gaps")
    tool_schema["input_schema"]["properties"] = {
        k: v for k, v in tool_schema["input_schema"]["properties"].items() if k in keep
    }
    tool_schema["input_schema"]["required"] = [
        k for k in tool_schema["input_schema"]["required"] if k in keep
    ]
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=max_tokens,
        system="Sei un esperto GEO/SEO specializzato in AI Query Fan-out.",
        tools=[tool_schema],
        tool_choice={"type": "tool", "name": "save_fanout_queries"},
        messages=[{"role": "user", "content": _build_prompt(
            keyword, lang, atom_limit, types_batch, include_plan, paa_seed
        )}],
    )
    for block in msg.content:
        if block.type == "tool_use" and block.name == "save_fanout_queries":
            return block.input
    raise ValueError("Claude non ha restituito il tool 'save_fanout_queries'")


def _rerank_queries(keyword, queries, jina_key):
    """Chiama Jina Reranker v3 e restituisce un dict query → score."""
    resp = requests.post(
        "https://api.jina.ai/v1/rerank",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {jina_key}"},
        json={
            "model": "jina-reranker-v3",
            "query": keyword,
            "documents": queries,
            "return_documents": False,
        },
        timeout=60,
    )
    resp.raise_for_status()
    results = resp.json()["results"]
    return {queries[r["index"]]: round(r["relevance_score"], 4) for r in results}


def _enrich(fanout_data, keyword, jina_key, rerank, top_n, w_priority, w_jina):
    """Arricchisce fanout_data con jina_score e combined_score."""
    jina_scores = {}
    if rerank and jina_key:
        all_q = [q["query"] for t in ALL_TYPES for q in fanout_data.get(t, [])]
        if all_q:
            try:
                jina_scores = _rerank_queries(keyword, all_q, jina_key)
            except Exception as e:
                st.warning(f"Jina Reranker non disponibile: {e} — proseguo senza reranking.")
    for t in ALL_TYPES:
        for q in fanout_data.get(t, []):
            js = jina_scores.get(q["query"], 0.0) if jina_scores else None
            q["jina_score"] = js
            if js is not None:
                q["combined_score"] = round((q.get("priority", 3) / 5) * w_priority + js * w_jina, 4)
            else:
                q["combined_score"] = round(q.get("priority", 3) / 5, 4)
        fanout_data[t] = sorted(
            fanout_data.get(t, []),
            key=lambda x: x.get("combined_score", 0),
            reverse=True,
        )
    return fanout_data, jina_scores


def _dedup_cross_type(fanout_data: dict, threshold: float = 0.85) -> dict:
    """Rimuove query semanticamente duplicate tra tipi diversi (Jaccard sui token)."""
    def _tokens(s: str) -> set:
        return set(re.sub(r"[^\w\s]", "", s.lower()).split())

    def _jaccard(a: set, b: set) -> float:
        if not a or not b:
            return 0.0
        return len(a & b) / len(a | b)

    all_q = []
    for tipo, queries in fanout_data.items():
        if tipo == "content_gaps":
            continue
        for i, q in enumerate(queries):
            all_q.append({
                "tipo": tipo,
                "idx": i,
                "tokens": _tokens(q.get("query", "")),
                "priority": q.get("priority", 1),
                "text": q.get("query", ""),
            })

    to_remove = {}
    for i in range(len(all_q)):
        for j in range(i + 1, len(all_q)):
            a, b = all_q[i], all_q[j]
            if a["tipo"] == b["tipo"]:
                continue
            if _jaccard(a["tokens"], b["tokens"]) >= threshold:
                loser = b if a["priority"] >= b["priority"] else a
                to_remove.setdefault(loser["tipo"], set()).add(loser["idx"])

    dedup_count = 0
    new_fanout = {}
    for tipo, queries in fanout_data.items():
        if tipo == "content_gaps":
            new_fanout[tipo] = queries
            continue
        removed = to_remove.get(tipo, set())
        new_fanout[tipo] = [q for i, q in enumerate(queries) if i not in removed]
        dedup_count += len(removed)

    return new_fanout, dedup_count


def _build_module_prompt(t, queries, keyword, lang, max_tokens=700):
    """Prompt per generare un modulo di contenuto passage-ready."""
    meta = TYPE_META[t]
    q_list = "\n".join(
        f'- "{q["query"]}" (score {q.get("combined_score", 0):.3f})'
        for q in queries
    )
    role_label, role_instructions = _TYPE_ROLE.get(t, ("modulo generico", ""))
    return (
        f"Sei un esperto GEO/SEO. Genera un modulo di contenuto passage-ready in Markdown.\n\n"
        f"KEYWORD PRINCIPALE: {keyword}\n"
        f"TIPO SEMANTICO: {meta['label']} — {meta['desc']}\n\n"
        f"SUB-QUERY DA COPRIRE:\n{q_list}\n\n"
        f"RUOLO DI QUESTO MODULO: {role_label}\n\n"
        f"ISTRUZIONI STRUTTURA ({role_label.upper()}):\n{role_instructions}\n\n"
        f"REGOLE:\n"
        f"1. Usa il testo della sub-query come heading H2 o H3\n"
        f"2. Inizia OGNI sezione con una frase DICHIARATIVA diretta (pattern snippet AI Overview)\n"
        f"3. Ogni paragrafo AUTOCONSISTENTE: leggibile e citabile senza contesto\n"
        f"4. Includi dati numerici, specifiche, percentuali dove rilevante\n"
        f"5. Lunghezza: 200-350 parole | Lingua: {lang}\n\n"
        f"Restituisci SOLO il Markdown, senza commenti o prefissi."
    )


def _generate_module(t, queries, keyword, lang, api_key, max_tokens=700):
    """Chiama Claude per generare un singolo modulo di contenuto."""
    client = anthropic.Anthropic(api_key=api_key.strip())
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=max_tokens,
        system=(
            "Sei un esperto GEO/SEO. Generi moduli passage-ready in Markdown. "
            "Ogni modulo è autoconsistente, inizia con frase dichiarativa, "
            "usa la sub-query come heading H2/H3."
        ),
        messages=[{"role": "user", "content": _build_module_prompt(t, queries, keyword, lang, max_tokens)}],
    )
    return t, msg.content[0].text


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS UI — badge e bar
# ══════════════════════════════════════════════════════════════════════════════

def priority_badge(p):
    c = {5: "#c0392b", 4: "#e67e22", 3: "#f1c40f", 2: "#2ecc71", 1: "#95a5a6"}.get(p, "#95a5a6")
    return f'<span style="background:{c};color:#fff;border-radius:8px;padding:1px 7px;font-size:11px;font-weight:700;">P{p}</span>'


def jina_bar(score):
    if score is None:
        return '<span style="font-size:10px;color:#aaa;">—</span>'
    pct = min(int(score * 100), 100)
    col = "#2ecc71" if score > 0.5 else "#f39c12" if score > 0.3 else "#e74c3c"
    return (
        f'<span style="display:inline-flex;align-items:center;gap:4px;font-size:10px;color:#aaa;">'
        f'<span style="display:inline-block;width:44px;height:3px;background:#e8ecef;border-radius:2px;overflow:hidden;">'
        f'<span style="display:block;width:{pct}%;height:100%;background:{col};"></span></span>{score:.3f}</span>'
    )


def gap_badge(is_gap, source=None):
    if source == "paa":
        return '<span style="background:#2980b9;color:#fff;border-radius:6px;padding:1px 6px;font-size:10px;">PAA</span>'
    if is_gap:
        return '<span style="background:#e74c3c;color:#fff;border-radius:6px;padding:1px 6px;font-size:10px;">GAP</span>'
    return '<span style="background:#27ae60;color:#fff;border-radius:6px;padding:1px 6px;font-size:10px;">OK</span>'


# ══════════════════════════════════════════════════════════════════════════════
# HTML REPORT BUILDER (adattato da cella [23])
# ══════════════════════════════════════════════════════════════════════════════

def _score_color(score: float) -> str:
    if score >= 0.75:
        return "#10b981"
    if score >= 0.50:
        return "#f59e0b"
    return "#ef4444"


def _priority_stars(p: int) -> str:
    return "★" * p + "☆" * (5 - p)


def _gap_chip_html(is_gap: bool) -> str:
    if is_gap:
        return '<span style="background:#fee2e2;color:#dc2626;padding:2px 7px;border-radius:10px;font-size:11px;font-weight:600">GAP</span>'
    return '<span style="background:#dcfce7;color:#16a34a;padding:2px 7px;border-radius:10px;font-size:11px;font-weight:600">OK</span>'


def _build_radar_svg(fanout_data, size=260):
    types = [t for t in ALL_TYPES if t in fanout_data and fanout_data[t]]
    if not types:
        return ""
    values = [len(fanout_data[t]) for t in types]
    max_v = max(values) if values else 1
    n = len(types)
    cx = cy = size / 2
    r = size / 2 - 34
    grid_svg = ""
    for level in [0.25, 0.5, 0.75, 1.0]:
        pts = []
        for i in range(n):
            angle = math.pi / 2 + 2 * math.pi * i / n
            pts.append(f"{cx + r * level * math.cos(angle):.1f},{cy - r * level * math.sin(angle):.1f}")
        grid_svg += f'<polygon points="{" ".join(pts)}" fill="none" stroke="#e5e7eb" stroke-width="1"/>'
        for i in range(n):
            angle = math.pi / 2 + 2 * math.pi * i / n
            x1 = cx + r * 0.05 * math.cos(angle)
            y1 = cy - r * 0.05 * math.sin(angle)
            x2 = cx + r * math.cos(angle)
            y2 = cy - r * math.sin(angle)
            grid_svg += f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="#e5e7eb" stroke-width="1"/>'
    poly_pts = []
    lbl_svg = ""
    for i, (t, v) in enumerate(zip(types, values)):
        angle = math.pi / 2 + 2 * math.pi * i / n
        vr = r * (v / max_v)
        poly_pts.append(f"{cx + vr * math.cos(angle):.1f},{cy - vr * math.sin(angle):.1f}")
        xl = cx + (r + 20) * math.cos(angle)
        yl = cy - (r + 20) * math.sin(angle)
        lbl_svg += f'<text x="{xl:.1f}" y="{yl:.1f}" text-anchor="middle" dominant-baseline="middle" font-size="14">{TYPE_META[t]["emoji"]}</text>'
    return (
        f'<svg width="{size}" height="{size}" xmlns="http://www.w3.org/2000/svg">'
        f'{grid_svg}'
        f'<polygon points="{" ".join(poly_pts)}" fill="rgba(99,102,241,0.2)" stroke="#6366f1" stroke-width="2.5"/>'
        f'{lbl_svg}'
        f'</svg>'
    )


def build_html_report(keyword, industry, fanout_data, fanout_modules, paa_questions):
    """Genera un report HTML standalone completo."""
    _now = datetime.now().strftime("%Y-%m-%d %H:%M")
    _all_q = sum(len(v) for k, v in fanout_data.items() if k != "content_gaps")
    _gap_q = sum(1 for k, v in fanout_data.items() if k != "content_gaps" for q in v if q.get("coverage_gap"))
    _cov_pct = round((_all_q - _gap_q) / _all_q * 100) if _all_q else 0
    _paa_count = len(paa_questions)
    _mod_count = len(fanout_modules)

    # Sezione query per tipo
    type_sections = ""
    for tipo in ALL_TYPES:
        queries = fanout_data.get(tipo, [])
        if not queries:
            continue
        color = TYPE_META[tipo]["color"]
        emoji = TYPE_META[tipo]["emoji"]
        n_gap = sum(1 for q in queries if q.get("coverage_gap"))
        gap_pct = round(n_gap / len(queries) * 100) if queries else 0
        rows = ""
        for q in sorted(queries, key=lambda x: x.get("combined_score", x.get("priority", 1) / 5), reverse=True):
            score = q.get("combined_score", round(q.get("priority", 1) / 5, 2))
            rows += (
                f'<tr style="border-bottom:1px solid #f1f5f9">'
                f'<td style="padding:8px 12px;font-size:13px">{q.get("query", "")}</td>'
                f'<td style="padding:8px 12px;text-align:center;font-size:13px;color:#6b7280">{_priority_stars(q.get("priority", 1))}</td>'
                f'<td style="padding:8px 12px;text-align:center">{_gap_chip_html(q.get("coverage_gap", False))}</td>'
                f'<td style="padding:8px 12px;text-align:center"><span style="color:{_score_color(score)};font-weight:700;font-size:13px">{score:.2f}</span></td>'
                f'</tr>'
            )
        type_sections += (
            f'<div style="margin-bottom:28px;border:1px solid #e2e8f0;border-radius:10px;overflow:hidden">'
            f'<div style="background:{color};padding:14px 18px;display:flex;justify-content:space-between;align-items:center">'
            f'<span style="color:white;font-weight:700;font-size:16px">{emoji} {tipo.replace("_", " ").title()}</span>'
            f'<span style="background:rgba(255,255,255,0.25);color:white;padding:3px 10px;border-radius:20px;font-size:12px">{len(queries)} query · {gap_pct}% gap</span>'
            f'</div>'
            f'<table style="width:100%;border-collapse:collapse;background:white">'
            f'<thead><tr style="background:#f8fafc;border-bottom:2px solid #e2e8f0;font-size:12px;color:#64748b;text-transform:uppercase">'
            f'<th style="padding:8px 12px;text-align:left">Sub-query</th>'
            f'<th style="padding:8px 12px">Priority</th>'
            f'<th style="padding:8px 12px">Gap</th>'
            f'<th style="padding:8px 12px">Score</th>'
            f'</tr></thead>'
            f'<tbody>{rows}</tbody></table></div>'
        )

    # Sezione distribuzione
    dist_rows = ""
    for tipo in ALL_TYPES:
        queries = fanout_data.get(tipo, [])
        if not queries:
            continue
        color = TYPE_META[tipo]["color"]
        emoji = TYPE_META[tipo]["emoji"]
        n_gap = sum(1 for q in queries if q.get("coverage_gap"))
        gap_pct = round(n_gap / len(queries) * 100) if queries else 0
        avg_pri = round(sum(q.get("priority", 1) for q in queries) / len(queries), 1)
        avg_sc = round(sum(q.get("combined_score", q.get("priority", 1) / 5) for q in queries) / len(queries), 3)
        bar_cov = 100 - gap_pct
        dist_rows += (
            f'<tr style="border-bottom:1px solid #f1f5f9">'
            f'<td style="padding:9px 14px;font-weight:600;font-size:13px">{emoji} {tipo.replace("_", " ")}</td>'
            f'<td style="padding:9px 14px;text-align:center;font-size:13px">{len(queries)}</td>'
            f'<td style="padding:9px 14px;text-align:center;font-size:13px">{n_gap}</td>'
            f'<td style="padding:9px 14px;min-width:130px">'
            f'<div style="display:flex;border-radius:4px;overflow:hidden;height:14px">'
            f'<div style="width:{bar_cov}%;background:#10b981"></div>'
            f'<div style="width:{gap_pct}%;background:#ef4444"></div></div>'
            f'<span style="font-size:11px;color:#6b7280">{gap_pct}% gap</span></td>'
            f'<td style="padding:9px 14px;text-align:center;font-size:13px">{avg_pri}/5</td>'
            f'<td style="padding:9px 14px;text-align:center;font-weight:700;font-size:13px;color:{_score_color(avg_sc)}">{avg_sc}</td>'
            f'</tr>'
        )

    radar_svg = _build_radar_svg(fanout_data)

    # Sezione moduli contenuto
    modules_section = ""
    if fanout_modules:
        cards = ""
        for tipo, md_content in fanout_modules.items():
            color = TYPE_META.get(tipo, {}).get("color", "#64748b")
            emoji = TYPE_META.get(tipo, {}).get("emoji", "")
            html_content = md_content
            html_content = re.sub(r"^### (.+)$", r"<h3>\1</h3>", html_content, flags=re.MULTILINE)
            html_content = re.sub(r"^## (.+)$",  r"<h2>\1</h2>", html_content, flags=re.MULTILINE)
            html_content = re.sub(r"^# (.+)$",   r"<h1>\1</h1>", html_content, flags=re.MULTILINE)
            html_content = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", html_content)
            html_content = re.sub(r"\*(.+?)\*",     r"<em>\1</em>", html_content)
            html_content = re.sub(r"^- (.+)$",      r"<li>\1</li>", html_content, flags=re.MULTILINE)
            html_content = re.sub(r"^\d+\. (.+)$",  r"<li>\1</li>", html_content, flags=re.MULTILINE)
            html_content = re.sub(r"\n\n", "</p><p>", html_content)
            html_content = f"<p>{html_content}</p>"
            n_words = len(md_content.split())
            cards += (
                f'<div style="margin-bottom:24px;border:1px solid #e2e8f0;border-radius:10px;overflow:hidden">'
                f'<div style="background:{color};padding:12px 18px;display:flex;justify-content:space-between;align-items:center">'
                f'<span style="color:white;font-weight:700;font-size:15px">{emoji} {tipo.replace("_", " ").title()}</span>'
                f'<span style="background:rgba(255,255,255,0.25);color:white;padding:2px 9px;border-radius:20px;font-size:12px">~{n_words} parole</span>'
                f'</div>'
                f'<div style="padding:18px 20px;background:white;font-size:14px;line-height:1.7;color:#374151">{html_content}</div>'
                f'</div>'
            )
        modules_section = (
            f'<section id="modules" style="margin-bottom:40px">'
            f'<h2 style="font-size:20px;font-weight:700;color:#1e293b;border-left:4px solid #6366f1;padding-left:12px;margin-bottom:16px">'
            f'🧩 Moduli Contenuto Passage-Ready ({_mod_count})</h2>{cards}</section>'
        )

    # PAA section
    paa_section = ""
    if paa_questions:
        paa_rows = "".join(
            f'<tr style="border-bottom:1px solid #f1f5f9"><td style="padding:8px 12px;font-size:13px">{q}</td></tr>'
            for q in paa_questions
        )
        paa_section = (
            f'<section id="paa" style="margin-bottom:40px">'
            f'<h2 style="font-size:20px;font-weight:700;color:#1e293b;border-left:4px solid #3b82f6;padding-left:12px;margin-bottom:16px">'
            f'💬 People Also Ask ({_paa_count})</h2>'
            f'<table style="width:100%;border-collapse:collapse;background:white;border-radius:8px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.08)">'
            f'<thead><tr style="background:#f8fafc;border-bottom:2px solid #e2e8f0;font-size:12px;color:#64748b;text-transform:uppercase">'
            f'<th style="padding:8px 12px;text-align:left">Domanda PAA</th></tr></thead>'
            f'<tbody>{paa_rows}</tbody></table></section>'
        )

    full_html = f"""<!DOCTYPE html>
<html lang="it">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Fan-out Report — {keyword}</title>
  <style>
    *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#f1f5f9;color:#1e293b;line-height:1.6}}
    .container{{max-width:1100px;margin:0 auto;padding:32px 20px}}
    table{{border-collapse:collapse}} p{{margin-bottom:.8em}} ul{{padding-left:1.4em;margin-bottom:.8em}} li{{margin-bottom:.3em}}
    nav a{{color:white;text-decoration:none;font-size:13px;padding:4px 10px;border-radius:6px;background:rgba(255,255,255,0.15);white-space:nowrap}}
    nav a:hover{{background:rgba(255,255,255,0.3)}}
  </style>
</head>
<body>
<div style="background:linear-gradient(135deg,#1e293b 0%,#334155 100%);padding:28px 0;margin-bottom:32px">
  <div style="max-width:1100px;margin:0 auto;padding:0 20px">
    <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:16px">
      <div>
        <div style="color:#94a3b8;font-size:13px;margin-bottom:4px">Fan-out Query Report</div>
        <h1 style="color:white;font-size:28px;font-weight:800">{keyword}</h1>
        <div style="color:#94a3b8;font-size:13px;margin-top:6px">
          Settore: <strong style="color:#e2e8f0">{industry}</strong> &nbsp;·&nbsp;
          Generato il: <strong style="color:#e2e8f0">{_now}</strong>
        </div>
      </div>
      <nav style="display:flex;gap:8px;flex-wrap:wrap">
        <a href="#kpi">KPI</a>
        <a href="#distribution">Distribuzione</a>
        <a href="#queries">Sub-query</a>
        {'<a href="#paa">PAA</a>' if paa_questions else ''}
        {'<a href="#modules">Moduli</a>' if fanout_modules else ''}
      </nav>
    </div>
  </div>
</div>
<div class="container">
  <section id="kpi" style="margin-bottom:40px">
    <div style="display:flex;gap:16px;flex-wrap:wrap">
      <div style="flex:1;min-width:140px;background:white;border-radius:10px;padding:20px;box-shadow:0 1px 4px rgba(0,0,0,.08);border-top:4px solid #6366f1">
        <div style="font-size:32px;font-weight:800;color:#6366f1">{_all_q}</div>
        <div style="font-size:13px;color:#64748b;margin-top:2px">Sub-query generate</div>
      </div>
      <div style="flex:1;min-width:140px;background:white;border-radius:10px;padding:20px;box-shadow:0 1px 4px rgba(0,0,0,.08);border-top:4px solid #ef4444">
        <div style="font-size:32px;font-weight:800;color:#ef4444">{_gap_q}</div>
        <div style="font-size:13px;color:#64748b;margin-top:2px">Coverage gap</div>
      </div>
      <div style="flex:1;min-width:140px;background:white;border-radius:10px;padding:20px;box-shadow:0 1px 4px rgba(0,0,0,.08);border-top:4px solid #10b981">
        <div style="font-size:32px;font-weight:800;color:#10b981">{_cov_pct}%</div>
        <div style="font-size:13px;color:#64748b;margin-top:2px">Coverage stimata</div>
      </div>
      <div style="flex:1;min-width:140px;background:white;border-radius:10px;padding:20px;box-shadow:0 1px 4px rgba(0,0,0,.08);border-top:4px solid #3b82f6">
        <div style="font-size:32px;font-weight:800;color:#3b82f6">{_paa_count}</div>
        <div style="font-size:13px;color:#64748b;margin-top:2px">PAA raccolte</div>
      </div>
      <div style="flex:1;min-width:140px;background:white;border-radius:10px;padding:20px;box-shadow:0 1px 4px rgba(0,0,0,.08);border-top:4px solid #8b5cf6">
        <div style="font-size:32px;font-weight:800;color:#8b5cf6">{_mod_count}</div>
        <div style="font-size:13px;color:#64748b;margin-top:2px">Moduli generati</div>
      </div>
    </div>
  </section>
  <section id="distribution" style="margin-bottom:40px">
    <h2 style="font-size:20px;font-weight:700;color:#1e293b;border-left:4px solid #8b5cf6;padding-left:12px;margin-bottom:16px">📊 Distribuzione Semantica</h2>
    <div style="display:flex;gap:20px;flex-wrap:wrap">
      <div style="flex:2;min-width:320px;background:white;border-radius:8px;padding:16px;box-shadow:0 1px 4px rgba(0,0,0,.08)">
        <table style="width:100%;border-collapse:collapse;font-size:13px">
          <thead><tr style="background:#f8fafc;border-bottom:2px solid #e2e8f0;color:#6b7280;font-size:11px;text-transform:uppercase">
            <th style="padding:8px 14px;text-align:left">Tipo</th><th style="padding:8px 14px">Query</th>
            <th style="padding:8px 14px">Gap</th><th style="padding:8px 14px;text-align:left">Coverage</th>
            <th style="padding:8px 14px">Avg Pri</th><th style="padding:8px 14px">Avg Score</th>
          </tr></thead>
          <tbody>{dist_rows}</tbody>
        </table>
      </div>
      <div style="flex:1;min-width:260px;background:white;border-radius:8px;padding:16px;box-shadow:0 1px 4px rgba(0,0,0,.08);text-align:center">
        <div style="font-size:13px;font-weight:600;color:#374151;margin-bottom:8px">Radar — query per tipo</div>
        {radar_svg}
      </div>
    </div>
  </section>
  <section id="queries" style="margin-bottom:40px">
    <h2 style="font-size:20px;font-weight:700;color:#1e293b;border-left:4px solid #f59e0b;padding-left:12px;margin-bottom:16px">🔍 Sub-query per tipo semantico</h2>
    {type_sections}
  </section>
  {paa_section}
  {modules_section}
  <footer style="text-align:center;padding:24px 0;border-top:1px solid #e2e8f0;color:#94a3b8;font-size:12px">
    Fan-out Query Generator · {_now} · keyword: <strong>{keyword}</strong>
  </footer>
</div>
</body>
</html>"""
    return full_html


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.title("Fan-out Generator")
    st.caption("GEO Edition — powered by Claude")

    # ── API Keys ──────────────────────────────────────────────────────────────
    st.subheader("🔑 API Keys")
    anthropic_key = st.text_input(
        "Anthropic API Key",
        type="password",
        placeholder="sk-ant-...",
        help="Obbligatoria — usa claude-sonnet-4-6",
    )
    jina_key = st.text_input(
        "Jina API Key",
        type="password",
        placeholder="jina_...",
        help="Facoltativa — abilita Jina Reranker v3",
    )

    # ── Keyword & Lingua ──────────────────────────────────────────────────────
    st.subheader("✍️ Keyword & Lingua")
    keyword = st.text_input("KEYWORD", placeholder="es. dieta mediterranea")
    fanout_lang = st.selectbox("FANOUT_LANG", ["it", "en", "de", "fr", "es"])
    fanout_industry = st.selectbox(
        "FANOUT_INDUSTRY",
        ["general", "ecommerce", "finance", "b2b_saas", "healthcare", "education"],
    )

    # ── Parametri Fan-out ─────────────────────────────────────────────────────
    st.subheader("⚙️ Parametri Fan-out")
    fanout_atom_limit = st.slider("FANOUT_ATOM_LIMIT", 5, 20, 12)
    fanout_rerank = st.checkbox("FANOUT_RERANK", value=True)
    fanout_rerank_top = st.slider("FANOUT_RERANK_TOP", 3, 12, 6)
    score_w_priority = st.slider(
        "SCORE_WEIGHT_PRIORITY", 0.1, 0.9, 0.6, step=0.1
    )
    score_w_jina = round(1.0 - score_w_priority, 1)
    st.info(f"SCORE_WEIGHT_JINA = {score_w_jina} (auto)")

    # ── DataForSEO ────────────────────────────────────────────────────────────
    with st.expander("📡 DataForSEO (opzionale)"):
        dfs_login = st.text_input("DATAFORSEO_LOGIN", placeholder="user@example.com")
        dfs_password = st.text_input("DATAFORSEO_PASSWORD", type="password")
        paa_location = st.number_input("PAA_LOCATION_CODE", value=2380)
        paa_lang_code = st.selectbox(
            "PAA_LANGUAGE_CODE", ["it", "en", "de", "fr", "es"]
        )
        paa_click_depth = st.slider("PAA_CLICK_DEPTH", 1, 4, 4)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN AREA
# ══════════════════════════════════════════════════════════════════════════════

st.markdown(
    "# 🧭 Fan-out Query Generator — GEO Edition\n"
    "Genera sub-query semantiche per ottimizzare la visibilità su AI Mode, Perplexity e Google AI Overview."
)
st.divider()

tab_fanout, tab_dist, tab_drill, tab_gen, tab_export = st.tabs([
    "🧭 Fan-out",
    "📊 Distribuzione",
    "🔍 Drill-down",
    "🧩 Content Generator",
    "💾 Export",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — FAN-OUT
# ══════════════════════════════════════════════════════════════════════════════

with tab_fanout:
    # ── PAA button ────────────────────────────────────────────────────────────
    col_paa, col_gen = st.columns([1, 3])
    with col_paa:
        paa_btn = st.button(
            "📡 Recupera PAA",
            disabled=not (dfs_login and dfs_password and keyword),
            use_container_width=True,
        )
    with col_gen:
        gen_btn = st.button(
            "🚀 Genera Fan-out",
            type="primary",
            disabled=not (anthropic_key and keyword),
            use_container_width=True,
        )

    # ── Esecuzione PAA ────────────────────────────────────────────────────────
    if paa_btn:
        if not (dfs_login and dfs_password):
            st.warning("Configura DataForSEO nella sidebar per recuperare le PAA.")
        else:
            with st.spinner(f"Recupero PAA per: {keyword!r}..."):
                try:
                    credentials = base64.b64encode(
                        f"{dfs_login}:{dfs_password}".encode()
                    ).decode()
                    payload = [{
                        "keyword":                     keyword,
                        "location_code":               int(paa_location),
                        "language_code":               paa_lang_code,
                        "device":                      "desktop",
                        "os":                          "windows",
                        "depth":                       10,
                        "people_also_ask_click_depth": paa_click_depth,
                    }]
                    resp = requests.post(
                        "https://api.dataforseo.com/v3/serp/google/organic/live/advanced",
                        headers={
                            "Authorization": f"Basic {credentials}",
                            "Content-Type":  "application/json",
                        },
                        json=payload,
                        timeout=60,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    if data.get("status_code") != 20000:
                        st.error(f"DataForSEO: {data.get('status_message', 'errore sconosciuto')}")
                    else:
                        paa_q = []

                        def _extract_paa(items):
                            for item in items or []:
                                if item.get("type") == "people_also_ask_element":
                                    q_txt = (item.get("title") or "").strip()
                                    if q_txt and q_txt not in paa_q:
                                        paa_q.append(q_txt)
                                    _extract_paa(item.get("items") or [])

                        for task in data.get("tasks", []):
                            for result in task.get("result", []):
                                for item in result.get("items", []):
                                    if item.get("type") == "people_also_ask":
                                        _extract_paa(item.get("items") or [])

                        st.session_state.paa_questions = paa_q
                        if paa_q:
                            st.success(f"{len(paa_q)} PAA recuperate e pronte come seed.")
                        else:
                            st.warning("Nessuna PAA trovata per questa keyword.")
                except Exception as e:
                    st.error(f"Errore DataForSEO: {e}")

    # ── Mostra PAA chips se presenti ──────────────────────────────────────────
    if st.session_state.paa_questions:
        chips = "".join(
            f'<span style="background:#ecf0f1;border-radius:20px;padding:5px 12px;'
            f'font-size:12px;color:#2c3e50;margin:3px;display:inline-block;">{q}</span>'
            for q in st.session_state.paa_questions
        )
        st.markdown(
            f'<div style="background:#2c3e50;color:#fff;border-radius:12px;padding:14px 18px;margin-bottom:10px;">'
            f'<b>🔍 People Also Ask — {len(st.session_state.paa_questions)} domande</b></div>'
            f'<div style="display:flex;flex-wrap:wrap;gap:4px;margin-bottom:12px;">{chips}</div>',
            unsafe_allow_html=True,
        )

    # ── Esecuzione Fan-out ────────────────────────────────────────────────────
    if gen_btn:
        if not anthropic_key:
            st.error("Inserisci la ANTHROPIC_API_KEY nella sidebar.")
            st.stop()
        _key = anthropic_key.strip()
        if not _key.startswith("sk-ant-"):
            st.error(
                "**Chiave Anthropic non valida.**\n\n"
                "La chiave deve iniziare con `sk-ant-`. "
                "Generane una nuova su [console.anthropic.com](https://console.anthropic.com/settings/keys)."
            )
            st.stop()
        if not keyword.strip():
            st.error("Inserisci una keyword.")
            st.stop()

        bench = INDUSTRY_BENCHMARKS.get(fanout_industry, INDUSTRY_BENCHMARKS["general"])
        paa_seed = st.session_state.paa_questions

        with st.spinner("Avvio 2 chiamate Claude in parallelo (4 tipi ciascuna)..."):
            t0 = time.time()
            results = {}
            errors = []
            try:
                with ThreadPoolExecutor(max_workers=2) as pool:
                    futures = {
                        pool.submit(
                            _call_claude,
                            keyword, fanout_lang, fanout_atom_limit,
                            TYPES_A, True, anthropic_key, 2000, paa_seed,
                        ): "A",
                        pool.submit(
                            _call_claude,
                            keyword, fanout_lang, fanout_atom_limit,
                            TYPES_B, False, anthropic_key, 2000, paa_seed,
                        ): "B",
                    }
                    for future in as_completed(futures):
                        label = futures[future]
                        try:
                            results[label] = future.result()
                        except Exception as e:
                            errors.append(f"Batch {label}: {e}")
                            results[label] = {}
            except Exception as e:
                st.error(f"Errore API: {e}")
                st.stop()

            if errors:
                for err in errors:
                    err_str = str(err)
                    if "401" in err_str or "authentication_error" in err_str or "invalid x-api-key" in err_str:
                        st.error(
                            f"**Errore autenticazione (401)** — La chiave Anthropic inserita non è valida o è scaduta.\n\n"
                            f"Genera una nuova chiave su [console.anthropic.com](https://console.anthropic.com/settings/keys) "
                            f"e incollala nel campo **Anthropic API Key** nella sidebar.\n\n"
                            f"Dettaglio: `{err_str}`"
                        )
                    else:
                        st.error(f"Errore: {err_str}")

            fanout_data = {**results.get("A", {}), **results.get("B", {})}
            elapsed_claude = round(time.time() - t0, 1)

        # Deduplicazione
        with st.spinner("Deduplicazione cross-tipo..."):
            fanout_data, dedup_count = _dedup_cross_type(fanout_data, threshold=0.85)
            if dedup_count:
                st.info(f"Rimossi {dedup_count} duplicati semantici cross-tipo.")

        # Jina Reranker / enrich
        with st.spinner("Calcolo score..."):
            fanout_data, jina_scores = _enrich(
                fanout_data, keyword, jina_key,
                fanout_rerank and bool(jina_key),
                fanout_rerank_top, score_w_priority, score_w_jina,
            )

        st.session_state.fanout_data = fanout_data

        # Calcolo dist_stats
        dist_stats = {}
        for tipo in ALL_TYPES:
            queries = fanout_data.get(tipo, [])
            if not queries:
                continue
            n_total = len(queries)
            n_gap = sum(1 for q in queries if q.get("coverage_gap"))
            avg_pri = round(sum(q.get("priority", 1) for q in queries) / n_total, 2)
            avg_score = round(sum(q.get("combined_score", q.get("priority", 1) / 5) for q in queries) / n_total, 3)
            dist_stats[tipo] = {
                "emoji": TYPE_META[tipo]["emoji"],
                "color": TYPE_META[tipo]["color"],
                "n_total": n_total,
                "n_gap": n_gap,
                "gap_pct": round(n_gap / n_total * 100),
                "avg_priority": avg_pri,
                "avg_score": avg_score,
            }
        st.session_state.dist_stats = dist_stats
        st.balloons()
        st.success(f"Fan-out completato in {elapsed_claude}s — {sum(len(fanout_data.get(t,[])) for t in ALL_TYPES)} sub-query generate.")

    # ── Display risultati fan-out ──────────────────────────────────────────────
    fanout_data = st.session_state.fanout_data
    if fanout_data:
        total_q = sum(len(fanout_data.get(t, [])) for t in ALL_TYPES)
        gap_q = sum(1 for t in ALL_TYPES for q in fanout_data.get(t, []) if q.get("coverage_gap"))
        covered_q = total_q - gap_q
        coverage_pct = round(covered_q / total_q * 100) if total_q else 0

        bench = INDUSTRY_BENCHMARKS.get(fanout_industry, INDUSTRY_BENCHMARKS["general"])

        # KPI metrics
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.metric("Sub-query totali", total_q)
        kpi2.metric("Gap da colmare", gap_q, delta=f"-{gap_q} opportunità")
        kpi3.metric("Coverage stimata", f"{coverage_pct}%")
        kpi4.metric(
            "Settore benchmark",
            bench["label"],
            delta=f"citation rate {bench['citation_rate']}",
        )

        st.divider()

        # Top N queries
        all_flat = [
            {**q, "type": t}
            for t in ALL_TYPES
            for q in fanout_data.get(t, [])
            if "combined_score" in q
        ]
        top_n = sorted(all_flat, key=lambda x: x["combined_score"], reverse=True)[:fanout_rerank_top]
        top_query_set = {q["query"] for q in top_n}

        if top_n:
            with st.expander(f"⭐ Top {fanout_rerank_top} sub-query per combined score", expanded=True):
                top_rows_html = "".join(
                    f'<tr style="border-bottom:1px solid #2c3e50;">'
                    f'<td style="padding:7px 10px;"><span style="font-size:11px;color:{TYPE_META[q["type"]]["color"]};font-weight:700;">'
                    f'{TYPE_META[q["type"]]["emoji"]} {TYPE_META[q["type"]]["label"]}</span></td>'
                    f'<td style="padding:7px 10px;font-weight:600;font-size:13px;">{q["query"]}</td>'
                    f'<td style="padding:7px 10px;">{gap_badge(q.get("coverage_gap", False), q.get("source"))}</td>'
                    f'<td style="padding:7px 10px;">{priority_badge(q.get("priority", 3))}</td>'
                    f'<td style="padding:7px 10px;">{jina_bar(q.get("jina_score"))}</td>'
                    f'<td style="padding:7px 10px;font-weight:700;color:#f1c40f;">{q["combined_score"]:.3f}</td>'
                    f'</tr>'
                    for q in top_n
                )
                st.markdown(
                    f'<div style="background:#1a252f;color:#fff;border-radius:14px;padding:16px 18px;">'
                    f'<table style="width:100%;border-collapse:collapse;font-size:13px;">'
                    f'<thead><tr style="opacity:.5;border-bottom:1px solid #34495e;">'
                    f'<th style="text-align:left;padding:4px 10px;">Tipo</th>'
                    f'<th style="text-align:left;padding:4px 10px;">Sub-query</th>'
                    f'<th style="padding:4px 10px;">Tag</th><th style="padding:4px 10px;">P</th>'
                    f'<th style="padding:4px 10px;">Jina</th><th style="padding:4px 10px;">Score</th>'
                    f'</tr></thead><tbody>{top_rows_html}</tbody></table></div>',
                    unsafe_allow_html=True,
                )

        # Expander per ogni tipo
        for t, meta in TYPE_META.items():
            queries = fanout_data.get(t, [])
            if not queries:
                continue
            type_gap = sum(1 for q in queries if q.get("coverage_gap"))
            type_gap_pct = round(type_gap / len(queries) * 100)
            with st.expander(
                f"{meta['emoji']} {meta['label']} — {len(queries)} query · {type_gap_pct}% gap",
                expanded=False,
            ):
                rows_html = "".join(
                    f'<tr style="background:{"#fffbea" if q["query"] in top_query_set else "#fff5f5" if q.get("coverage_gap") else ("#fff" if i%2==0 else "#f8f9fa")};border-bottom:1px solid #eaecef;">'
                    f'<td style="padding:9px 14px;font-weight:{"800" if q["query"] in top_query_set else "600"};font-size:13px;color:#2c3e50;">'
                    f'{q["query"]}{"  ★" if q["query"] in top_query_set else ""}</td>'
                    f'<td style="padding:9px 10px;text-align:center;">{gap_badge(q.get("coverage_gap", False), q.get("source"))}</td>'
                    f'<td style="padding:9px 10px;text-align:center;">{priority_badge(q.get("priority", 3))}</td>'
                    f'<td style="padding:9px 10px;text-align:center;">{jina_bar(q.get("jina_score"))}</td>'
                    f'<td style="padding:9px 10px;text-align:center;font-weight:700;color:#b45309;">'
                    f'{"—" if q.get("combined_score") is None else str(round(q.get("combined_score", 0), 3))}</td>'
                    f'</tr>'
                    for i, q in enumerate(queries)
                )
                st.markdown(
                    f'<div style="border:2px solid {meta["color"]};border-radius:12px;overflow:hidden;">'
                    f'<div style="font-size:11px;color:#7f8c8d;padding:6px 14px;background:{meta["color"]}11;">{meta["desc"]}</div>'
                    f'<table style="width:100%;border-collapse:collapse;font-size:13px;">'
                    f'<thead><tr style="background:{meta["color"]}22;border-bottom:2px solid {meta["color"]};">'
                    f'<th style="text-align:left;padding:9px 14px;font-weight:600;color:#2c3e50;width:60%;">Sub-query</th>'
                    f'<th style="text-align:center;padding:9px 10px;font-weight:600;color:#2c3e50;width:10%;">Tag</th>'
                    f'<th style="text-align:center;padding:9px 10px;font-weight:600;color:#2c3e50;width:10%;">P</th>'
                    f'<th style="text-align:center;padding:9px 10px;font-weight:600;color:#2c3e50;width:10%;">Jina</th>'
                    f'<th style="text-align:center;padding:9px 10px;font-weight:600;color:#2c3e50;width:10%;">Score</th>'
                    f'</tr></thead><tbody>{rows_html}</tbody></table></div>',
                    unsafe_allow_html=True,
                )

        # Content gaps plan
        gaps_plan = fanout_data.get("content_gaps", [])
        if gaps_plan:
            elem_colors = {
                "definizione": "#3498db", "lista": "#27ae60", "tabella": "#9b59b6",
                "faq": "#e67e22", "comparativa": "#c0392b", "scheda-tecnica": "#1abc9c",
            }
            rows_gaps = "".join(
                f'<tr style="background:{"rgba(255,255,255,.05)" if i%2==0 else "transparent"}">'
                f'<td style="padding:7px 10px;font-family:monospace;font-size:11px;color:#74b9ff;">/{row.get("url_slug","")}</td>'
                f'<td style="padding:7px 10px;"><span style="font-size:11px;color:{TYPE_META.get(row.get("covers_type",""),{}).get("color","#aaa")};">'
                f'{TYPE_META.get(row.get("covers_type",""),{}).get("emoji","")} {TYPE_META.get(row.get("covers_type",""),{}).get("label","")}</span></td>'
                f'<td style="padding:7px 10px;font-size:12px;">{row.get("query_target","")}</td>'
                f'<td style="padding:7px 10px;"><span style="background:{elem_colors.get(row.get("content_element",""),"#555")};color:#fff;border-radius:6px;padding:2px 7px;font-size:11px;">{row.get("content_element","")}</span></td>'
                f'</tr>'
                for i, row in enumerate(gaps_plan)
            )
            st.markdown(
                f'<div style="border:2px solid #e74c3c;border-radius:14px;padding:16px 18px;margin-top:12px;background:#1a252f;color:#fff;">'
                f'<div style="font-weight:700;font-size:14px;margin-bottom:10px;color:#e74c3c;">📋 Content Gaps — Pagine da creare</div>'
                f'<table style="width:100%;border-collapse:collapse;font-size:13px;">'
                f'<thead><tr style="opacity:.5;border-bottom:1px solid #34495e;">'
                f'<th style="text-align:left;padding:4px 10px;">URL slug</th>'
                f'<th style="text-align:left;padding:4px 10px;">Tipo fan-out</th>'
                f'<th style="text-align:left;padding:4px 10px;">Query target</th>'
                f'<th style="text-align:left;padding:4px 10px;">Elemento</th>'
                f'</tr></thead><tbody>{rows_gaps}</tbody></table></div>',
                unsafe_allow_html=True,
            )
    else:
        st.info("Configura la sidebar e clicca **Genera Fan-out** per iniziare.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — DISTRIBUZIONE
# ══════════════════════════════════════════════════════════════════════════════

with tab_dist:
    fanout_data = st.session_state.fanout_data
    dist_stats = st.session_state.dist_stats

    if not fanout_data:
        st.info("Genera il fan-out prima di visualizzare la distribuzione.")
    else:
        total_generated = sum(s["n_total"] for s in dist_stats.values())
        total_gap = sum(s["n_gap"] for s in dist_stats.values())
        coverage_pct_dist = round((total_generated - total_gap) / total_generated * 100) if total_generated else 0

        _BENCH_AVG = {
            "general": 13, "ecommerce": 20, "finance": 18,
            "b2b_saas": 16, "healthcare": 25, "education": 14,
        }
        bench_avg = _BENCH_AVG.get(fanout_industry, 13)
        bench_delta = total_generated - bench_avg * len(dist_stats)
        bench_sign = "+" if bench_delta >= 0 else ""

        # KPI
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Query generate", total_generated)
        col2.metric("Coverage gap", total_gap)
        col3.metric("Coverage stimata", f"{coverage_pct_dist}%")
        col4.metric(f"vs benchmark {fanout_industry}", f"{bench_sign}{bench_delta}")

        st.divider()

        # Tabella distribuzione
        st.subheader("Distribuzione per tipo semantico")
        rows_dist = ""
        for tipo, s in dist_stats.items():
            bar_gap = s["gap_pct"]
            bar_cov = 100 - bar_gap
            rows_dist += (
                f'<tr>'
                f'<td style="padding:8px 12px;font-weight:600">{s["emoji"]} {tipo}</td>'
                f'<td style="padding:8px 12px;text-align:center">{s["n_total"]}</td>'
                f'<td style="padding:8px 12px;text-align:center">{s["n_gap"]}</td>'
                f'<td style="padding:8px 12px;min-width:120px">'
                f'<div style="display:flex;border-radius:4px;overflow:hidden;height:16px">'
                f'<div style="width:{bar_cov}%;background:#10b981"></div>'
                f'<div style="width:{bar_gap}%;background:#ef4444"></div>'
                f'</div><span style="font-size:11px;color:#6b7280">{bar_gap}% gap</span></td>'
                f'<td style="padding:8px 12px;text-align:center">{s["avg_priority"]:.1f}/5</td>'
                f'<td style="padding:8px 12px;text-align:center">{s["avg_score"]:.3f}</td>'
                f'</tr>'
            )
        st.markdown(
            f'<div style="background:white;border-radius:8px;padding:16px;box-shadow:0 1px 3px rgba(0,0,0,.08)">'
            f'<table style="width:100%;border-collapse:collapse;font-size:13px">'
            f'<thead><tr style="border-bottom:2px solid #e5e7eb;color:#6b7280">'
            f'<th style="padding:8px 12px;text-align:left">Tipo</th>'
            f'<th style="padding:8px 12px">Query</th>'
            f'<th style="padding:8px 12px">Gap</th>'
            f'<th style="padding:8px 12px;text-align:left">Coverage</th>'
            f'<th style="padding:8px 12px">Avg Priority</th>'
            f'<th style="padding:8px 12px">Avg Score</th>'
            f'</tr></thead><tbody>{rows_dist}</tbody></table></div>',
            unsafe_allow_html=True,
        )

        st.divider()

        # Radar SVG
        col_radar, col_heat = st.columns(2)
        with col_radar:
            st.subheader("Radar — query per tipo")
            labels = list(dist_stats.keys())
            n_sides = len(labels)
            values = [s["n_total"] for s in dist_stats.values()]
            max_val = max(values) if values else 1
            cx, cy, r = 160, 160, 120
            svg_poly_pts, svg_lbl = [], []
            for i, (lbl, val) in enumerate(zip(labels, values)):
                angle = math.pi / 2 + 2 * math.pi * i / n_sides
                vr = r * (val / max_val)
                svg_poly_pts.append(f"{cx + vr * math.cos(angle):.1f},{cy - vr * math.sin(angle):.1f}")
                xl = cx + (r + 22) * math.cos(angle)
                yl = cy - (r + 22) * math.sin(angle)
                svg_lbl.append(
                    f'<text x="{xl:.1f}" y="{yl:.1f}" text-anchor="middle" dominant-baseline="middle" font-size="13">'
                    f'{dist_stats[lbl]["emoji"]}</text>'
                )
            svg_grid = ""
            for level in [0.25, 0.5, 0.75, 1.0]:
                pts = []
                for i in range(n_sides):
                    angle = math.pi / 2 + 2 * math.pi * i / n_sides
                    pts.append(f"{cx + r * level * math.cos(angle):.1f},{cy - r * level * math.sin(angle):.1f}")
                svg_grid += f'<polygon points="{" ".join(pts)}" fill="none" stroke="#e5e7eb" stroke-width="1"/>'
            radar_svg = (
                f'<svg width="320" height="320" xmlns="http://www.w3.org/2000/svg">'
                f'{svg_grid}'
                f'<polygon points=\'{" ".join(svg_poly_pts)}\' fill="rgba(99,102,241,0.25)" stroke="#6366f1" stroke-width="2"/>'
                f'{"".join(svg_lbl)}'
                f'</svg>'
            )
            st.markdown(radar_svg, unsafe_allow_html=True)

        with col_heat:
            st.subheader("Heatmap gap × priority")
            heatmap_rows = ""
            for tipo, s in dist_stats.items():
                gap_norm = s["gap_pct"] / 100
                r_val = int(239 * gap_norm)
                g_val = int(68 + 120 * (1 - gap_norm))
                b_val = 68
                cell_color = f"rgb({r_val},{g_val},{b_val})"
                heatmap_rows += (
                    f'<tr>'
                    f'<td style="padding:6px 10px;font-size:13px">{s["emoji"]} {tipo}</td>'
                    f'<td style="padding:6px 10px;text-align:center;background:{cell_color};color:white;border-radius:4px;font-weight:700">{s["gap_pct"]}%</td>'
                    f'<td style="padding:6px 10px;text-align:center">{s["avg_priority"]:.1f}/5</td>'
                    f'</tr>'
                )
            st.markdown(
                f'<div style="background:white;border-radius:8px;padding:16px;box-shadow:0 1px 3px rgba(0,0,0,.08)">'
                f'<table style="width:100%;border-collapse:collapse;font-size:13px">'
                f'<thead><tr style="border-bottom:2px solid #e5e7eb;color:#6b7280">'
                f'<th style="padding:6px 10px;text-align:left">Tipo</th>'
                f'<th style="padding:6px 10px">Gap %</th>'
                f'<th style="padding:6px 10px">Avg Pri</th>'
                f'</tr></thead><tbody>{heatmap_rows}</tbody></table>'
                f'<div style="margin-top:8px;font-size:11px;color:#6b7280">🔴 Gap alto + priority alta = priorità contenuto massima</div>'
                f'</div>',
                unsafe_allow_html=True,
            )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — DRILL-DOWN
# ══════════════════════════════════════════════════════════════════════════════

with tab_drill:
    fanout_data = st.session_state.fanout_data

    if not fanout_data:
        st.info("Genera il fan-out prima di eseguire il drill-down.")
    else:
        available_types = [t for t in ALL_TYPES if fanout_data.get(t)]
        col_dd1, col_dd2, col_dd3 = st.columns(3)
        with col_dd1:
            drill_type = st.selectbox("DRILL_TYPE", available_types)
        with col_dd2:
            drill_count = st.slider("DRILL_COUNT", 5, 30, 20)
        with col_dd3:
            drill_only_gaps = st.checkbox("DRILL_ONLY_GAPS", value=True)

        drill_btn = st.button("🔍 Esegui Drill-down", type="primary")

        if drill_btn:
            if not anthropic_key:
                st.error("Inserisci la ANTHROPIC_API_KEY nella sidebar.")
            else:
                meta = TYPE_META[drill_type]
                drilldown_prompt = (
                    f"Sei un esperto GEO/SEO specializzato in AI Query Fan-out.\n\n"
                    f"Esegui un drill-down approfondito sul tipo semantico '{meta['label']}' "
                    f"per la keyword: {keyword}\n\n"
                    f"DEFINIZIONE DEL TIPO:\n{meta['desc']}\n"
                    f"ESEMPIO: {meta['example']}\n\n"
                    f"ISTRUZIONI:\n"
                    f"1. Genera ESATTAMENTE {drill_count} sub-query di tipo '{meta['label']}'\n"
                    f"2. Vai in profondità: esplora angolazioni insolite, long-tail, varianti di nicchia\n"
                    f"3. Per ogni query specifica:\n"
                    f'   - "query": stringa concisa in {fanout_lang}\n'
                    f'   - "priority": 1-5 (probabilità che AI Mode la generi)\n'
                    f'   - "coverage_gap": true se raramente coperta, false se comune\n'
                    f'   - "rationale": 1 frase — perché un motore AI genererebbe questa query\n\n'
                    f"Usa tool_use 'save_drill_queries'."
                )
                with st.spinner(f"Drill-down su {meta['emoji']} {meta['label']}..."):
                    try:
                        client = anthropic.Anthropic(api_key=anthropic_key.strip())
                        msg = client.messages.create(
                            model="claude-sonnet-4-6",
                            max_tokens=1500,
                            system="Sei un esperto GEO/SEO specializzato in AI Query Fan-out.",
                            tools=[DRILL_TOOL_SCHEMA],
                            tool_choice={"type": "tool", "name": "save_drill_queries"},
                            messages=[{"role": "user", "content": drilldown_prompt}],
                        )
                        drill_result = []
                        for block in msg.content:
                            if block.type == "tool_use" and block.name == "save_drill_queries":
                                drill_result = block.input.get("queries", [])
                                break

                        # Jina rerank opzionale
                        if jina_key and drill_result:
                            dd_q = [q["query"] for q in drill_result]
                            try:
                                dd_sc = _rerank_queries(keyword, dd_q, jina_key)
                                for q in drill_result:
                                    js = dd_sc.get(q["query"], 0.0)
                                    q["jina_score"] = js
                                    q["combined_score"] = round((q.get("priority", 3) / 5) * 0.6 + js * 0.4, 4)
                                drill_result = sorted(drill_result, key=lambda x: x.get("combined_score", 0), reverse=True)
                            except Exception as e:
                                st.warning(f"Jina skip: {e}")
                                for q in drill_result:
                                    q["jina_score"] = None
                                    q["combined_score"] = round(q.get("priority", 3) / 5, 4)
                        else:
                            for q in drill_result:
                                q["jina_score"] = None
                                q["combined_score"] = round(q.get("priority", 3) / 5, 4)

                        st.session_state.drill_results = drill_result

                        # Aggiorna fanout_data con nuove query
                        existing = {q["query"] for q in st.session_state.fanout_data.get(drill_type, [])}
                        new_entries = [q for q in drill_result if q["query"] not in existing]
                        st.session_state.fanout_data.setdefault(drill_type, []).extend(new_entries)
                        st.session_state.fanout_data[drill_type] = sorted(
                            st.session_state.fanout_data[drill_type],
                            key=lambda x: x.get("combined_score", 0),
                            reverse=True,
                        )
                        st.success(f"{len(drill_result)} generate · {len(new_entries)} nuove aggiunte a fanout_data.")
                    except Exception as e:
                        st.error(f"Errore API drill-down: {e}")

        # Mostra risultati drill-down
        drill_results = st.session_state.drill_results
        if drill_results:
            meta = TYPE_META.get(drill_type, TYPE_META["follow_up"])
            filtered = [q for q in drill_results if q.get("coverage_gap")] if drill_only_gaps else drill_results
            n_gaps = sum(1 for q in filtered if q.get("coverage_gap"))

            col_a, col_b, col_c = st.columns(3)
            col_a.metric("Generate", len(drill_results))
            col_b.metric("Mostrate", len(filtered))
            col_c.metric("Gap", n_gaps)

            rows_drill = ""
            for i, q in enumerate(filtered):
                bg = "#fff5f5" if q.get("coverage_gap") else ("#fff" if i % 2 == 0 else "#f8f9fa")
                cs = q.get("combined_score", 0)
                rows_drill += (
                    f'<tr style="background:{bg};border-bottom:1px solid #eaecef;">'
                    f'<td style="padding:9px 14px;font-weight:600;">{q["query"]}</td>'
                    f'<td style="padding:9px 10px;text-align:center;">{gap_badge(q.get("coverage_gap", False))}</td>'
                    f'<td style="padding:9px 10px;text-align:center;">{priority_badge(q.get("priority", 3))}</td>'
                    f'<td style="padding:9px 10px;text-align:center;font-weight:700;color:#b45309;">{cs:.3f}</td>'
                    f'<td style="padding:9px 14px;font-size:11px;color:#7f8c8d;">{q.get("rationale", "")}</td>'
                    f'</tr>'
                )
            st.markdown(
                f'<div style="border:2px solid {meta["color"]};border-radius:14px;overflow:hidden;margin-top:12px;">'
                f'<div style="background:{meta["color"]};color:#fff;padding:12px 18px;font-weight:700;font-size:14px;">'
                f'{meta["emoji"]} {meta["label"]} — Drill-down</div>'
                f'<table style="width:100%;border-collapse:collapse;font-size:13px;">'
                f'<thead><tr style="background:{meta["color"]}22;border-bottom:2px solid {meta["color"]};">'
                f'<th style="text-align:left;padding:9px 14px;width:50%;">Sub-query</th>'
                f'<th style="padding:9px 10px;width:8%;">Gap</th>'
                f'<th style="padding:9px 10px;width:8%;">P</th>'
                f'<th style="padding:9px 10px;width:8%;">Score</th>'
                f'<th style="text-align:left;padding:9px 14px;font-size:11px;width:26%;">Rationale</th>'
                f'</tr></thead><tbody>{rows_drill}</tbody></table></div>',
                unsafe_allow_html=True,
            )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — CONTENT GENERATOR
# ══════════════════════════════════════════════════════════════════════════════

with tab_gen:
    fanout_data = st.session_state.fanout_data

    if not fanout_data:
        st.info("Genera il fan-out prima di usare il Content Generator.")
    else:
        col_g1, col_g2, col_g3 = st.columns(3)
        with col_g1:
            gen_min_score = st.slider("FANOUT_GEN_MIN_SCORE", 0.1, 0.9, 0.25, step=0.05)
        with col_g2:
            gen_only_gaps = st.checkbox("FANOUT_GEN_ONLY_GAPS", value=True)
        with col_g3:
            gen_top_per_type = st.slider("FANOUT_GEN_TOP_PER_TYPE", 1, 5, 3)

        gen_modules_btn = st.button("🧩 Genera Moduli", type="primary")

        # Selezione query
        selected = {}
        for t in ALL_TYPES:
            qs = fanout_data.get(t, [])
            filtered = [
                q for q in qs
                if (not gen_only_gaps or q.get("coverage_gap", False))
                and q.get("combined_score", 0) >= gen_min_score
            ]
            top = sorted(filtered, key=lambda x: x.get("combined_score", 0), reverse=True)[:gen_top_per_type]
            if top:
                selected[t] = top

        total_sel = sum(len(v) for v in selected.values())
        st.info(f"Moduli da generare: **{len(selected)}** tipi · **{total_sel}** query totali · score >= {gen_min_score}, only_gap = {gen_only_gaps}")

        if gen_modules_btn:
            if not anthropic_key:
                st.error("Inserisci la ANTHROPIC_API_KEY nella sidebar.")
            elif not selected:
                st.warning("Nessuna query selezionata. Riduci FANOUT_GEN_MIN_SCORE o disattiva FANOUT_GEN_ONLY_GAPS.")
            else:
                prog = st.progress(0, text="Generazione moduli in parallelo...")
                fanout_modules_new = {}
                errors_gen = []

                with ThreadPoolExecutor(max_workers=4) as pool:
                    futures = {
                        pool.submit(
                            _generate_module,
                            t, qs, keyword, fanout_lang, anthropic_key, 700,
                        ): t
                        for t, qs in selected.items()
                    }
                    done = 0
                    for future in as_completed(futures):
                        t_key = futures[future]
                        try:
                            _, content = future.result()
                            fanout_modules_new[t_key] = content
                        except Exception as exc:
                            errors_gen.append(f"{TYPE_META[t_key]['emoji']} {TYPE_META[t_key]['label']}: {exc}")
                        done += 1
                        prog.progress(done / len(futures), text=f"Completati {done}/{len(futures)} moduli...")

                prog.empty()
                if errors_gen:
                    for err in errors_gen:
                        st.error(f"Errore modulo: {err}")

                st.session_state.fanout_modules = fanout_modules_new

                # Documento combinato
                combined = (
                    f"# GEO Fan-out Modules — {keyword}\n\n"
                    f"> Moduli passage-ready per le sub-query con coverage gap.\n"
                    f"> Ogni sezione è autoconsistente e citabile da AI Overview indipendentemente.\n\n"
                    f"---\n\n"
                )
                for t in ALL_TYPES:
                    if t in fanout_modules_new:
                        meta = TYPE_META[t]
                        combined += f"<!-- {meta['emoji']} {meta['label']} -->\n\n"
                        combined += fanout_modules_new[t] + "\n\n---\n\n"
                st.session_state.geo_fanout_combined = combined
                st.success(f"{len(fanout_modules_new)} moduli generati.")

        # Mostra moduli
        fanout_modules = st.session_state.fanout_modules
        if fanout_modules:
            for t in ALL_TYPES:
                if t not in fanout_modules:
                    continue
                meta = TYPE_META[t]
                content = fanout_modules[t]
                qs = selected.get(t, [])
                word_est = len(content.split())
                with st.expander(
                    f"{meta['emoji']} {meta['label']} — ~{word_est} parole",
                    expanded=False,
                ):
                    q_chips = "".join(
                        f'<span style="display:inline-block;background:#fff;border:1px solid {meta["color"]}44;'
                        f'border-radius:20px;padding:3px 10px;font-size:11px;color:#2c3e50;margin:2px 3px;">'
                        f'{"⬛ " if q.get("coverage_gap") else "✅ "}{q["query"]}'
                        f'<span style="color:{meta["color"]};font-size:10px;margin-left:4px;">{q.get("combined_score",0):.2f}</span></span>'
                        for q in qs
                    ) if qs else ""
                    if q_chips:
                        st.markdown(
                            f'<div style="margin-bottom:8px;">{q_chips}</div>',
                            unsafe_allow_html=True,
                        )
                    st.markdown(content)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — EXPORT
# ══════════════════════════════════════════════════════════════════════════════

with tab_export:
    fanout_data = st.session_state.fanout_data
    fanout_modules = st.session_state.fanout_modules
    paa_questions = st.session_state.paa_questions
    geo_fanout_combined = st.session_state.geo_fanout_combined

    if not fanout_data:
        st.info("Genera il fan-out prima di esportare.")
    else:
        slug = re.sub(r"[^a-z0-9]+", "_", (keyword or "fanout").lower()).strip("_")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        base = f"fanout_{slug}_{timestamp}"

        st.subheader("💾 Esporta i risultati")
        total_q = sum(len(fanout_data.get(t, [])) for t in ALL_TYPES)
        gap_q = sum(1 for t in ALL_TYPES for q in fanout_data.get(t, []) if q.get("coverage_gap"))
        paa_count = len(paa_questions)
        mod_count = len(fanout_modules)

        col_e1, col_e2, col_e3, col_e4 = st.columns(4)
        col_e1.metric("Sub-query", total_q)
        col_e2.metric("Gap", gap_q)
        col_e3.metric("PAA", paa_count)
        col_e4.metric("Moduli", mod_count)

        st.divider()

        # ── CSV ────────────────────────────────────────────────────────────────
        csv_buf = io.StringIO()
        writer = csv.DictWriter(csv_buf, fieldnames=[
            "type", "type_label", "query", "priority",
            "coverage_gap", "source", "jina_score", "combined_score",
        ])
        writer.writeheader()
        for t in ALL_TYPES:
            for q in fanout_data.get(t, []):
                writer.writerow({
                    "type":           t,
                    "type_label":     TYPE_META[t]["label"],
                    "query":          q.get("query", ""),
                    "priority":       q.get("priority", ""),
                    "coverage_gap":   q.get("coverage_gap", ""),
                    "source":         q.get("source", "claude"),
                    "jina_score":     q.get("jina_score", ""),
                    "combined_score": q.get("combined_score", ""),
                })

        st.download_button(
            label="📥 Download CSV",
            data=csv_buf.getvalue().encode("utf-8"),
            file_name=f"{base}.csv",
            mime="text/csv",
            use_container_width=True,
        )

        # ── JSON ───────────────────────────────────────────────────────────────
        json_payload = {
            "keyword":        keyword,
            "generated_at":   datetime.now().isoformat(),
            "fanout_data":    fanout_data,
            "fanout_modules": fanout_modules,
        }
        json_str = json.dumps(json_payload, ensure_ascii=False, indent=2)

        st.download_button(
            label="📥 Download JSON",
            data=json_str.encode("utf-8"),
            file_name=f"{base}.json",
            mime="application/json",
            use_container_width=True,
        )

        # ── Markdown ───────────────────────────────────────────────────────────
        if geo_fanout_combined:
            md_content = geo_fanout_combined
        else:
            md_content = (
                f"# Fan-out Queries — {keyword}\n\n"
                + "\n\n".join(
                    f"## {TYPE_META[t]['emoji']} {TYPE_META[t]['label']}\n\n"
                    + "\n".join(f"- {q['query']}" for q in fanout_data.get(t, []))
                    for t in ALL_TYPES
                    if fanout_data.get(t)
                )
            )

        st.download_button(
            label="📥 Download Markdown",
            data=md_content.encode("utf-8"),
            file_name=f"{base}_modules.md",
            mime="text/markdown",
            use_container_width=True,
        )

        # ── HTML Report ────────────────────────────────────────────────────────
        html_report = build_html_report(
            keyword=keyword,
            industry=fanout_industry,
            fanout_data=fanout_data,
            fanout_modules=fanout_modules,
            paa_questions=paa_questions,
        )
        size_kb = round(len(html_report.encode("utf-8")) / 1024, 1)

        st.download_button(
            label=f"📥 Download HTML Report ({size_kb} KB)",
            data=html_report.encode("utf-8"),
            file_name=f"{base}.html",
            mime="text/html",
            use_container_width=True,
        )

        st.caption(
            f"File pronti per il download — keyword: **{keyword}** · "
            f"{total_q} sub-query · {gap_q} gap · {mod_count} moduli"
        )
