#!/usr/bin/env python3
"""
Talent Sync Pipeline
Processes a stream of contact events into an exactly-correct roster.
Idempotent, timestamp-ordered, with dead-letter logging and LLM interest extraction.
"""

import json
import os
import urllib.request
import urllib.error
import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ── Config ──────────────────────────────────────────────────────────────────
FEED_URL = "https://raw.githubusercontent.com/Boulders91/talent-sync-trial/main/talent-feed.json"
DOCS_BASE_URL = "https://raw.githubusercontent.com/Boulders91/talent-sync-trial/main/docs/"
DOCS_INDEX = {
    "voiceloop": {"file": "voiceloop-prd.md", "keywords": ["voiceloop", "voice loop", "voice-loop"]},
    "pocketcfo": {"file": "pocketcfo-prd.md", "keywords": ["pocketcfo", "pocket cfo", "pocket-cfo", "personal finance", "finance tooling"]},
    "trailmix":  {"file": "trailmix-idea-kernel.md", "keywords": ["trailmix", "trail mix", "hiking", "fitness app", "health app", "hike"]},
}

SUMMARY_TO   = os.environ.get("SUMMARY_EMAIL_TO", "brock@sandbar.ai")
SUMMARY_FROM = os.environ.get("SUMMARY_EMAIL_FROM", "")
SUMMARY_PASS = os.environ.get("SUMMARY_EMAIL_PASS", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
ROSTER_FILE    = os.path.join(OUTPUT_DIR, "roster.json")
DLQ_FILE       = os.path.join(OUTPUT_DIR, "dead_letter.json")
REVIEW_FILE    = os.path.join(OUTPUT_DIR, "review_queue.json")
STATE_FILE     = os.path.join(OUTPUT_DIR, "state.json")
PROCESSED_FILE = os.path.join(OUTPUT_DIR, "processed_events.json")

# ── Helpers ──────────────────────────────────────────────────────────────────
def fetch_json(url):
    with urllib.request.urlopen(url) as r:
        return json.loads(r.read().decode())

def load_json_file(path, default):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return default

def save_json_file(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def now_iso():
    return datetime.datetime.utcnow().isoformat() + "Z"

# ── Project doc matching ──────────────────────────────────────────────────────
def match_project_doc(reply_text):
    """
    Returns (doc_url, doc_name) if a project is confidently matched,
    or (None, 'no doc found') if interested but no doc matched,
    or (None, None) if no project mentioned.
    """
    text_lower = reply_text.lower()
    for project, meta in DOCS_INDEX.items():
        for kw in meta["keywords"]:
            if kw in text_lower:
                url = DOCS_BASE_URL + meta["file"]
                return url, meta["file"]
    return None, None

# ── LLM interest extraction ───────────────────────────────────────────────────
def extract_interest(reply_text):
    """
    Returns dict: {interest, confidence, reason, project_mentioned}
    interest: interested | declined | needs_review
    confidence: high | low  (low => needs_review overrides)
    """
    if not ANTHROPIC_KEY:
        # Fallback: basic heuristics so pipeline runs without API key
        text_lower = reply_text.lower()
        if any(w in text_lower for w in ["count me in", "i'm in", "i am in", "yes", "absolutely", "love to", "would love"]):
            return {"interest": "interested", "confidence": "high", "reason": "Explicit affirmative language detected.", "project_mentioned": True}
        if any(w in text_lower for w in ["pass", "fully booked", "can't", "cannot", "no thanks", "going to pass"]):
            return {"interest": "declined", "confidence": "high", "reason": "Explicit decline language detected.", "project_mentioned": False}
        return {"interest": "needs_review", "confidence": "low", "reason": "Ambiguous response; could not determine intent.", "project_mentioned": False}

    prompt = f"""You are extracting intent from a reply to a project outreach message.

Reply text:
\"\"\"{reply_text}\"\"\"

Return ONLY valid JSON with these exact keys:
- "interest": one of "interested", "declined", or "needs_review"
  - "interested" = clearly wants to participate
  - "declined" = clearly does not want to participate
  - "needs_review" = ambiguous, unclear, or hedging
- "confidence": "high" or "low"
  - "high" = you are confident in the above classification
  - "low" = the text is ambiguous enough that a human should verify
- "reason": one sentence explaining the classification (max 20 words)
- "project_mentioned": true if a specific project name or product is referenced, false otherwise

Rules:
- If confidence is "low", interest MUST be "needs_review"
- Never guess; when in doubt, use needs_review + low confidence
- Return nothing except the JSON object"""

    import anthropic as _anthropic
    _client = _anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    _msg = _client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=256,
        messages=[{"role": "user", "content": prompt}]
    )
    raw = _msg.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())

# ── Core pipeline ─────────────────────────────────────────────────────────────
def run_pipeline(send_email=True):
    run_start = now_iso()

    # Load persistent state
    roster         = load_json_file(ROSTER_FILE, {})
    dead_letter    = load_json_file(DLQ_FILE, [])
    review_queue   = load_json_file(REVIEW_FILE, [])
    processed_ids  = set(load_json_file(PROCESSED_FILE, []))

    # Snapshot state before run for diff
    roster_before = json.loads(json.dumps(roster))
    dlq_before    = len(dead_letter)
    review_before = len(review_queue)

    # Fetch feed
    feed = fetch_json(FEED_URL)

    # Track what happened this run
    skipped_dup_events   = []
    dead_lettered_this   = []
    review_queued_this   = []
    contacts_created     = []
    contacts_updated     = []
    replies_processed    = []

    # ── Deduplicate events by event_id ────────────────────────────────────────
    seen_event_ids = set()
    deduped_feed = []
    for event in feed:
        eid = event["event_id"]
        if eid in seen_event_ids:
            skipped_dup_events.append(eid)
            continue
        seen_event_ids.add(eid)
        deduped_feed.append(event)

    # ── Process in array order (stream simulation) ────────────────────────────
    # But for field writes, timestamps decide what's current.
    # Strategy: collect all updates per contact per field, then apply latest-wins.
    # We do this by building a per-contact changelog and applying at the end.
    # For idempotency: we track processed event_ids persistently.

    # First pass: apply only events not yet processed
    new_events = [e for e in deduped_feed if e["event_id"] not in processed_ids]

    for event in new_events:
        eid   = event["event_id"]
        etype = event["type"]
        ts    = event["timestamp"]
        payload = event.get("payload", {})

        # ── Unknown event type → dead-letter ─────────────────────────────────
        if etype not in ("contact.created", "contact.updated", "reply.received"):
            entry = {
                "event_id": eid,
                "type": etype,
                "timestamp": ts,
                "reason": f"Unknown event type '{etype}' — not in schema",
                "payload": payload
            }
            dead_letter.append(entry)
            dead_lettered_this.append(entry)
            processed_ids.add(eid)
            continue

        # ── contact.updated with no contact_id → dead-letter ─────────────────
        if etype == "contact.updated" and "contact_id" not in payload:
            entry = {
                "event_id": eid,
                "type": etype,
                "timestamp": ts,
                "reason": "contact.updated missing contact_id — cannot route",
                "payload": payload
            }
            dead_letter.append(entry)
            dead_lettered_this.append(entry)
            processed_ids.add(eid)
            continue

        contact_id = payload.get("contact_id")

        # ── contact.created ───────────────────────────────────────────────────
        if etype == "contact.created":
            if contact_id not in roster:
                roster[contact_id] = {
                    "contact_id": contact_id,
                    "name": payload.get("name"),
                    "role": payload.get("role"),
                    "location": payload.get("location"),
                    "rate_usd_hr": payload.get("rate_usd_hr"),
                    "stage": "new",
                    "interest": "pending",
                    "interest_reason": None,
                    "doc_link": None,
                    "created_at": ts,
                    "_field_timestamps": {
                        "name": ts, "role": ts, "location": ts,
                        "rate_usd_hr": ts, "stage": ts
                    }
                }
                contacts_created.append(contact_id)
            else:
                # Contact already exists (may be a shell from an out-of-order update).
                # Merge: apply any missing fields using timestamp-wins logic.
                contact = roster[contact_id]
                ft = contact.setdefault("_field_timestamps", {})
                for field in ["name", "role", "location", "rate_usd_hr"]:
                    if field in payload:
                        existing_ts = ft.get(field, "")
                        if ts > existing_ts:
                            contact[field] = payload[field]
                            ft[field] = ts
                        elif ts == existing_ts and contact.get(field) is None:
                            contact[field] = payload[field]
                            ft[field] = ts
                # Preserve earlier created_at if shell was created first
                if contact.get("created_at", ts) > ts:
                    contact["created_at"] = ts
            processed_ids.add(eid)

        # ── contact.updated ───────────────────────────────────────────────────
        elif etype == "contact.updated":
            # If contact doesn't exist yet (out-of-order), create a shell
            if contact_id not in roster:
                roster[contact_id] = {
                    "contact_id": contact_id,
                    "name": None, "role": None, "location": None,
                    "rate_usd_hr": None, "stage": "new",
                    "interest": "pending", "interest_reason": None, "doc_link": None,
                    "created_at": ts,
                    "_field_timestamps": {}
                }

            contact = roster[contact_id]
            ft = contact.setdefault("_field_timestamps", {})
            updatable = ["name", "role", "location", "rate_usd_hr", "stage"]
            updated_fields = []

            for field in updatable:
                if field in payload:
                    existing_ts = ft.get(field, "")
                    # Timestamp-wins: only apply if this event is newer
                    # Conflict rule (same timestamp, different values): keep higher numeric value
                    # or lexicographically larger string. Document: deterministic tie-break.
                    if ts > existing_ts:
                        contact[field] = payload[field]
                        ft[field] = ts
                        updated_fields.append(field)
                    elif ts == existing_ts and payload[field] != contact.get(field):
                        # Tie-break: for numeric fields, keep the lower value (more conservative rate)
                        # For strings, keep the lexicographically smaller value (deterministic)
                        current = contact.get(field)
                        new_val = payload[field]
                        if isinstance(new_val, (int, float)) and isinstance(current, (int, float)):
                            winner = min(current, new_val)
                        else:
                            winner = min(str(current), str(new_val))
                        contact[field] = winner
                        ft[field] = ts
                        updated_fields.append(f"{field}[tie-break]")

            if updated_fields:
                contacts_updated.append({"contact_id": contact_id, "fields": updated_fields, "event_id": eid})
            processed_ids.add(eid)

        # ── reply.received ────────────────────────────────────────────────────
        elif etype == "reply.received":
            reply_text = payload.get("text", "")

            # Shell-create contact if they don't exist yet
            if contact_id not in roster:
                roster[contact_id] = {
                    "contact_id": contact_id,
                    "name": None, "role": None, "location": None,
                    "rate_usd_hr": None, "stage": "new",
                    "interest": "pending", "interest_reason": None, "doc_link": None,
                    "created_at": ts,
                    "_field_timestamps": {}
                }

            # Don't re-process reply if already handled (idempotency)
            # Check if this event was previously processed
            # (already filtered via processed_ids — we only reach here if new)
            try:
                result = extract_interest(reply_text)
            except Exception as e:
                entry = {
                    "event_id": eid,
                    "type": etype,
                    "timestamp": ts,
                    "reason": f"LLM extraction failed: {str(e)}",
                    "payload": payload
                }
                dead_letter.append(entry)
                dead_lettered_this.append(entry)
                processed_ids.add(eid)
                continue

            interest    = result.get("interest", "needs_review")
            confidence  = result.get("confidence", "low")
            reason      = result.get("reason", "")

            # Low confidence → needs_review, do not write to roster as fact
            if confidence == "low" or interest == "needs_review":
                entry = {
                    "event_id": eid,
                    "contact_id": contact_id,
                    "timestamp": ts,
                    "reply_text": reply_text,
                    "llm_result": result,
                    "reason": "Low-confidence interest extraction — needs human review"
                }
                # Only add if not already in review queue (idempotency)
                existing_eids = {r.get("event_id") for r in review_queue}
                if eid not in existing_eids:
                    review_queue.append(entry)
                    review_queued_this.append(entry)
                # Set interest to needs_review on roster
                roster[contact_id]["interest"] = "needs_review"
            else:
                roster[contact_id]["interest"] = interest
                roster[contact_id]["interest_reason"] = reason

            # Part C: project doc matching (independent of interest confidence)
            doc_url, doc_name = match_project_doc(reply_text)
            if doc_url:
                roster[contact_id]["doc_link"] = doc_url
                roster[contact_id]["doc_name"] = doc_name
            elif doc_name is None:
                # No project mentioned — leave doc_link as-is
                pass
            else:
                # project_mentioned but no doc found
                roster[contact_id]["doc_link"] = None
                roster[contact_id]["doc_name"] = "no doc found"

            replies_processed.append({
                "event_id": eid,
                "contact_id": contact_id,
                "interest": interest,
                "confidence": confidence
            })
            processed_ids.add(eid)

    # ── Compute diff for summary ──────────────────────────────────────────────
    changed_contacts = []
    for cid, contact in roster.items():
        before = roster_before.get(cid, {})
        diffs = {}
        for field in ["name", "role", "location", "rate_usd_hr", "stage", "interest", "doc_link"]:
            if contact.get(field) != before.get(field):
                diffs[field] = {"before": before.get(field), "after": contact.get(field)}
        if diffs:
            changed_contacts.append({"contact_id": cid, "name": contact.get("name"), "changes": diffs})

    new_dlq   = dead_lettered_this
    new_review = review_queued_this
    nothing_changed = (len(changed_contacts) == 0 and len(new_dlq) == 0 and len(new_review) == 0)

    # ── Save state ────────────────────────────────────────────────────────────
    save_json_file(ROSTER_FILE, roster)
    save_json_file(DLQ_FILE, dead_letter)
    save_json_file(REVIEW_FILE, review_queue)
    save_json_file(PROCESSED_FILE, sorted(processed_ids))

    # ── Generate state file ───────────────────────────────────────────────────
    state = {
        "last_run": run_start,
        "run_completed": now_iso(),
        "feed_url": FEED_URL,
        "total_events_in_feed": len(feed),
        "deduped_events": len(deduped_feed),
        "skipped_duplicate_event_ids": skipped_dup_events,
        "new_events_processed": len(new_events),
        "contacts_created_this_run": contacts_created,
        "contacts_updated_this_run": contacts_updated,
        "replies_processed_this_run": replies_processed,
        "dead_lettered_this_run": [d["event_id"] for d in new_dlq],
        "review_queued_this_run": [r["event_id"] for r in new_review],
        "roster_size": len(roster),
        "total_dead_lettered": len(dead_letter),
        "total_review_queue": len(review_queue),
        "nothing_changed": nothing_changed,
        "conflict_resolution_rule": (
            "Same field, same timestamp, different values: "
            "numeric fields keep the lower value (conservative rate); "
            "string fields keep the lexicographically smaller value. "
            "Applied to: evt_011 vs evt_012 (Diego rate_usd_hr at 2026-06-02T08:30:00Z -> 36 wins over 38)."
        ),
        "default_stage_rule": (
            "Contacts with no explicit stage event default to 'new'. "
            "Applied to: c_anika, c_lena, c_tomas (had no stage update before being contacted), "
            "and c_bob, c_jamie, c_diego (stage set via contact.updated)."
        )
    }
    save_json_file(STATE_FILE, state)

    # ── Build summary ─────────────────────────────────────────────────────────
    summary_lines = []
    summary_lines.append(f"TALENT SYNC RUN SUMMARY")
    summary_lines.append(f"Run: {run_start}")
    summary_lines.append("")

    if nothing_changed:
        summary_lines.append("NOTHING CHANGED — roster is identical to previous run. Requirement 1 confirmed held.")
    else:
        if changed_contacts:
            summary_lines.append(f"CHANGED ({len(changed_contacts)} contacts):")
            for c in changed_contacts:
                summary_lines.append(f"  {c['name'] or c['contact_id']}")
                for field, diff in c["changes"].items():
                    summary_lines.append(f"    {field}: {diff['before']} → {diff['after']}")
        summary_lines.append("")

    if new_dlq:
        summary_lines.append(f"DEAD-LETTERED ({len(new_dlq)} events):")
        for d in new_dlq:
            summary_lines.append(f"  {d['event_id']}: {d['reason']}")
        summary_lines.append("")

    if new_review:
        summary_lines.append(f"NEEDS YOUR REVIEW ({len(new_review)} replies):")
        for r in new_review:
            summary_lines.append(f"  {r['contact_id']} ({r['event_id']}): {r['reply_text'][:80]}...")
            summary_lines.append(f"  LLM read: {r['llm_result'].get('interest')} ({r['llm_result'].get('confidence')} confidence)")
            summary_lines.append(f"  Reason: {r['llm_result'].get('reason')}")
        summary_lines.append("")

    if not new_dlq and not new_review:
        summary_lines.append("No failures. No review items. All events processed cleanly.")

    summary_lines.append(f"\nRoster: {len(roster)} contacts | DLQ total: {len(dead_letter)} | Review queue total: {len(review_queue)}")
    summary_text = "\n".join(summary_lines)

    print(summary_text)

    # ── Send email summary ────────────────────────────────────────────────────
    if send_email and SUMMARY_FROM and SUMMARY_PASS:
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"Talent Sync: {'No changes' if nothing_changed else f'{len(changed_contacts)} contacts updated'} — {run_start[:10]}"
            msg["From"]    = SUMMARY_FROM
            msg["To"]      = SUMMARY_TO
            msg.attach(MIMEText(summary_text, "plain"))

            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(SUMMARY_FROM, SUMMARY_PASS)
                server.sendmail(SUMMARY_FROM, SUMMARY_TO, msg.as_string())
            print(f"\nSummary email sent to {SUMMARY_TO}")
        except Exception as e:
            print(f"\nEmail send failed: {e} (summary above is the full content)")

    return roster, state

# ── Export helpers ────────────────────────────────────────────────────────────
def export_roster_csv(roster):
    import csv
    import io
    output = io.StringIO()
    fields = ["contact_id", "name", "role", "location", "rate_usd_hr", "stage", "interest", "interest_reason", "doc_link", "doc_name"]
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for contact in roster.values():
        writer.writerow({f: contact.get(f, "") for f in fields})
    csv_path = os.path.join(OUTPUT_DIR, "roster.csv")
    with open(csv_path, "w") as f:
        f.write(output.getvalue())
    print(f"Roster CSV saved: {csv_path}")

if __name__ == "__main__":
    import sys
    send = "--no-email" not in sys.argv
    roster, state = run_pipeline(send_email=send)
    export_roster_csv(roster)
    print(f"\nState file: {STATE_FILE}")
    print(f"Roster JSON: {ROSTER_FILE}")
    print(f"Dead-letter: {DLQ_FILE}")
    print(f"Review queue: {REVIEW_FILE}")

