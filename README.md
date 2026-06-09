# Talent Sync Pipeline

A Python pipeline that ingests a stream of contact events, maintains an exactly-correct roster, and connects interested people to project context automatically.

## What it does

- Fetches talent-feed.json from GitHub on each run
- Deduplicates events by event_id
- Processes events using timestamps to decide what is current
- Handles out-of-order arrivals
- Uses Claude (LLM) to extract interest from replies
- Matches interested contacts to project docs
- Logs unprocessable events to a dead-letter queue
- Sends one summary email per run
- Fully idempotent: running twice produces the same result

## How to run

pip3 install anthropic

export ANTHROPIC_API_KEY=your_key
export SUMMARY_EMAIL_FROM=your_gmail
export SUMMARY_EMAIL_PASS=your_app_password
export SUMMARY_EMAIL_TO=brock@sandbar.ai

python3 pipeline.py

## Run again to prove idempotency

python3 pipeline.py

## Output files

- roster.json / roster.csv — final contact roster
- dead_letter.json — events that could not be processed
- review_queue.json — low-confidence replies needing human review
- state.json — auto-generated run log
