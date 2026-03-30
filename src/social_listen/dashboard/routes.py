from __future__ import annotations

import json
from math import ceil

from fastapi import APIRouter, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from social_listen.database import Database

TEMPLATE_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))


def _get_db(request: Request) -> Database:
    return request.app.state.db


def create_router() -> APIRouter:
    router = APIRouter()

    @router.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        db = _get_db(request)
        stats = await db.get_stats()

        # Top 10 leads
        top_leads, _ = await db.get_leads_paginated(page=1, page_size=10, sort="score")

        # Enrich top leads with platform info
        for lead in top_leads:
            lead["accounts"] = await db.get_platform_accounts_for_lead(lead["id"])

        # Recent collection runs
        recent_runs = await db.get_recent_runs(limit=10)

        return templates.TemplateResponse(request, "index.html", {
            "stats": stats,
            "top_leads": top_leads,
            "recent_runs": recent_runs,
        })

    @router.get("/leads", response_class=HTMLResponse)
    async def leads_list(
        request: Request,
        page: int = Query(1, ge=1),
        platform: str | None = Query(None),
        status: str | None = Query(None),
        min_score: float | None = Query(None),
        sort: str = Query("score"),
    ):
        db = _get_db(request)
        page_size = request.app.state.config.dashboard.page_size

        leads, total = await db.get_leads_paginated(
            page=page,
            page_size=page_size,
            platform=platform,
            status=status,
            min_score=min_score,
            sort=sort,
        )

        # Enrich with platform accounts
        for lead in leads:
            lead["accounts"] = await db.get_platform_accounts_for_lead(lead["id"])

        total_pages = ceil(total / page_size) if total > 0 else 1

        return templates.TemplateResponse(request, "leads.html", {
            "leads": leads,
            "page": page,
            "total_pages": total_pages,
            "total": total,
            "platform": platform,
            "status": status,
            "min_score": min_score,
            "sort": sort,
        })

    @router.get("/leads/{lead_id}", response_class=HTMLResponse)
    async def lead_detail(request: Request, lead_id: int):
        db = _get_db(request)
        lead = await db.get_lead(lead_id)
        if not lead:
            return HTMLResponse("Lead not found", status_code=404)

        # If merged, redirect to primary
        if lead.get("merged_into_id"):
            return RedirectResponse(f"/leads/{lead['merged_into_id']}")

        accounts = await db.get_platform_accounts_for_lead(lead_id)
        posts = await db.get_posts_for_lead(lead_id)

        # Parse JSON fields in posts
        for post in posts:
            if isinstance(post.get("engagement"), str):
                try:
                    post["engagement"] = json.loads(post["engagement"])
                except (json.JSONDecodeError, TypeError):
                    post["engagement"] = {}
            if isinstance(post.get("matched_keywords"), str):
                try:
                    post["matched_keywords"] = json.loads(post["matched_keywords"])
                except (json.JSONDecodeError, TypeError):
                    post["matched_keywords"] = []

        # Parse raw_data in accounts
        for account in accounts:
            if isinstance(account.get("raw_data"), str):
                try:
                    account["raw_data"] = json.loads(account["raw_data"])
                except (json.JSONDecodeError, TypeError):
                    account["raw_data"] = {}

        return templates.TemplateResponse(request, "lead_detail.html", {
            "lead": lead,
            "accounts": accounts,
            "posts": posts,
        })

    @router.post("/leads/{lead_id}/status")
    async def update_status(request: Request, lead_id: int, status: str = Form(...)):
        db = _get_db(request)
        await db.update_lead_status(lead_id, status)
        return RedirectResponse(f"/leads/{lead_id}", status_code=303)

    @router.post("/leads/{lead_id}/notes")
    async def update_notes(request: Request, lead_id: int, notes: str = Form(...)):
        db = _get_db(request)
        await db.update_lead_notes(lead_id, notes)
        return RedirectResponse(f"/leads/{lead_id}", status_code=303)

    @router.post("/leads/merge")
    async def merge_leads(
        request: Request,
        primary_id: int = Form(...),
        secondary_id: int = Form(...),
    ):
        db = _get_db(request)
        await db.merge_leads(primary_id, secondary_id)
        return RedirectResponse(f"/leads/{primary_id}", status_code=303)

    @router.get("/keywords", response_class=HTMLResponse)
    async def keywords_page(request: Request):
        db = _get_db(request)
        keywords = await db.get_all_keywords()
        return templates.TemplateResponse(request, "keywords.html", {
            "keywords": keywords,
        })

    @router.post("/keywords")
    async def add_keyword(
        request: Request,
        term: str = Form(...),
        category: str = Form("concept"),
    ):
        db = _get_db(request)
        await db.add_keyword(term, category)
        return RedirectResponse("/keywords", status_code=303)

    @router.post("/keywords/{keyword_id}/toggle")
    async def toggle_keyword(request: Request, keyword_id: int):
        db = _get_db(request)
        await db.toggle_keyword(keyword_id)
        return RedirectResponse("/keywords", status_code=303)

    @router.get("/runs", response_class=HTMLResponse)
    async def runs_page(request: Request):
        db = _get_db(request)
        runs = await db.get_recent_runs(limit=100)

        # Parse metadata JSON
        for run in runs:
            if isinstance(run.get("metadata"), str):
                try:
                    run["metadata"] = json.loads(run["metadata"])
                except (json.JSONDecodeError, TypeError):
                    run["metadata"] = {}

        return templates.TemplateResponse(request, "runs.html", {
            "runs": runs,
        })

    @router.get("/api/stats")
    async def api_stats(request: Request):
        db = _get_db(request)
        return await db.get_stats()

    return router
