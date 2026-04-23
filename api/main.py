"""
main.py
FastAPI application entrypoint.
"""

from fastapi import FastAPI
from api.routes import router

app = FastAPI(
    title="mx-economic-pipeline",
    description="REST API for Mexican economic indicators — Banxico data",
    version="0.1.0",
)

app.include_router(router)