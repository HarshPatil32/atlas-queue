from fastapi import FastAPI

from api.dead_letter import router as dead_letter_router
from api.jobs import router as jobs_router

app = FastAPI(title="Atlas Queue API")
app.include_router(jobs_router)
app.include_router(dead_letter_router)
