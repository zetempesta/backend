from typing import Union
from fastapi import FastAPI, Request
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from fastapi.responses import RedirectResponse
import v1.routers.city as city
import v1.routers.research as research
import v1.routers.user as user
import v1.routers.neighborhood  as neighborhood
import v1.routers.answer as answer
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI()

origins = ['*']

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(city.router)
app.include_router(research.router)
app.include_router(user.router)
app.include_router(neighborhood.router)
app.include_router(answer.router)