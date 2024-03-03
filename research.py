from fastapi import FastAPI
import v1.routers.city as city
import v1.routers.research as research
import v1.routers.user as user
import v1.routers.neighborhood  as neighborhood
import v1.routers.answer as answer
import v1.routers.fix_answers as fix_answers
import v1.routers.ura as ura
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
app.include_router(fix_answers.router)
app.include_router(ura.router)