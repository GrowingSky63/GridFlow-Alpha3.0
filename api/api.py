from fastapi import FastAPI
from api.bdgd import region_routes

app = FastAPI()

app.include_router(region_routes.router, prefix="/bdgd")


