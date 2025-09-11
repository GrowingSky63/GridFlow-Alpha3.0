from fastapi import FastAPI
from api.bdgd import region_routes, substation_routes, trhv_routes

app = FastAPI()

app.include_router(region_routes.router, prefix="/bdgd")
app.include_router(substation_routes.router, prefix="/bdgd")
app.include_router(trhv_routes.router, prefix="/bdgd")
