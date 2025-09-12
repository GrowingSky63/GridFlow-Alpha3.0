from fastapi import FastAPI
from api.bdgd import region_routes, substation_routes, trhv_routes

api_prefix = '/api'
bdgd_prefix = '/'.join([api_prefix, 'bdgd'])

app = FastAPI()

app.include_router(region_routes.router, prefix=bdgd_prefix)
app.include_router(substation_routes.router, prefix=bdgd_prefix)
app.include_router(trhv_routes.router, prefix=bdgd_prefix)
