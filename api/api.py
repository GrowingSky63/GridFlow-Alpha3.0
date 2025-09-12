from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.bdgd import region_routes, substation_routes, trhv_routes

api_prefix = '/api'
bdgd_prefix = '/'.join([api_prefix, 'bdgd'])

app = FastAPI()

ports = [
    8000
]

hosts = [
    '172.25.0.232'
]

origins = [f'http://{host}:{port}' for host in hosts for port in ports]

app.add_middleware(
    CORSMiddleware,
    # allow_origins=origins,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(region_routes.router, prefix=bdgd_prefix)
app.include_router(substation_routes.router, prefix=bdgd_prefix)
app.include_router(trhv_routes.router, prefix=bdgd_prefix)
